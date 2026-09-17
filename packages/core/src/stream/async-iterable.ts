import type { Eff } from "../eff";
import { async, ensuring, succeed } from "../constructors";
import { Cause } from "../cause";
import type { Fiber, FiberResult } from "../fiber";
import { runFiber } from "../runtime";
import type { Chunk } from "./chunk";
import type { Step, Stream } from "./stream";

interface Settle<T> {
  readonly resolve: (value: T) => void;
  readonly reject: (error: unknown) => void;
}

const DONE: IteratorReturnResult<undefined> = { done: true, value: undefined };

export function streamToAsyncIterable<A>(stream: Stream<A, unknown>): AsyncIterable<A> {
  return {
    [Symbol.asyncIterator]: () => new StreamAsyncIterator(stream),
  };
}

// One driver fiber per iterator. It pulls a chunk only while a `next()` call
// is waiting, then parks until the consumer drains the chunk and asks again.
// Keeping the fiber alive between pulls matters: fibers forked by concurrent
// operators are children of the pulling fiber and die with it.
class StreamAsyncIterator<A> implements AsyncIterator<A> {
  private fiber: Fiber<void> | null = null;
  private chunk: Chunk<A> | null = null;
  private index = 0;
  private done = false;
  private demand: ((pull: boolean) => void) | null = null;
  private waiter: Settle<IteratorResult<A>> | null = null;
  private inFlight: Promise<IteratorResult<A>> | null = null;
  private closer: Settle<void> | null = null;
  private closing: Promise<void> | null = null;

  constructor(private readonly stream: Stream<A, unknown>) {}

  next(): Promise<IteratorResult<A>> {
    if (this.inFlight !== null) {
      const retry = () => this.next();
      return this.inFlight.then(retry, retry);
    }
    const chunk = this.chunk;
    if (chunk !== null) {
      const value = chunk.get(this.index++);
      if (this.index >= chunk.length) this.chunk = null;
      return Promise.resolve({ done: false, value });
    }
    if (this.closing !== null) {
      const done = () => DONE;
      return this.closing.then(done, done);
    }
    if (this.done) return Promise.resolve(DONE);

    const pending = new Promise<IteratorResult<A>>((resolve, reject) => {
      this.waiter = { resolve, reject };
    });
    this.inFlight = pending;
    if (this.fiber === null) this.start();
    else this.signal(true);
    return pending;
  }

  return(value?: unknown): Promise<IteratorResult<A>> {
    const result: IteratorReturnResult<unknown> = { done: true, value };
    this.chunk = null;
    if (this.closing === null) {
      if (this.fiber === null || this.done) {
        this.done = true;
        return Promise.resolve(result);
      }
      this.closing = new Promise<void>((resolve, reject) => {
        this.closer = { resolve, reject };
      });
      // A parked driver stops like `take` does; a pull in flight is interrupted.
      if (this.demand !== null) this.signal(false);
      else this.fiber.interrupt();
    }
    return this.closing.then(() => result);
  }

  private start(): void {
    const fiber = runFiber(this.drive() as Eff<void, never>);
    this.fiber = fiber;
    fiber.onComplete((result) => this.complete(result));
  }

  private signal(pull: boolean): void {
    const demand = this.demand;
    this.demand = null;
    demand?.(pull);
  }

  private drive(): Eff<void, unknown> {
    const loop = (stream: Stream<A, unknown>): Eff<void, unknown> =>
      stream.step.flatMap((step: Step<A>): Eff<void, unknown> => {
        if (step._tag === "Done") return succeed(undefined);
        if (step.chunk.isEmpty) return loop(step.next);
        return this.deliver(step.chunk).flatMap((pull) =>
          pull ? loop(step.next) : succeed(undefined),
        );
      });
    const body = loop(this.stream);
    const finalizer = this.stream._finalizer;
    return finalizer === null ? body : ensuring(body, finalizer);
  }

  private deliver(chunk: Chunk<A>): Eff<boolean, never> {
    return async<boolean>((resume) => {
      this.demand = (pull) => resume(succeed(pull));
      this.chunk = chunk.length > 1 ? chunk : null;
      this.index = 1;
      this.settleWaiter((waiter) => waiter.resolve({ done: false, value: chunk.get(0) }));
      return () => {
        this.demand = null;
      };
    }) as Eff<boolean, never>;
  }

  private complete(result: FiberResult<void>): void {
    this.done = true;
    this.chunk = null;
    this.demand = null;
    const closer = this.closer;
    this.closer = null;
    const stopped = closer !== null;
    const failure =
      result.ok || (stopped && Cause.isInterruptedOnly(result.cause))
        ? null
        : { error: Cause.squash(result.cause) };

    if (stopped) {
      this.settleWaiter((waiter) => waiter.resolve(DONE));
      if (failure === null) closer.resolve();
      else closer.reject(failure.error);
      return;
    }
    this.settleWaiter((waiter) =>
      failure === null ? waiter.resolve(DONE) : waiter.reject(failure.error),
    );
  }

  private settleWaiter(settle: (waiter: Settle<IteratorResult<A>>) => void): void {
    const waiter = this.waiter;
    this.waiter = null;
    this.inFlight = null;
    if (waiter !== null) settle(waiter);
  }
}
