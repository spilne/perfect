import type { Eff } from "../eff";
import { async, ensuring, succeed, suspend } from "../constructors";
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

// One driver fiber per iterator runs every pull and, through `ensuring`, the
// stream's finalizer, which also stops the fibers the stream's operators own.
// It pulls a chunk only while a `next()` call is waiting, then parks until the
// consumer drains the chunk and asks again. Keeping pulls on that fiber lets
// `return()` interrupt one in flight without skipping or repeating the finalizer.
class StreamAsyncIterator<A> implements AsyncIterableIterator<A> {
  private stream: Stream<A, unknown> | null;
  private fiber: Fiber<void> | null = null;
  private running = false;
  private stopRequested = false;
  private chunk: Chunk<A> | null = null;
  private index = 0;
  private done = false;
  private demand: ((pull: boolean) => void) | null = null;
  private readonly waiters: Settle<IteratorResult<A>>[] = [];
  private closer: Settle<void> | null = null;
  private closing: Promise<void> | null = null;

  constructor(stream: Stream<A, unknown>) {
    this.stream = stream;
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  next(): Promise<IteratorResult<A>> {
    if (this.waiters.length === 0) {
      const chunk = this.chunk;
      if (chunk !== null) return Promise.resolve({ done: false, value: this.take(chunk) });
      if (this.closing !== null) {
        const done = () => DONE;
        return this.closing.then(done, done);
      }
      if (this.done) return Promise.resolve(DONE);
    }

    const pending = new Promise<IteratorResult<A>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
    if (this.waiters.length === 1) {
      if (this.fiber === null) this.start();
      else this.signal(true);
    }
    return pending;
  }

  return(value?: unknown): Promise<IteratorResult<A>> {
    const result: IteratorReturnResult<unknown> = { done: true, value };
    if (this.closing !== null) {
      const finished = () => result;
      return this.closing.then(finished, finished);
    }
    if (this.done || this.fiber === null) {
      this.finish();
      return Promise.resolve(result);
    }

    this.chunk = null;
    const closing = new Promise<void>((resolve, reject) => {
      this.closer = { resolve, reject };
    });
    this.closing = closing;
    // Not yet running: the driver sees the request and only runs finalizers.
    // Parked: it stops like `take` does. Pulling: the pull is interrupted.
    if (!this.running) this.stopRequested = true;
    else if (this.demand !== null) this.signal(false);
    else this.fiber.interrupt();
    return closing.then(() => result);
  }

  private start(): void {
    const fiber = runFiber(this.drive(this.stream!) as Eff<void, never>);
    this.fiber = fiber;
    fiber.onComplete((result) => this.complete(result));
  }

  private signal(pull: boolean): void {
    const demand = this.demand;
    this.demand = null;
    demand?.(pull);
  }

  private take(chunk: Chunk<A>): A {
    const value = chunk.get(this.index++);
    if (this.index >= chunk.length) this.chunk = null;
    return value;
  }

  private drive(stream: Stream<A, unknown>): Eff<void, unknown> {
    const loop = (current: Stream<A, unknown>): Eff<void, unknown> =>
      current.step.flatMap((step: Step<A>): Eff<void, unknown> => {
        if (step._tag === "Done") return succeed(undefined);
        if (step.chunk.isEmpty) return loop(step.next);
        return this.offer(step.chunk).flatMap((pull) =>
          pull ? loop(step.next) : succeed(undefined),
        );
      });
    const body = suspend((): Eff<void, unknown> => {
      this.running = true;
      return this.stopRequested ? succeed(undefined) : loop(stream);
    });
    const finalizer = stream._finalizer;
    return finalizer === null ? body : ensuring(body, finalizer);
  }

  private offer(chunk: Chunk<A>): Eff<boolean, never> {
    return async<boolean>((resume) => {
      this.chunk = chunk;
      this.index = 0;
      let waiter = this.waiters.shift();
      while (waiter !== undefined) {
        waiter.resolve({ done: false, value: this.take(chunk) });
        waiter = this.chunk === null ? undefined : this.waiters.shift();
      }
      if (this.waiters.length > 0) {
        resume(succeed(true));
        return;
      }
      this.demand = (pull) => resume(succeed(pull));
      return () => {
        this.demand = null;
      };
    }) as Eff<boolean, never>;
  }

  private complete(result: FiberResult<void>): void {
    const closer = this.closer;
    const waiters = this.waiters.splice(0);
    this.finish();
    const failure =
      result.ok || (closer !== null && Cause.isInterruptedOnly(result.cause))
        ? null
        : { error: Cause.squash(result.cause) };

    if (closer !== null) {
      for (const waiter of waiters) waiter.resolve(DONE);
      if (failure === null) closer.resolve();
      else closer.reject(failure.error);
      return;
    }
    const first = waiters.shift();
    if (first !== undefined) {
      if (failure === null) first.resolve(DONE);
      else first.reject(failure.error);
    }
    for (const waiter of waiters) waiter.resolve(DONE);
  }

  private finish(): void {
    this.done = true;
    this.stream = null;
    this.fiber = null;
    this.chunk = null;
    this.demand = null;
    this.closer = null;
    this.closing = null;
  }
}
