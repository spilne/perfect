import type { Eff } from "../eff";
import {
  awaitFiber,
  ensuring,
  failCause,
  fork,
  forkDaemon,
  interrupt,
  succeed,
  suspend,
  uninterruptible,
} from "../constructors";
import { Cause } from "../cause";
import { InProcessDeferred } from "../deferred";
import type { Fiber } from "../fiber";
import { Queue } from "../queue";
import { Semaphore } from "../semaphore";
import type { Chunk } from "./chunk";
import { Stream, type Step } from "./stream";
import { combineFinalizers, interruptAllEff } from "./driver-lifecycle";

const OUTPUT_CAPACITY = 16;
const UNIT: Eff<void, never> = succeed(undefined);
const DONE: Step<never> = { _tag: "Done" };

interface InnerHandle {
  fiber: Fiber<any> | null;
  finalizer: Eff<void, unknown> | null;
}

type AnyStream<A> = Stream<A, unknown>;

export function parJoinStreams<A>(params: {
  outer: Stream<AnyStream<A>, unknown>;
  maxOpen: number;
}): Stream<A, unknown> {
  const { outer, maxOpen } = params;
  const drivers: Fiber<any>[] = [];

  const setup = Queue.bounded<Chunk<A>>(OUTPUT_CAPACITY).flatMap((output) =>
    (maxOpen === Infinity ? succeed(null) : Semaphore.make(maxOpen)).flatMap((permits) => {
      const open = new Set<InnerHandle>();
      const idle = new InProcessDeferred<void>();
      const acquire: Eff<void, never> = permits === null ? UNIT : permits.acquire();
      const release: Eff<void, never> = permits === null ? UNIT : permits.release();
      let driver: Fiber<any> | null = null;
      let outerDone = false;
      let closed = false;
      let failure: Cause | null = null;

      // Closing the output lets the consumer drain chunks already accepted,
      // then observe completion or the recorded failure.
      const close = (cause: Cause | null): Eff<void, never> =>
        suspend(() => {
          if (closed) return UNIT;
          closed = true;
          failure = cause;
          return output.close();
        });

      // The first failure wins; interrupting the driver stops the outer and
      // every sibling without waiting for the consumer to reach the failure.
      const failWith = (cause: Cause): Eff<void, never> =>
        suspend(() =>
          closed ? UNIT : close(cause).flatMap(() => (driver === null ? UNIT : interrupt(driver))),
        );

      const releaseInner = (handle: InnerHandle): Eff<void, unknown> =>
        suspend(() => {
          const finalizer = handle.finalizer;
          handle.finalizer = null;
          return finalizer ?? UNIT;
        });

      const publish = (inner: AnyStream<A>): Eff<void, unknown> =>
        inner.step.flatMap((step) =>
          step._tag === "Done"
            ? UNIT
            : output
                .offer(step.chunk)
                .catch(() => succeed(false))
                .flatMap((accepted) => (accepted && !closed ? publish(step.next) : UNIT)),
        );

      const innerDone = (handle: InnerHandle): Eff<void, never> =>
        suspend(() => {
          open.delete(handle);
          return outerDone && open.size === 0
            ? idle.succeed(undefined).flatMap(() => release)
            : release;
        });

      const runInner = (inner: AnyStream<A>, handle: InnerHandle): Eff<void, unknown> =>
        ensuring(
          ensuring(publish(inner), releaseInner(handle)).catchAllCause(failWith),
          innerDone(handle),
        );

      // Registration is uninterruptible so teardown never misses a forked inner.
      const launch = (inner: AnyStream<A>): Eff<void, unknown> =>
        uninterruptible(
          suspend(() => {
            if (closed) return release;
            const handle: InnerHandle = { fiber: null, finalizer: inner._finalizer };
            open.add(handle);
            return fork(runInner(inner, handle)).map((fiber) => {
              handle.fiber = fiber;
            });
          }),
        );

      // A permit is taken before each outer pull, so a full join stops pulling.
      const pullOuter = (
        stream: Stream<AnyStream<A>, unknown>,
        holding: boolean,
      ): Eff<void, unknown> =>
        (holding ? stream.step : acquire.flatMap(() => stream.step)).flatMap((step) => {
          if (step._tag === "Done") return release;
          if (step.chunk.isEmpty) return pullOuter(step.next, true);
          return launchChunk(step.chunk, 0, step.next);
        });

      const launchChunk = (
        chunk: Chunk<AnyStream<A>>,
        index: number,
        next: Stream<AnyStream<A>, unknown>,
      ): Eff<void, unknown> =>
        launch(chunk.get(index)).flatMap(() => {
          if (closed) return UNIT;
          if (index + 1 < chunk.length) {
            return acquire.flatMap(() => launchChunk(chunk, index + 1, next));
          }
          return pullOuter(next, false);
        });

      const awaitInners: Eff<void, unknown> = suspend(() => {
        outerDone = true;
        return closed || open.size === 0 ? UNIT : idle.await;
      });

      const stopInners: Eff<void, unknown> = close(Cause.interrupt()).flatMap(() => {
        const handles = Array.from(open);
        for (const handle of handles) handle.fiber?.interrupt();
        // An inner interrupted before its first step never reaches its own
        // ensuring, so every handle's finalizer is released here as well.
        return handles.reduce<Eff<void, unknown>>(
          (acc, handle) =>
            ensuring(
              acc,
              handle.fiber === null
                ? releaseInner(handle)
                : awaitFiber(handle.fiber).flatMap(() => releaseInner(handle)),
            ),
          UNIT,
        );
      });

      const run = ensuring(
        pullOuter(outer, false)
          .flatMap(() => awaitInners)
          .flatMap(() => close(null))
          .catchAllCause(close),
        stopInners,
      );

      const pull = (): Eff<Step<A>, unknown> =>
        output
          .take()
          .map((chunk): Step<A> => ({
            _tag: "Emit",
            chunk,
            next: new Stream(suspend(pull)),
          }))
          .catch(() => (failure === null ? succeed(DONE) : failCause(failure)));

      // The driver is owned by the stream finalizer rather than the pulling
      // fiber: pulls may run on short-lived fibers (e.g. `timeout` races each
      // pull), and a structured child would die with that fiber.
      return uninterruptible(
        forkDaemon(run).map((fiber) => {
          driver = fiber;
          drivers.push(fiber);
        }),
      ).flatMap(pull);
    }),
  );

  return new Stream(
    suspend(() => setup),
    combineFinalizers(interruptAllEff(drivers), outer._finalizer),
  );
}
