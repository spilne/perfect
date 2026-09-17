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
import { combineFinalizers } from "./driver-lifecycle";

const OUTPUT_CAPACITY = 16;
const UNIT: Eff<void, never> = succeed(undefined);
const DONE: Step<never> = { _tag: "Done" };

type AnyStream<A> = Stream<A, unknown>;

interface InnerHandle {
  fiber: Fiber<any> | null;
  finalizer: Eff<void, unknown> | null;
}

interface Session<A> {
  readonly pull: () => Eff<Step<A>, unknown>;
  readonly shutdown: Eff<void, unknown>;
}

const andThen = (first: Cause | null, second: Cause): Cause =>
  first === null ? second : Cause.then(first, second);

export function parJoinStreams<A>(params: {
  outer: Stream<AnyStream<A>, unknown>;
  maxOpen: number;
}): Stream<A, unknown> {
  const { outer, maxOpen } = params;
  // One join per run: re-running the first pull (e.g. `timeout(ms).retry()`)
  // resumes the running join instead of starting a second one beside it.
  let session: Session<A> | null = null;

  const open = (): Eff<Step<A>, unknown> =>
    Queue.bounded<Chunk<A>>(OUTPUT_CAPACITY).flatMap((output) =>
      (maxOpen === Infinity ? succeed(null) : Semaphore.make(maxOpen)).flatMap((permits) => {
        const handles = new Set<InnerHandle>();
        const idle = new InProcessDeferred<void>();
        const acquire: Eff<void, never> = permits === null ? UNIT : permits.acquire();
        const releasePermit: Eff<void, never> = permits === null ? UNIT : permits.release();
        let driver: Fiber<any> | null = null;
        // Outer elements already pulled but not yet launched; teardown
        // finalizes them like opened inner streams.
        let backlog: Chunk<AnyStream<A>> | null = null;
        let backlogIndex = 0;
        let outerDone = false;
        let closed = false;
        let stopping = false;
        let observed = false;
        let failure: Cause | null = null;
        let unreported: Cause | null = null;

        const close = (cause: Cause | null): Eff<void, never> =>
          suspend(() => {
            if (closed) return UNIT;
            closed = true;
            failure = cause;
            return output.close();
          });

        // The first failure wins, as with `merge`. Interrupting the driver
        // stops the outer and every sibling without waiting for the consumer.
        const failJoin = (cause: Cause): Eff<void, never> =>
          suspend(() =>
            closed
              ? UNIT
              : close(cause).flatMap(() => (driver === null ? UNIT : interrupt(driver))),
          );

        const finalizerFailed = (cause: Cause): Eff<void, never> =>
          suspend(() => {
            const errors = Cause.stripInterrupts(cause);
            if (errors === null) return UNIT;
            if (!closed) return failJoin(errors);
            unreported = andThen(unreported, errors);
            return UNIT;
          });

        const runFinalizer = (finalizer: Eff<void, unknown> | null): Eff<void, unknown> =>
          finalizer === null ? UNIT : finalizer.catchAllCause(finalizerFailed);

        const releaseInner = (handle: InnerHandle): Eff<void, unknown> =>
          suspend(() => {
            const finalizer = handle.finalizer;
            handle.finalizer = null;
            return runFinalizer(finalizer);
          });

        const publish = (inner: AnyStream<A>): Eff<void, unknown> =>
          inner.step.flatMap((step) => {
            if (step._tag === "Done") return UNIT;
            if (step.chunk.isEmpty) return publish(step.next);
            return output
              .offer(step.chunk)
              .catch(() => succeed(false))
              .flatMap((accepted) => (accepted && !closed ? publish(step.next) : UNIT));
          });

        // Teardown closes the output before interrupting anything, so an
        // interruption seen while the join is still open came from the stream
        // itself and must not pass for completion.
        const innerFailed = (cause: Cause): Eff<void, unknown> => {
          const errors = Cause.stripInterrupts(cause);
          if (errors !== null) return failJoin(errors);
          return closed ? failCause(cause) : failJoin(cause).flatMap(() => failCause(cause));
        };

        const innerDone = (handle: InnerHandle): Eff<void, never> =>
          suspend(() => {
            handles.delete(handle);
            if (!outerDone || handles.size > 0) return releasePermit;
            return idle.succeed(undefined).flatMap(() => releasePermit);
          });

        const runInner = (inner: AnyStream<A>, handle: InnerHandle): Eff<void, unknown> =>
          ensuring(
            publish(inner).catchAllCause(innerFailed),
            releaseInner(handle).flatMap(() => innerDone(handle)),
          );

        // Registration is uninterruptible so teardown never misses a forked inner.
        const launchNext: Eff<void, unknown> = uninterruptible(
          suspend(() => {
            if (closed || backlog === null) return releasePermit;
            const inner = backlog.get(backlogIndex++);
            if (backlogIndex >= backlog.length) backlog = null;
            const handle: InnerHandle = { fiber: null, finalizer: inner._finalizer };
            handles.add(handle);
            return fork(runInner(inner, handle)).map((fiber) => {
              handle.fiber = fiber;
            });
          }),
        );

        // A permit is taken before each outer pull, so a full join stops pulling.
        const pullOuter = (
          stream: Stream<AnyStream<A>, unknown>,
          holdingPermit: boolean,
        ): Eff<void, unknown> =>
          (holdingPermit ? stream.step : acquire.flatMap(() => stream.step)).flatMap((step) => {
            if (step._tag === "Done") return releasePermit;
            if (step.chunk.isEmpty) return pullOuter(step.next, true);
            backlog = step.chunk;
            backlogIndex = 0;
            return launchBacklog(step.next);
          });

        const launchBacklog = (next: Stream<AnyStream<A>, unknown>): Eff<void, unknown> =>
          launchNext.flatMap(() => {
            if (closed) return UNIT;
            if (backlog === null) return pullOuter(next, false);
            return acquire.flatMap(() => launchBacklog(next));
          });

        const awaitInners: Eff<void, unknown> = suspend(() => {
          outerDone = true;
          return closed || handles.size === 0 ? UNIT : idle.await;
        });

        const stopInners: Eff<void, unknown> = close(null).flatMap(() => {
          const opened = Array.from(handles);
          const pending = backlog === null ? [] : backlog.toArray().slice(backlogIndex);
          backlog = null;
          for (const handle of opened) handle.fiber?.interrupt();
          // An inner interrupted before its first step never reaches its own
          // ensuring, so every handle's finalizer is released here as well.
          const drained = opened.reduce<Eff<void, unknown>>(
            (acc, handle) =>
              acc
                .flatMap((): Eff<unknown, never> =>
                  handle.fiber === null ? UNIT : awaitFiber(handle.fiber),
                )
                .flatMap(() => releaseInner(handle)),
            UNIT,
          );
          return pending.reduce<Eff<void, unknown>>(
            (acc, inner) => acc.flatMap(() => runFinalizer(inner._finalizer)),
            drained,
          );
        });

        const run = ensuring(
          pullOuter(outer, false)
            .flatMap(() => awaitInners)
            .flatMap(() => close(null))
            .catchAllCause((cause) => {
              const errors = Cause.stripInterrupts(cause);
              if (errors !== null) return close(errors);
              return closed || stopping ? failCause(cause) : close(cause);
            }),
          stopInners,
        );

        // Finalizer failures recorded after the output closed ride along with
        // the failure the consumer observes, or are raised by `shutdown`.
        const finish: Eff<Step<A>, unknown> = suspend(() => {
          if (!observed) {
            observed = true;
            if (failure !== null && unreported !== null) {
              failure = Cause.then(failure, unreported);
              unreported = null;
            }
          }
          return failure === null ? succeed(DONE) : failCause(failure);
        });

        const pull = (): Eff<Step<A>, unknown> =>
          output
            .take()
            .map((chunk): Step<A> => ({
              _tag: "Emit",
              chunk,
              next: new Stream(suspend(pull)),
            }))
            .catch(() => finish);

        const shutdown = (fiber: Fiber<any>): Eff<void, unknown> =>
          suspend(() => {
            stopping = true;
            return interrupt(fiber);
          })
            .flatMap(() => awaitFiber(fiber))
            .flatMap((exit) =>
              suspend(() => {
                const driverErrors =
                  exit._tag === "Failure" ? Cause.stripInterrupts(exit.cause) : null;
                const errors =
                  unreported === null ? driverErrors : andThen(driverErrors, unreported);
                unreported = null;
                return errors === null ? UNIT : failCause(errors);
              }),
            );

        // The driver is owned by the stream finalizer rather than the pulling
        // fiber: pulls may run on short-lived fibers (e.g. `timeout` races each
        // pull), and a structured child would die with that fiber.
        return uninterruptible(
          forkDaemon(run).map((fiber) => {
            driver = fiber;
            session = { pull, shutdown: shutdown(fiber) };
          }),
        ).flatMap(pull);
      }),
    );

  const release: Eff<void, unknown> = suspend(() => {
    const current = session;
    session = null;
    return current === null ? UNIT : current.shutdown;
  });

  return new Stream(
    suspend(() => (session === null ? open() : session.pull())),
    combineFinalizers(release, outer._finalizer),
  );
}
