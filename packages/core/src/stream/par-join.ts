import type { Eff } from "../eff.js";
import {
  awaitFiber,
  ensuring,
  failCause,
  onExit,
  succeed,
  suspend,
  sync,
  uninterruptible,
  uninterruptibleMask,
} from "../constructors.js";
import { Cause } from "../cause.js";
import { InProcessDeferred } from "../deferred.js";
import type { Exit } from "../exit.js";
import type { Fiber } from "../fiber.js";
import { Queue } from "../queue.js";
import { Semaphore } from "../semaphore.js";
import type { Chunk } from "./chunk.js";
import type { Stream, Step } from "./stream.js";
import { driverStream, type DriverRun, type Pull } from "./driver-lifecycle.js";

const OUTPUT_CAPACITY = 16;
const UNIT: Eff<void, never> = succeed(undefined);
const DONE: Step<never> = { _tag: "Done" };

type AnyStream<A> = Stream<A, unknown>;

interface InnerHandle {
  fiber: Fiber<any> | null;
  finalizer: Eff<void, unknown> | null;
}

const parallel = (first: Cause | null, second: Cause | null): Cause | null =>
  first === null ? second : second === null ? first : Cause.both(first, second);

const failureOf = (exit: Exit<unknown, unknown>): Cause | null =>
  exit._tag === "Failure" ? Cause.stripInterrupts(exit.cause) : null;

const interruptedOnly = (exit: Exit<unknown, unknown>): boolean =>
  exit._tag === "Failure" && Cause.isInterruptedOnly(exit.cause);

export function parJoinStreams<A>(params: {
  outer: Stream<AnyStream<A>, unknown>;
  maxOpen: number;
}): Stream<A, unknown> {
  const { outer, maxOpen } = params;

  const start = (run: DriverRun<A>): Eff<Pull<A>, unknown> =>
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
        // Set once the join fails or starts tearing down; nothing new is
        // launched after that.
        let ended = false;
        let failure: Cause | null = null;
        // Failures raised after `failure` while the join tears down.
        let reportedTeardown: Cause | null = null;
        let releaseFailures: Cause | null = null;
        let closedWith: Cause | null = null;

        const noteFailure = (cause: Cause): void => {
          if (ended && failure !== null) {
            reportedTeardown = parallel(reportedTeardown, cause);
            return;
          }
          ended = true;
          failure = cause;
        };

        // Interrupting the driver tears the join down without waiting for the
        // consumer to reach the failure.
        const failJoin = (cause: Cause): void => {
          const first = !ended;
          noteFailure(cause);
          if (first) driver?.interrupt();
        };

        const releaseInner = (handle: InnerHandle): Eff<void, unknown> =>
          suspend(() => {
            const finalizer = handle.finalizer;
            handle.finalizer = null;
            return finalizer ?? UNIT;
          });

        const publish = (inner: AnyStream<A>): Eff<void, unknown> =>
          inner.step.flatMap((step) => {
            if (step._tag === "Done") return UNIT;
            if (step.chunk.isEmpty) return publish(step.next);
            return output.offer(step.chunk).flatMap(() => publish(step.next));
          });

        // An interruption the inner raised on its own, not one from teardown,
        // must not pass for completion.
        const noteInnerCause = (cause: Cause): void => {
          const errors = Cause.stripInterrupts(cause);
          if (errors !== null) failJoin(errors);
          else if (!ended) failJoin(cause);
        };

        const innerDone = (handle: InnerHandle): Eff<void, never> =>
          suspend(() => {
            handles.delete(handle);
            if (!outerDone || handles.size > 0) return releasePermit;
            return idle.succeed(undefined).flatMap(() => releasePermit);
          });

        // The handler runs in an uninterruptible region and only does
        // synchronous bookkeeping, so it also sees the whole cause of an inner
        // interrupted by the join's own teardown (typically its finalizer
        // failing). While the run stops, the cause is raised for the stop.
        const runInner = (inner: AnyStream<A>, handle: InnerHandle): Eff<unknown, unknown> =>
          onExit(
            uninterruptibleMask((restore) =>
              restore(ensuring(publish(inner), releaseInner(handle))).catchAllCause((cause) =>
                run.stopping ? failCause(cause) : sync(() => noteInnerCause(cause)),
              ),
            ),
            () => innerDone(handle),
          );

        // Registration is uninterruptible so teardown never misses a forked inner.
        const launchNext: Eff<void, unknown> = uninterruptible(
          suspend(() => {
            if (ended || backlog === null) return releasePermit;
            const inner = backlog.get(backlogIndex++);
            if (backlogIndex >= backlog.length) backlog = null;
            const handle: InnerHandle = { fiber: null, finalizer: inner._finalizer };
            handles.add(handle);
            return run.fork(runInner(inner, handle)).map((fiber) => {
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
            if (ended) return UNIT;
            if (backlog === null) return pullOuter(next, false);
            return acquire.flatMap(() => launchBacklog(next));
          });

        const awaitInners: Eff<void, unknown> = suspend(() => {
          outerDone = true;
          return handles.size === 0 ? UNIT : idle.await;
        });

        const noteDriverExit = (exit: Exit<unknown, void>): Eff<void, never> =>
          sync(() => {
            const errors = failureOf(exit);
            if (errors !== null) noteFailure(errors);
            else if (interruptedOnly(exit) && !ended && !run.stopping) {
              noteFailure((exit as { cause: Cause }).cause);
            }
          });

        // Every opened inner is interrupted and awaited, then the finalizers
        // no inner fiber ran (never started, or never launched) are released.
        // Each release runs even when an earlier one fails.
        const stopInners: Eff<void, unknown> = suspend(() => {
          ended = true;
          const opened = Array.from(handles);
          const pending = backlog === null ? [] : backlog.toArray().slice(backlogIndex);
          backlog = null;
          for (const handle of opened) handle.fiber?.interrupt();
          const released = opened.reduce<Eff<void, unknown>>(
            (acc, handle) =>
              ensuring(
                acc,
                suspend((): Eff<unknown, never> =>
                  handle.fiber === null ? UNIT : awaitFiber(handle.fiber),
                ).flatMap(() => releaseInner(handle)),
              ),
            UNIT,
          );
          return pending.reduce<Eff<void, unknown>>(
            (acc, inner) => ensuring(acc, inner._finalizer ?? UNIT),
            released,
          );
        });

        // The consumer sees the first failure followed by the failures raised
        // while tearing down. When the run is stopping instead, failures that
        // inner fibers reported rather than raised go to the stop.
        const closeOutput = (): Eff<void, unknown> =>
          suspend(() => {
            const teardown = parallel(reportedTeardown, releaseFailures);
            closedWith =
              failure === null
                ? teardown
                : teardown === null
                  ? failure
                  : Cause.then(failure, teardown);
            return output
              .close()
              .flatMap(() =>
                run.stopping && reportedTeardown !== null ? failCause(reportedTeardown) : UNIT,
              );
          });

        // Failures here were already noted for the consumer, or are raised
        // again by `reportFailure` for the stop when the run is stopping.
        const drive: Eff<void, unknown> = run.reportFailure(
          onExit(
            ensuring(
              onExit(
                pullOuter(outer, false).flatMap(() => awaitInners),
                noteDriverExit,
              ),
              onExit(stopInners, (exit) =>
                sync(() => {
                  releaseFailures = failureOf(exit);
                }),
              ),
            ),
            closeOutput,
          ),
          () => UNIT,
        );

        const pull = (): Eff<Step<A>, unknown> =>
          output
            .take()
            .map((chunk): Step<A> => ({ _tag: "Emit", chunk, next }))
            .catch(() => (closedWith === null ? succeed(DONE) : failCause(closedWith)));
        const next = run.continueWith(pull);

        return run.fork(drive).map((fiber) => {
          driver = fiber;
          return pull;
        });
      }),
    );

  return driverStream<A>({ start, finalizer: outer._finalizer });
}
