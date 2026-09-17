import { type Eff, Suspend, Op } from "../eff";
import { suspend, succeed, sync, failCause, awaitFiber, uninterruptible } from "../constructors";
import { Cause } from "../cause";
import type { Fiber } from "../fiber";
import { Stream, type Step } from "./stream";

const UNIT: Eff<void, never> = succeed(undefined);
const DONE: Step<never> = { _tag: "Done" };

function stopFibers(fibers: readonly Fiber<any>[]): Eff<void, never> {
  if (fibers.length === 0) return UNIT;
  for (const fiber of fibers) fiber.interrupt();
  return fibers.reduce<Eff<void, never>>(
    (acc, fiber) => acc.flatMap(() => awaitFiber(fiber)).map(() => undefined),
    UNIT,
  );
}

export function combineFinalizers(
  first: Eff<void, unknown> | null,
  second: Eff<void, unknown> | null,
): Eff<void, unknown> | null {
  if (first === null) return second;
  if (second === null) return first;
  return new Suspend(Op.Ensuring, first, second) as any;
}

export type Pull<A> = () => Eff<Step<A>, unknown>;

/** The fibers and pull state of one run of a concurrent stream operator. */
export interface DriverRun<A> {
  /**
   * Fork a background fiber owned by this run. It outlives the pull that
   * forks it and is interrupted and awaited when the run stops.
   */
  fork<B>(eff: Eff<B, unknown>): Eff<Fiber<B>, never>;
  /** A continuation whose pull belongs to this run. */
  continueWith(pull: Pull<A>): Stream<A, unknown>;
  /** A continuation that completes this run. */
  readonly end: Stream<A, unknown>;
}

class Run<A> implements DriverRun<A> {
  private readonly fibers = new Set<Fiber<any>>();
  first: Pull<A> | null = null;
  // Id of the pull in flight, 0 when none. Pulls of one run are sequential; an
  // interrupted pull that settles late, or replays its continuation, carries a
  // stale id and is ignored.
  private pending = 0;
  private pulls = 0;
  private delivered = false;
  private failure: Cause | null = null;
  private finished = false;
  private abandoned = false;

  readonly end: Stream<A, unknown> = new Stream(
    sync(() => {
      this.finished = true;
      return DONE;
    }),
  );

  readonly stop: Eff<void, never> = suspend(() => stopFibers(Array.from(this.fibers)));

  /** Started, then every pull so far was interrupted before delivering a step. */
  get resumable(): boolean {
    return this.first !== null && this.pending === 0 && !this.delivered && !this.ended;
  }

  get ended(): boolean {
    return this.failure !== null || this.finished || this.abandoned;
  }

  private readonly register = (fiber: Fiber<any>): Eff<Fiber<any>, never> => {
    this.fibers.add(fiber);
    fiber.onComplete(() => this.fibers.delete(fiber));
    return succeed(fiber);
  };

  fork<B>(eff: Eff<B, unknown>): Eff<Fiber<B>, never> {
    // Registration is atomic with the fork so a stop never misses a fiber.
    return uninterruptible(
      new Suspend(Op.FlatMap, new Suspend(Op.ForkDaemon, eff, null), this.register) as any,
    ) as Eff<Fiber<B>, never>;
  }

  pull(pull: Pull<A>): Eff<Step<A>, unknown> {
    return suspend(() => {
      if (this.failure !== null) return failCause(this.failure);
      const id = ++this.pulls;
      this.pending = id;
      const onStep = (step: Step<A>): Eff<Step<A>, never> => {
        if (this.pending === id) {
          this.pending = 0;
          this.delivered = true;
          if (step._tag === "Done") this.finished = true;
        }
        return succeed(step);
      };
      const onCause = (cause: Cause): Eff<never, unknown> => {
        if (this.pending === id) {
          this.pending = 0;
          if (this.first === null) this.abandoned = true;
          if (!Cause.isInterruptedOnly(cause)) this.failure = cause;
        }
        return failCause(cause);
      };
      return new Suspend(Op.CatchAll, new Suspend(Op.FlatMap, pull(), onStep), onCause) as Eff<
        Step<A>,
        unknown
      >;
    });
  }

  continueWith(pull: Pull<A>): Stream<A, unknown> {
    return new Stream(this.pull(pull));
  }
}

/**
 * Build a stream whose operator runs background fibers. The fibers belong to
 * the stream's run and are stopped by its finalizer, not by the fiber that
 * happens to execute a pull: operators such as `timeout`, `interruptAfter`
 * or `takeUntil` run each pull on a short-lived race fiber, and a structured
 * child forked there would be interrupted as soon as that pull settled.
 *
 * `start` sets up the operator, forks its fibers through `run.fork`, and
 * returns the consumer pull. It runs on the first pull. When the first pull
 * runs again after being interrupted before it delivered anything (e.g.
 * `timeout(ms).retry(policy)`), it resumes that run instead of starting a
 * second set of fibers. Any other first pull starts a separate run, stopping
 * the leftover fibers of runs that already completed or failed; a later pull
 * of a failed run fails again with the same cause.
 */
export function driverStream<A>(params: {
  start: (run: DriverRun<A>) => Eff<Pull<A>, unknown>;
  finalizer: Eff<void, unknown> | null;
}): Stream<A, unknown> {
  const { start, finalizer } = params;
  const runs = new Set<Run<A>>();

  const stopRuns = (stopped: readonly Run<A>[]): Eff<void, never> =>
    stopped.reduce<Eff<void, never>>((acc, run) => acc.flatMap(() => run.stop), UNIT);

  const begin = (): Eff<Step<A>, unknown> => {
    const run = new Run<A>();
    runs.add(run);
    return run.pull(() =>
      uninterruptible(
        suspend(() => start(run)).map((pull) => {
          run.first = pull;
        }),
      ).flatMap(() => run.first!()),
    );
  };

  const firstPull: Eff<Step<A>, unknown> = suspend(() => {
    for (const run of runs) if (run.resumable) return run.pull(run.first!);
    const ended = Array.from(runs).filter((run) => run.ended);
    if (ended.length === 0) return begin();
    for (const run of ended) runs.delete(run);
    return stopRuns(ended).flatMap(begin);
  });

  const stop: Eff<void, never> = suspend(() => {
    const all = Array.from(runs);
    runs.clear();
    return stopRuns(all);
  });

  return new Stream(firstPull, combineFinalizers(stop, finalizer));
}
