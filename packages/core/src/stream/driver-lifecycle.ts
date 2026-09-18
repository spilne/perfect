import { type Eff, Suspend, Op } from "../eff";
import {
  suspend,
  succeed,
  sync,
  failCause,
  awaitFiber,
  uninterruptible,
  interruptible,
} from "../constructors";
import type { Exit } from "../exit";
import { Cause } from "../cause";
import type { Fiber } from "../fiber";
import { emptyContext } from "../service";
import { Stream, type Step } from "./stream";

const UNIT: Eff<void, never> = succeed(undefined);
const DONE: Step<never> = { _tag: "Done" };

const NO_CAUSE: Eff<Cause | null, never> = succeed(null);

const sequential = (first: Cause | null, second: Cause | null): Cause | null =>
  first === null ? second : second === null ? first : Cause.then(first, second);

const parallel = (first: Cause | null, second: Cause | null): Cause | null =>
  first === null ? second : second === null ? first : Cause.both(first, second);

/** Interrupt the fibers, await them all, and return their teardown failures. */
function stopFibers(fibers: readonly Fiber<any>[]): Eff<Cause | null, never> {
  if (fibers.length === 0) return NO_CAUSE;
  for (const fiber of fibers) fiber.interrupt();
  return fibers.reduce<Eff<Cause | null, never>>(
    (acc, fiber) =>
      acc.flatMap((failure) =>
        awaitFiber(fiber).map((exit) =>
          exit._tag === "Failure" ? parallel(failure, Cause.stripInterrupts(exit.cause)) : failure,
        ),
      ),
    NO_CAUSE,
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

// ── Pull attempts ──────────────────────────────────────────────────
//
// `Stream.retry` runs every attempt of one retry loop with the same attempt
// token in the fiber context, prepended to the tokens of enclosing loops. A
// first pull resumes (or re-fails) a run only when it shares a token with the
// run's last pull, so running a stream again (`concat`, `catch`, a second
// consumer) starts a fresh run instead of adopting an interrupted one.

type PullAttempts = readonly object[];

const PULL_ATTEMPTS_KEY = Symbol.for("spilne/stream/pull-attempts");
const NO_ATTEMPTS: PullAttempts = [];
if (!emptyContext.has(PULL_ATTEMPTS_KEY)) emptyContext.set(PULL_ATTEMPTS_KEY, NO_ATTEMPTS);

const currentAttempts: Eff<PullAttempts, never> = new Suspend(
  Op.GetCtx,
  PULL_ATTEMPTS_KEY,
  null,
) as any;

/**
 * Run `retryLoop(attempt)` with a fresh attempt token visible to every attempt
 * the loop makes. `attempt` wraps the effect each attempt runs.
 */
export function withPullAttempts<A, S>(
  retryLoop: (attempt: <B, S2>(eff: Eff<B, S2>) => Eff<B, S2>) => Eff<A, S>,
): Eff<A, S> {
  return new Suspend(Op.FlatMap, currentAttempts, (outer: PullAttempts) => {
    const context = new Map<symbol, unknown>([[PULL_ATTEMPTS_KEY, [{}, ...outer]]]);
    return retryLoop((eff) => new Suspend(Op.Provide, eff, context) as any);
  }) as any;
}

const sharesAttempt = (a: PullAttempts, b: PullAttempts): boolean =>
  a.some((token) => b.includes(token));

/** The fibers and pull state of one run of a concurrent stream operator. */
export interface DriverRun<A> {
  /**
   * Fork a background fiber owned by this run. It outlives the pull that
   * forks it and is interrupted and awaited when the run stops.
   */
  fork<B>(eff: Eff<B, unknown>): Eff<Fiber<B>, never>;
  /**
   * Run a background fiber's `body`, handing a failure to `report` (which
   * typically offers it to the consumer). Once the run is stopping, the full
   * cause is re-raised instead, so the stop can collect failures such as a
   * finalizer that failed while its fiber was interrupted.
   *
   * Only the stop interrupts these fibers, so before it an interruption is
   * one the body raised itself, and it is reported like any other failure
   * rather than passing for teardown with nobody left to see it.
   *
   * An interrupted fiber skips interruptible error handlers and drops the
   * typed failures they would have seen, so the handler is installed in an
   * uninterruptible region. `body` and `report` run interruptibly, so a
   * blocked offer can still be interrupted. Use it only at the top of a fiber
   * forked with `fork`, which starts interruptible. An operator that
   * interrupts its own fibers outside the stop needs its own bookkeeping.
   */
  reportFailure<B>(
    body: Eff<B, unknown>,
    report: (cause: Cause) => Eff<B, unknown>,
  ): Eff<B, unknown>;
  /** True once the run has started stopping its fibers. */
  readonly stopping: boolean;
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
  // Retry-loop tokens the last pull ran under, and whether it was interrupted.
  private attempts: PullAttempts = NO_ATTEMPTS;
  private interrupted = false;
  private delivered = false;
  failure: Cause | null = null;
  private finished = false;
  private abandoned = false;

  readonly end: Stream<A, unknown> = new Stream(
    sync(() => {
      this.finished = true;
      return DONE;
    }),
  );

  private stopRequested = false;

  get stopping(): boolean {
    return this.stopRequested;
  }

  reportFailure<B>(
    body: Eff<B, unknown>,
    report: (cause: Cause) => Eff<B, unknown>,
  ): Eff<B, unknown> {
    // Background fibers start interruptible, so "restore" is interruptible.
    return uninterruptible(
      new Suspend(Op.CatchAll, interruptible(body), (cause: Cause) =>
        this.stopRequested ? failCause(cause) : interruptible(report(cause)),
      ) as Eff<B, unknown>,
    );
  }

  /**
   * Interrupt and await every fiber, returning their teardown failures. A
   * fiber registered once stopping has begun (forked by a pull that outlived
   * the stop, or registered after the stop took its snapshot) is interrupted
   * on registration, and the stop waits until none is left.
   */
  readonly stop: Eff<Cause | null, never> = suspend(() => {
    this.stopRequested = true;
    let failure: Cause | null = null;
    const drain = (): Eff<Cause | null, never> =>
      suspend(() =>
        this.fibers.size === 0
          ? succeed(failure)
          : stopFibers(Array.from(this.fibers)).flatMap((cause) => {
              failure = sequential(failure, cause);
              return drain();
            }),
      );
    return drain();
  });

  /** The first pull, retried under one of these attempt tokens, may resume this run. */
  retriedBy(attempts: PullAttempts): boolean {
    return attempts.length > 0 && sharesAttempt(this.attempts, attempts);
  }

  /** Started and never delivered a step, so its first pull can run again. */
  get resumable(): boolean {
    return this.first !== null && !this.delivered && !this.finished;
  }

  /** Completed, failed, never started, or interrupted with no retry to resume it. */
  get stale(): boolean {
    return (
      this.failure !== null ||
      this.finished ||
      this.abandoned ||
      (this.interrupted && this.pending === 0 && this.attempts.length === 0)
    );
  }

  private readonly register = (fiber: Fiber<any>): Eff<Fiber<any>, never> => {
    this.fibers.add(fiber);
    fiber.onComplete(() => this.fibers.delete(fiber));
    if (this.stopRequested) fiber.interrupt();
    return succeed(fiber);
  };

  fork<B>(eff: Eff<B, unknown>): Eff<Fiber<B>, never> {
    // Registration is atomic with the fork so a stop never misses a fiber.
    return uninterruptible(
      new Suspend(Op.FlatMap, new Suspend(Op.ForkDaemon, eff, null), this.register) as any,
    ) as Eff<Fiber<B>, never>;
  }

  pull(pull: Pull<A>): Eff<Step<A>, unknown> {
    return new Suspend(Op.FlatMap, currentAttempts, (attempts: PullAttempts) => {
      if (this.failure !== null) return failCause(this.failure);
      const id = ++this.pulls;
      this.pending = id;
      this.attempts = attempts;
      this.interrupted = false;
      // An Exit finalizer, not an error handler: an interrupted pull skips
      // error handlers, but its bookkeeping must still run.
      const settle = (exit: Exit<unknown, Step<A>>): null => {
        if (this.pending !== id) return null;
        this.pending = 0;
        if (exit._tag === "Success") {
          this.delivered = true;
          if (exit.value._tag === "Done") this.finished = true;
        } else {
          if (this.first === null) this.abandoned = true;
          if (Cause.hasInterrupt(exit.cause)) this.interrupted = true;
          else this.failure = exit.cause;
        }
        return null;
      };
      return new Suspend(Op.Ensuring, pull(), settle);
    }) as Eff<Step<A>, unknown>;
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
 * returns the consumer pull. It runs on the first pull. Under `Stream.retry`:
 * - a pull interrupted before it delivered anything (`timeout(ms).retry()`)
 *   resumes the same run when retried, without a second set of fibers;
 * - a failure that reached the consumer fails every retried pull of that run
 *   again, the first pull included, because the work that failed ran in a
 *   background fiber and cannot be run again from the consumer.
 * Any other first pull, such as running the stream again after `catch` or in
 * `concat`, starts a fresh run and stops the fibers of runs that completed,
 * failed, or were interrupted outside a retry.
 *
 * The finalizer stops the fibers, so these streams must be consumed through
 * a terminal operator (or have `_finalizer` run): pulling `step` by hand
 * without finalizing leaves them running.
 */
export function driverStream<A>(params: {
  start: (run: DriverRun<A>) => Eff<Pull<A>, unknown>;
  finalizer: Eff<void, unknown> | null;
}): Stream<A, unknown> {
  const { start, finalizer } = params;
  const runs = new Set<Run<A>>();

  // Runs stay registered until their fibers have stopped, so an interrupted
  // stop leaves them to the next attempt or the stream finalizer.
  const stopRuns = (stopped: readonly Run<A>[]): Eff<void, unknown> =>
    stopped
      .reduce<Eff<Cause | null, never>>(
        (acc, run) => acc.flatMap((failure) => run.stop.map((cause) => sequential(failure, cause))),
        NO_CAUSE,
      )
      .flatMap((failure) => {
        for (const run of stopped) runs.delete(run);
        return failure === null ? UNIT : failCause(failure);
      });

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

  const firstPull: Eff<Step<A>, unknown> = new Suspend(
    Op.FlatMap,
    currentAttempts,
    (attempts: PullAttempts) => {
      for (const run of runs) {
        if (!run.retriedBy(attempts)) continue;
        if (run.failure !== null) return failCause(run.failure);
        if (run.resumable) return run.pull(run.first!);
      }
      const stale = Array.from(runs).filter((run) => run.stale);
      return stale.length === 0 ? begin() : stopRuns(stale).flatMap(begin);
    },
  ) as any;

  const stop: Eff<void, unknown> = suspend(() => stopRuns(Array.from(runs)));

  return new Stream(firstPull, combineFinalizers(stop, finalizer));
}
