import { type Eff, type Throws, type ErrorsOf, Suspend, Op } from "../eff.js";
import { type Fiber } from "../fiber.js";
import { type Exit } from "../exit.js";
import {
  sleep,
  timeout,
  timeoutFail,
  timeoutOption,
  onExit,
  race,
  raceFirst,
  raceEither,
  scoped,
  use,
  acquireRelease,
  retry,
  type RetryConfig,
} from "../constructors.js";
import { provide, type ServiceTag, type ProvidedService } from "../service.js";
import { type RetryPolicy } from "../retry-policy.js";
import { type Schedule, repeat, retryWith } from "../schedule.js";
import {
  retryAllBy,
  repeatUntil,
  repeatUntilWithBackoff,
  type RepeatTimeoutError,
} from "../combinators-extra.js";
import { type RetryAllByOptions } from "../combinators-extra.js";
import { withSpan } from "../tracing.js";

declare module "../eff.js" {
  interface Suspend {
    fork<A, S>(this: Eff<A, S>): Eff<Fiber<A, ErrorsOf<S>>, Exclude<S, Throws<unknown>>>;
    withSpan<A, S>(this: Eff<A, S>, name: string, attributes?: Record<string, unknown>): Eff<A, S>;
    forkDaemon<A, S>(this: Eff<A, S>): Eff<Fiber<A, ErrorsOf<S>>, Exclude<S, Throws<unknown>>>;
    ensuring<A, S, S2>(this: Eff<A, S>, finalizer: Eff<unknown, S2>): Eff<A, S | S2>;
    onExit<A, S, S2>(
      this: Eff<A, S>,
      handler: (exit: Exit<unknown, A>) => Eff<unknown, S2>,
    ): Eff<A, S | S2>;
    uninterruptible<A, S>(this: Eff<A, S>): Eff<A, S>;
    interruptible<A, S>(this: Eff<A, S>): Eff<A, S>;
    timeout<A, S, E>(this: Eff<A, S>, ms: number, onTimeout: () => E): Eff<A, S | Throws<E>>;
    timeoutFail<A, S, E>(this: Eff<A, S>, ms: number, onTimeout: () => E): Eff<A, S | Throws<E>>;
    timeoutOption<A, S>(this: Eff<A, S>, ms: number): Eff<A | undefined, S>;
    race<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<A | B, S1 | S2>;
    /** @deprecated Same as `.race`. */
    raceFirst<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<A | B, S1 | S2>;
    raceEither<A, S1, B, S2>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
    ): Eff<{ _tag: "Left"; left: A } | { _tag: "Right"; right: B }, S1 | S2>;
    delay<A, S>(this: Eff<A, S>, ms: number): Eff<A, S>;
    scoped<A, S>(this: Eff<A, S>): Eff<A, S>;
    /**
     * Use this resource in `f`, and release it as soon as `f` is done:
     * `scoped(this.flatMap(f))`.
     */
    use<A, S, B, S2>(this: Eff<A, S>, f: (a: A) => Eff<B, S2>): Eff<B, S | S2>;
    /**
     * Pair this acquire effect with a release function — the release fires
     * when the surrounding `scoped` block ends.
     */
    acquireRelease<A, S, S2>(this: Eff<A, S>, release: (a: A) => Eff<unknown, S2>): Eff<A, S | S2>;
    provide<A, S, T, Name extends string>(
      this: Eff<A, S>,
      tag: ServiceTag<T, Name>,
      impl: NoInfer<T>,
    ): Eff<A, ProvidedService<S, T, Name>>;
    retry<A, S>(this: Eff<A, S>, policy: RetryPolicy | RetryConfig<ErrorsOf<S>>): Eff<A, S>;
    retryAllBy<A, S, E = ErrorsOf<S>>(this: Eff<A, S>, options: RetryAllByOptions<A, E>): Eff<A, S>;
    repeat<A, S>(this: Eff<A, S>, schedule: Schedule<A>): Eff<A, S>;
    /** See the standalone `retryWith`: also retries defects; `while` filters. */
    retryWith<A, S, In = unknown, Out = unknown>(
      this: Eff<A, S>,
      schedule: Schedule<In, Out>,
      opts?: { while?: (error: unknown) => boolean },
    ): Eff<A, S>;
    /**
     * Re-run until `until(value)` returns true. Fixed interval between
     * attempts. Caps via `maxAttempts` + `maxDurationMs`. Both caps produce
     * a typed `RepeatTimeoutError<A>` carrying the last observed value.
     */
    repeatUntil<A, S>(
      this: Eff<A, S>,
      opts: {
        until: (value: A) => boolean;
        intervalMs?: number;
        maxAttempts?: number;
        maxDurationMs?: number;
      },
    ): Eff<A, S | Throws<RepeatTimeoutError<A>>>;
    /**
     * Like `repeatUntil` but the interval doubles each attempt (capped at
     * `maxIntervalMs`). Use for polling flaky async APIs.
     */
    repeatUntilWithBackoff<A, S>(
      this: Eff<A, S>,
      opts: {
        until: (value: A) => boolean;
        initialIntervalMs?: number;
        maxIntervalMs?: number;
        maxAttempts?: number;
        maxDurationMs?: number;
      },
    ): Eff<A, S | Throws<RepeatTimeoutError<A>>>;
    when<A, S>(this: Eff<A, S>, cond: () => boolean): Eff<A | undefined, S>;
    unless<A, S>(this: Eff<A, S>, cond: () => boolean): Eff<A | undefined, S>;
  }
}

Suspend.prototype.withSpan = function (name: string, attributes?: Record<string, unknown>) {
  return withSpan(this, name, attributes);
};

Suspend.prototype.fork = function (this: Suspend) {
  return new Suspend(Op.Fork, this, null) as Eff<unknown, unknown>;
} as Suspend["fork"];

Suspend.prototype.forkDaemon = function (this: Suspend) {
  return new Suspend(Op.ForkDaemon, this, null) as Eff<unknown, unknown>;
} as Suspend["forkDaemon"];

Suspend.prototype.ensuring = function (this: Suspend, finalizer: Suspend) {
  return new Suspend(Op.Ensuring, this, finalizer) as Eff<unknown, unknown>;
} as Suspend["ensuring"];

Suspend.prototype.onExit = function (handler) {
  return onExit(this, handler);
};

Suspend.prototype.uninterruptible = function (this: Suspend) {
  return new Suspend(Op.SetInterruptible, this, false) as Eff<unknown, unknown>;
} as Suspend["uninterruptible"];

Suspend.prototype.interruptible = function (this: Suspend) {
  return new Suspend(Op.SetInterruptible, this, true) as Eff<unknown, unknown>;
} as Suspend["interruptible"];

Suspend.prototype.timeout = function (ms, onTimeout) {
  return timeout(this, ms, onTimeout);
};

Suspend.prototype.timeoutFail = function (ms, onTimeout) {
  return timeoutFail(this, ms, onTimeout);
};

Suspend.prototype.timeoutOption = function (ms) {
  return timeoutOption(this, ms);
};

Suspend.prototype.race = function (that) {
  return race([this, that]);
};

Suspend.prototype.raceFirst = function (that) {
  return raceFirst([this, that]);
};

Suspend.prototype.raceEither = function (that) {
  return raceEither(this, that);
};

Suspend.prototype.delay = function (this: Suspend, ms: number) {
  return new Suspend(Op.FlatMap, sleep(ms), () => this) as Eff<unknown, unknown>;
} as Suspend["delay"];

Suspend.prototype.scoped = function () {
  return scoped(this);
};

Suspend.prototype.use = function <A, S, B, S2>(this: Eff<A, S>, f: (a: A) => Eff<B, S2>) {
  return use(this, f);
};

Suspend.prototype.acquireRelease = function (release) {
  return acquireRelease(this, release);
};

Suspend.prototype.provide = function (tag, impl) {
  return provide(this, tag, impl);
};

Suspend.prototype.retry = function <A, S>(
  this: Eff<A, S>,
  policy: RetryPolicy | RetryConfig<ErrorsOf<S>>,
) {
  // retry() takes either shape at runtime; only its overloads split them.
  return retry(this, policy as RetryConfig<ErrorsOf<S>>);
};

Suspend.prototype.retryAllBy = function <A, S, E>(
  this: Eff<A, S>,
  options: RetryAllByOptions<A, E>,
) {
  return retryAllBy(this, options as RetryAllByOptions<A, ErrorsOf<S>>);
};

Suspend.prototype.repeat = function (schedule) {
  return repeat(this, schedule);
};

Suspend.prototype.retryWith = function (schedule, opts) {
  return retryWith(this, schedule, opts);
};

Suspend.prototype.repeatUntil = function (opts) {
  return repeatUntil(this, opts);
};

Suspend.prototype.repeatUntilWithBackoff = function (opts) {
  return repeatUntilWithBackoff(this, opts);
};

Suspend.prototype.when = function (cond) {
  return (new Suspend(Op.Sync, () => cond(), null) as Eff<boolean, never>).flatMap((c) =>
    c ? this : (new Suspend(Op.Succeed, undefined, null) as Eff<undefined, never>),
  );
};

Suspend.prototype.unless = function (cond) {
  return (new Suspend(Op.Sync, () => cond(), null) as Eff<boolean, never>).flatMap((c) =>
    c ? (new Suspend(Op.Succeed, undefined, null) as Eff<undefined, never>) : this,
  );
};
