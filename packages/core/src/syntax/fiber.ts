import { type Eff, type Throws, type Needs, Suspend, Op } from "../eff";
import { type Fiber } from "../fiber";
import { type Exit } from "../exit";
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
  acquireRelease,
  retry,
  type RetryConfig,
} from "../constructors";
import { provide, type ServiceTag } from "../service";
import { type RetryPolicy } from "../retry-policy";
import { type Schedule, repeat, retryWith } from "../schedule";
import { repeatUntil, repeatUntilWithBackoff, type RepeatTimeoutError } from "../combinators-extra";
import { withSpan } from "../tracing";

declare module "../eff" {
  interface Suspend {
    fork<A, S>(this: Eff<A, S>): Eff<Fiber<A>, never>;
    withSpan<A, S>(this: Eff<A, S>, name: string, attributes?: Record<string, unknown>): Eff<A, S>;
    forkDaemon<A, S>(this: Eff<A, S>): Eff<Fiber<A>, never>;
    ensuring<A, S>(this: Eff<A, S>, finalizer: Eff<void, never>): Eff<A, S>;
    onExit<A, S, S2>(
      this: Eff<A, S>,
      handler: (exit: Exit<unknown, A>) => Eff<void, S2>,
    ): Eff<A, S | S2>;
    uninterruptible<A, S>(this: Eff<A, S>): Eff<A, S>;
    interruptible<A, S>(this: Eff<A, S>): Eff<A, S>;
    timeout<A, S, E>(this: Eff<A, S>, ms: number, onTimeout: () => E): Eff<A, S | Throws<E>>;
    timeoutFail<A, S, E>(this: Eff<A, S>, ms: number, onTimeout: () => E): Eff<A, S | Throws<E>>;
    timeoutOption<A, S>(this: Eff<A, S>, ms: number): Eff<A | undefined, S>;
    race<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<A | B, S1 | S2>;
    raceFirst<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<A | B, S1 | S2>;
    raceEither<A, S1, B, S2>(
      this: Eff<A, S1>,
      that: Eff<B, S2>,
    ): Eff<{ _tag: "Left"; left: A } | { _tag: "Right"; right: B }, S1 | S2>;
    delay<A, S>(this: Eff<A, S>, ms: number): Eff<A, S>;
    scoped<A, S>(this: Eff<A, S>): Eff<A, S>;
    /**
     * Pair this acquire effect with a release function — the release fires
     * when the surrounding `scoped` block ends.
     */
    acquireRelease<A, S>(this: Eff<A, S>, release: (a: A) => Eff<void, never>): Eff<A, S>;
    provide<A, S, T>(
      this: Eff<A, S | Needs<T>>,
      tag: ServiceTag<T>,
      impl: T,
    ): Eff<A, Exclude<S, Needs<T>>>;
    retry<A, S>(this: Eff<A, S>, policy: RetryPolicy | RetryConfig<any>): Eff<A, S>;
    repeat<A, S>(this: Eff<A, S>, schedule: Schedule<any>): Eff<A, S>;
    retryWith<A, S>(
      this: Eff<A, S>,
      schedule: Schedule<any>,
      opts?: { while?: (e: any) => boolean },
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
  return withSpan(this as any, name, attributes) as any;
};

Suspend.prototype.fork = function () {
  return new Suspend(Op.Fork, this, null) as any;
};

Suspend.prototype.forkDaemon = function () {
  return new Suspend(Op.ForkDaemon, this, null) as any;
};

Suspend.prototype.ensuring = function (finalizer: any) {
  return new Suspend(Op.Ensuring, this, finalizer) as any;
};

Suspend.prototype.onExit = function (handler: any) {
  return onExit(this as any, handler) as any;
};

Suspend.prototype.uninterruptible = function () {
  return new Suspend(Op.SetInterruptible, this, false) as any;
};

Suspend.prototype.interruptible = function () {
  return new Suspend(Op.SetInterruptible, this, true) as any;
};

Suspend.prototype.timeout = function (ms: any, onTimeout: any) {
  return timeout(this as any, ms, onTimeout) as any;
};

Suspend.prototype.timeoutFail = function (ms: any, onTimeout: any) {
  return timeoutFail(this as any, ms, onTimeout) as any;
};

Suspend.prototype.timeoutOption = function (ms: any) {
  return timeoutOption(this as any, ms) as any;
};

Suspend.prototype.race = function (that: any) {
  return race([this as any, that]) as any;
};

Suspend.prototype.raceFirst = function (that: any) {
  return raceFirst([this as any, that]) as any;
};

Suspend.prototype.raceEither = function (that: any) {
  return raceEither(this as any, that) as any;
};

Suspend.prototype.delay = function (ms: any) {
  return new Suspend(Op.FlatMap, sleep(ms), () => this) as any;
};

Suspend.prototype.scoped = function () {
  return scoped(this as any) as any;
};

Suspend.prototype.acquireRelease = function (release: any) {
  return acquireRelease(this as any, release) as any;
};

Suspend.prototype.provide = function (tag: any, impl: any) {
  return provide(this as any, tag, impl) as any;
};

Suspend.prototype.retry = function (policy: any) {
  return retry(this as any, policy) as any;
};

Suspend.prototype.repeat = function (schedule: any) {
  return repeat(this as any, schedule) as any;
};

Suspend.prototype.retryWith = function (schedule: any, opts?: any) {
  return retryWith(this as any, schedule, opts) as any;
};

Suspend.prototype.repeatUntil = function (opts: any) {
  return repeatUntil(this as any, opts) as any;
};

Suspend.prototype.repeatUntilWithBackoff = function (opts: any) {
  return repeatUntilWithBackoff(this as any, opts) as any;
};

Suspend.prototype.when = function (cond: any) {
  return new Suspend(Op.Sync, () => cond(), null).flatMap((c: boolean) =>
    c ? (this as any) : new Suspend(Op.Succeed, undefined, null),
  ) as any;
};

Suspend.prototype.unless = function (cond: any) {
  return new Suspend(Op.Sync, () => cond(), null).flatMap((c: boolean) =>
    c ? new Suspend(Op.Succeed, undefined, null) : (this as any),
  ) as any;
};
