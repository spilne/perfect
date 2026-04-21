// Polling helpers — repeatedly run a request until a condition is met.
//
// Two variants:
//   `poll(opts)`             — fixed interval between attempts
//   `pollWithBackoff(opts)`  — exponential interval, capped at maxIntervalMs
//
// Both have dual timeouts: a `maxAttempts` count AND a `maxDurationMs`
// wall-clock cap. Whichever fires first produces a typed `PollTimeoutError`
// with the last observed result attached for diagnostics.

import {
  type Eff,
  type Throws,
  TaggedError,
  succeed,
  fail,
  sleep,
} from "@perfect/core";

export class PollTimeoutError extends TaggedError("PollTimeoutError")<{
  readonly attempts: number;
  readonly lastResult: unknown;
  readonly message: string;
}>() {}

export interface PollOptions<T, E> {
  /** Effect run on each tick. */
  readonly request: Eff<T, Throws<E>>;
  /** Return `true` when the desired condition is met and polling should stop. */
  readonly until: (result: T) => boolean;
  /** Interval between polls in ms. Default: 1 000. */
  readonly intervalMs?: number;
  /** Maximum attempt count. Default: 60. */
  readonly maxAttempts?: number;
  /** Maximum wall-clock duration in ms. Default: 5 minutes. */
  readonly maxDurationMs?: number;
}

/**
 * Poll until `until(result)` is true, or fail with `PollTimeoutError`.
 * Fixed interval between attempts.
 */
export function poll<T, E>(
  options: PollOptions<T, E>,
): Eff<T, Throws<E | PollTimeoutError>> {
  const {
    request,
    until,
    intervalMs = 1000,
    maxAttempts = 60,
    maxDurationMs = 5 * 60 * 1000,
  } = options;

  const loop = (attempt: number, lastResult: T | undefined): Eff<T, Throws<E | PollTimeoutError>> =>
    (request as any).flatMap((result: T) => {
      if (until(result)) return succeed(result);
      if (attempt >= maxAttempts) {
        return fail(
          new PollTimeoutError({
            attempts: maxAttempts,
            lastResult: result,
            message: `Polling exhausted ${maxAttempts} attempts without satisfying condition`,
          }),
        ) as Eff<T, Throws<E | PollTimeoutError>>;
      }
      return (sleep(intervalMs) as any).flatMap(() => loop(attempt + 1, result));
    }) as Eff<T, Throws<E | PollTimeoutError>>;

  return (loop(1, undefined) as any).timeoutFail(
    maxDurationMs,
    (): PollTimeoutError =>
      new PollTimeoutError({
        attempts: maxAttempts,
        lastResult: undefined,
        message: `Polling exceeded max duration of ${maxDurationMs}ms`,
      }),
  ) as Eff<T, Throws<E | PollTimeoutError>>;
}

export interface PollWithBackoffOptions<T, E> extends PollOptions<T, E> {
  /** Starting interval. Default: 500ms. */
  readonly initialIntervalMs?: number;
  /** Maximum interval (cap for exponential doubling). Default: 30 000ms. */
  readonly maxIntervalMs?: number;
}

/**
 * Poll with exponential backoff. Interval starts at `initialIntervalMs`,
 * doubles each attempt, capped at `maxIntervalMs`.
 */
export function pollWithBackoff<T, E>(
  options: PollWithBackoffOptions<T, E>,
): Eff<T, Throws<E | PollTimeoutError>> {
  const {
    request,
    until,
    initialIntervalMs = 500,
    maxIntervalMs = 30_000,
    maxAttempts = 60,
    maxDurationMs = 5 * 60 * 1000,
  } = options;

  const loop = (
    attempt: number,
    interval: number,
  ): Eff<T, Throws<E | PollTimeoutError>> =>
    (request as any).flatMap((result: T) => {
      if (until(result)) return succeed(result);
      if (attempt >= maxAttempts) {
        return fail(
          new PollTimeoutError({
            attempts: maxAttempts,
            lastResult: result,
            message: `Polling with backoff exhausted ${maxAttempts} attempts without satisfying condition`,
          }),
        ) as Eff<T, Throws<E | PollTimeoutError>>;
      }
      const nextInterval = Math.min(interval * 2, maxIntervalMs);
      return (sleep(interval) as any).flatMap(() => loop(attempt + 1, nextInterval));
    }) as Eff<T, Throws<E | PollTimeoutError>>;

  return (loop(1, initialIntervalMs) as any).timeoutFail(
    maxDurationMs,
    (): PollTimeoutError =>
      new PollTimeoutError({
        attempts: maxAttempts,
        lastResult: undefined,
        message: `Polling with backoff exceeded max duration of ${maxDurationMs}ms`,
      }),
  ) as Eff<T, Throws<E | PollTimeoutError>>;
}
