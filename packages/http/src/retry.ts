// Retry with outcome ADT.
//
// Core design (from promin, proven in production): normalize every possible
// outcome into a `PipelineResult<T>` tagged union FIRST, then decide whether
// to retry via a single `shouldRetry(result) => boolean` predicate. This lets
// callers retry on:
//   - HTTP errors (5xx, 429, timeouts)
//   - Thrown defects from downstream transforms
//   - "Not ready" success values (e.g. polling a job status)
//
// Two wrappers:
//   `withRetry(eff, opts?)`      — retry transient HTTP errors only (default)
//   `withRetryAll(eff, opts?)`   — retry with full outcome visibility
//
// Both return `Eff<T, Throws<HttpClientError>>`. Thrown defects remain
// defects; HTTP errors remain typed; success values succeed.

import {
  type Eff,
  type Throws,
  type CauseT,
  Cause,
  succeed,
  fail,
  failCause,
  die,
  sleep,
} from "@perfect/core";
import { type HttpClientError, HTTP_RETRYABLE } from "./errors";

// ── PipelineResult ADT ─────────────────────────────────────────────

export type PipelineResult<T> =
  | { readonly _tag: "success"; readonly value: T }
  | { readonly _tag: "httpError"; readonly error: HttpClientError }
  | { readonly _tag: "thrown"; readonly error: unknown };

export const PipelineResult = {
  success: <T>(value: T): PipelineResult<T> => ({ _tag: "success", value }),
  httpError: <T = never>(error: HttpClientError): PipelineResult<T> => ({
    _tag: "httpError",
    error,
  }),
  thrown: <T = never>(error: unknown): PipelineResult<T> => ({ _tag: "thrown", error }),
  isSuccess: <T>(r: PipelineResult<T>): r is { _tag: "success"; value: T } => r._tag === "success",
  isHttpError: <T>(r: PipelineResult<T>): r is { _tag: "httpError"; error: HttpClientError } =>
    r._tag === "httpError",
  isThrown: <T>(r: PipelineResult<T>): r is { _tag: "thrown"; error: unknown } =>
    r._tag === "thrown",
} as const;

// ── withRetryAll ───────────────────────────────────────────────────

export interface RetryAllOptions<T = unknown> {
  /** Max number of retries (excluding the initial attempt). Default: 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff in ms. Default: 250. */
  readonly baseDelayMs?: number;
  /** Cap on per-attempt delay in ms. Default: 30 000. */
  readonly maxDelayMs?: number;
  /**
   * Decide whether to retry. Default: retry anything that isn't a success.
   * Override to:
   *   - poll a job: `(r) => !PipelineResult.isSuccess(r) || r.value.status !== "done"`
   *   - retry only thrown defects: `PipelineResult.isThrown`
   *   - retry only transient HTTP: `(r) => PipelineResult.isHttpError(r) && HTTP_RETRYABLE(r.error)`
   */
  readonly shouldRetry?: (result: PipelineResult<T>) => boolean;
}

const DEFAULT_SHOULD_RETRY = <T>(r: PipelineResult<T>): boolean => r._tag !== "success";

/**
 * Retry that observes every outcome. The `shouldRetry` predicate sees the
 * full `PipelineResult<T>` and decides what to retry.
 */
export function withRetryAll<T>(
  eff: Eff<T, Throws<HttpClientError>>,
  options: RetryAllOptions<T> = {},
): Eff<T, Throws<HttpClientError>> {
  const {
    maxRetries = 3,
    baseDelayMs = 250,
    maxDelayMs = 30_000,
    shouldRetry = DEFAULT_SHOULD_RETRY,
  } = options;

  // Normalize: map success/failure/defect into PipelineResult<T> on the success channel.
  // Interrupts propagate unchanged (we don't retry cancellations).
  const normalized = (eff as any)
    .map((value: T) => PipelineResult.success(value))
    .catchAllCause((cause: CauseT): Eff<PipelineResult<T>, never> => {
      const f = Cause.firstFail(cause);
      if (f !== null) return succeed(PipelineResult.httpError(f.value as HttpClientError));
      const d = Cause.firstDie(cause);
      if (d !== null) return succeed(PipelineResult.thrown(d.value));
      // Interrupt (or composite of interrupts) — propagate
      return failCause(cause) as any;
    }) as Eff<PipelineResult<T>, never>;

  const unwrap = (r: PipelineResult<T>): Eff<T, Throws<HttpClientError>> => {
    switch (r._tag) {
      case "success":
        return succeed(r.value);
      case "httpError":
        return fail(r.error) as Eff<T, Throws<HttpClientError>>;
      case "thrown":
        return die(r.error) as Eff<T, Throws<HttpClientError>>;
    }
  };

  const loop = (attempt: number, wait: number): Eff<T, Throws<HttpClientError>> =>
    (normalized as any).flatMap((r: PipelineResult<T>) => {
      if (!shouldRetry(r) || attempt >= maxRetries) return unwrap(r);
      return (sleep(wait) as any).flatMap(() => loop(attempt + 1, Math.min(wait * 2, maxDelayMs)));
    }) as Eff<T, Throws<HttpClientError>>;

  return loop(0, baseDelayMs);
}

// ── withRetry (HTTP-aware default) ────────────────────────────────

export interface RetryOptions {
  /** Max retries (excluding initial attempt). Default: 3. */
  readonly maxRetries?: number;
  /** Base exponential backoff delay. Default: 250ms. */
  readonly baseDelayMs?: number;
  /** Per-attempt cap. Default: 30 000ms. */
  readonly maxDelayMs?: number;
  /**
   * Predicate to decide if a typed HTTP error should retry. Default:
   * `HTTP_RETRYABLE` — 5xx, 429, timeouts, network errors.
   */
  readonly when?: (error: HttpClientError) => boolean;
}

/**
 * Retry on transient HTTP errors only. Convenience wrapper around
 * `withRetryAll` that ignores thrown defects and success values.
 */
export function withRetry<T>(
  eff: Eff<T, Throws<HttpClientError>>,
  options: RetryOptions = {},
): Eff<T, Throws<HttpClientError>> {
  const { when = HTTP_RETRYABLE } = options;
  return withRetryAll(eff, {
    maxRetries: options.maxRetries,
    baseDelayMs: options.baseDelayMs,
    maxDelayMs: options.maxDelayMs,
    shouldRetry: (r) => PipelineResult.isHttpError(r) && when(r.error),
  });
}
