// Retry with attempt outcome ADT.

// Core design: normalize every possible outcome into a `RetryAttempt<T>` tagged
// union FIRST, then decide whether to retry via a single
// `shouldRetry(result) => boolean` predicate. This lets callers retry on:
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
  RetryAttempt as CoreRetryAttempt,
  type RetryAttempt as CoreRetryAttemptType,
  RetryDecision,
  type RetryAttemptHandler as CoreRetryAttemptHandler,
  type Throws,
  retryAllBy,
  RetryPolicy,
} from "@perfect/core";
import { type HttpClientError, HTTP_RETRYABLE } from "./errors";

// ── RetryAttempt ADT ──────────────────────────────────────────────

export type RetryAttempt<T> = CoreRetryAttemptType<T, HttpClientError>;

export const RetryAttempt = {
  ...CoreRetryAttempt,
  success: CoreRetryAttempt.success as <T>(value: T) => RetryAttempt<T>,
  error: CoreRetryAttempt.error as <T>(error: HttpClientError) => RetryAttempt<T>,
  thrown: CoreRetryAttempt.thrown as <T>(error: unknown) => RetryAttempt<T>,
  isSuccess: CoreRetryAttempt.isSuccess as <T>(
    r: RetryAttempt<T>,
  ) => r is { readonly _tag: "success"; readonly value: T },
  isError: CoreRetryAttempt.isError as <T>(
    r: RetryAttempt<T>,
  ) => r is { readonly _tag: "error"; readonly error: HttpClientError },
  isThrown: CoreRetryAttempt.isThrown as <T>(
    r: RetryAttempt<T>,
  ) => r is { readonly _tag: "thrown"; readonly error: unknown },
  // HTTP-specific aliases for API compatibility.
  httpError: CoreRetryAttempt.error as <T>(error: HttpClientError) => RetryAttempt<T>,
  isHttpError: CoreRetryAttempt.isError as <T>(
    r: RetryAttempt<T>,
  ) => r is { readonly _tag: "error"; readonly error: HttpClientError },
} as const;

export { RetryDecision };

export type RetryAttemptHandler<T> = CoreRetryAttemptHandler<T, HttpClientError>;

// ── withRetryAll ───────────────────────────────────────────────────

export interface RetryAllOptions<T = unknown> {
  /** Max number of retries (excluding the initial attempt). Default: 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff in ms. Default: 250. */
  readonly baseDelayMs?: number;
  /** Cap on per-attempt delay in ms. Default: 30 000. */
  readonly maxDelayMs?: number;
  /** Full retry policy override. If provided, base/max timing options are ignored. */
  readonly policy?: RetryPolicy;
  /**
   * Decide whether to retry. Default: retry anything that isn't a success.
   * Override to:
   *   - poll a job: `(r) => !RetryAttempt.isSuccess(r) || r.value.status !== "done"`
   *   - retry only thrown defects: `RetryAttempt.isThrown`
   *   - retry only transient HTTP: `(r) => RetryAttempt.isHttpError(r) && HTTP_RETRYABLE(r.error)`
   */
  readonly shouldRetry?: (result: RetryAttempt<T>) => boolean;
}

const DEFAULT_SHOULD_RETRY = <T>(r: RetryAttempt<T>): boolean => r._tag !== "success";

export interface RetryAllByOptions<T = unknown> {
  /** Max number of retries (excluding the initial attempt). Default: 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff in ms. Default: 250. */
  readonly baseDelayMs?: number;
  /** Cap on per-attempt delay in ms. Default: 30 000. */
  readonly maxDelayMs?: number;
  /** Full retry policy override. If provided, base/max timing options are ignored. */
  readonly policy?: RetryPolicy;
  /** Inspect outcome and decide whether to retry (`RetryDecision.retry`) or stop (`RetryDecision.stop`). */
  readonly handle: RetryAttemptHandler<T>;
}

/**
 * Retry that observes every outcome. The `shouldRetry` predicate sees the
 * full `RetryAttempt<T>` and decides what to retry.
 */
export function withRetryAll<T>(
  eff: Eff<T, Throws<HttpClientError>>,
  options: RetryAllOptions<T> = {},
): Eff<T, Throws<HttpClientError>> {
  const { shouldRetry = DEFAULT_SHOULD_RETRY, ...rest } = options;
  return withRetryAllBy(eff, {
    ...rest,
    handle: (result) => (shouldRetry(result) ? RetryDecision.retry() : RetryDecision.stop()),
  });
}

/**
 * Cats-like handler style: `handle` inspects each outcome and decides
 * whether to retry (`RetryDecision.retry`) or return now (`RetryDecision.stop`).
 */
export function withRetryAllBy<T>(
  eff: Eff<T, Throws<HttpClientError>>,
  options: RetryAllByOptions<T>,
): Eff<T, Throws<HttpClientError>> {
  return retryAllBy(eff, options);
}

// ── withRetry (HTTP-aware default) ────────────────────────────────

export interface RetryOptions {
  /** Max retries (excluding initial attempt). Default: 3. */
  readonly maxRetries?: number;
  /** Base exponential backoff delay. Default: 250ms. */
  readonly baseDelayMs?: number;
  /** Per-attempt cap. Default: 30 000ms. */
  readonly maxDelayMs?: number;
  /** Full retry policy override. If provided, base/max timing options are ignored. */
  readonly policy?: RetryPolicy;
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
    policy: options.policy,
    shouldRetry: (r) => RetryAttempt.isHttpError(r) && when(r.error),
  });
}
