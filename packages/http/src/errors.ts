// Typed HTTP client errors. Matches promin's `HttpClientError` shape so the
// migration is near-mechanical. Uses Perfect's `TaggedError` helper — each
// error class has a static `_tag` (for `.catchTag`) AND is an Error subclass
// (for `instanceof` + stack traces).

import { TaggedError } from "@perfect/core";

/** Network-level failure (DNS, connection refused, socket hang up, fetch aborted). */
export class HttpNetworkError extends TaggedError("HttpNetworkError")<{
  readonly url: string;
  readonly cause: unknown;
  readonly message: string;
}>() {}

/** Request exceeded its timeout budget. */
export class HttpTimeoutError extends TaggedError("HttpTimeoutError")<{
  readonly url: string;
  readonly timeoutMs: number;
  readonly message: string;
}>() {}

/** Server returned a non-OK status code. Carries the body for diagnostics. */
export class HttpStatusError extends TaggedError("HttpStatusError")<{
  readonly url: string;
  readonly status: number;
  readonly body: string;
  readonly message: string;
}>() {
  /** 5xx or 429 — retryable by default. */
  get isRetryable(): boolean {
    return this.status >= 500 || this.status === 429;
  }
  /** 4xx — caller bug, don't retry. */
  get isClientError(): boolean {
    return this.status >= 400 && this.status < 500;
  }
  /** 5xx — server trouble. */
  get isServerError(): boolean {
    return this.status >= 500;
  }
}

/** Response body couldn't be parsed (bad JSON, schema mismatch, etc.). */
export class HttpParseError extends TaggedError("HttpParseError")<{
  readonly url: string;
  readonly cause: unknown;
  readonly message: string;
}>() {}

/** Union of all HTTP client errors — discriminate on `_tag`. */
export type HttpClientError =
  | HttpNetworkError
  | HttpTimeoutError
  | HttpStatusError
  | HttpParseError;

/**
 * Default "transient" predicate — retry 5xx, 429, timeouts, network errors.
 * Caller bugs (4xx other than 429) and parse errors do NOT retry.
 */
export const HTTP_RETRYABLE = (error: HttpClientError): boolean =>
  error._tag === "HttpTimeoutError" ||
  error._tag === "HttpNetworkError" ||
  (error._tag === "HttpStatusError" && error.isRetryable);
