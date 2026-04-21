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

/**
 * Server returned a non-OK status code with a body of known shape.
 *
 * Generic over the body type. By default `body: string` (raw response text).
 * If you pass `errorSchema` to `httpRequest` / `httpFetchOk` / client methods,
 * the body is parsed through it and `body: B` reflects the typed shape.
 *
 * If parsing fails (bad JSON or schema mismatch), `HttpUnknownError` is
 * raised instead — `HttpStatusError<B>` always carries a body of the typed
 * shape, never a `string` fallback.
 */
export class HttpStatusError<B = string> extends Error {
  static readonly _tag = "HttpStatusError" as const;
  readonly _tag = "HttpStatusError" as const;

  readonly url: string;
  readonly status: number;
  readonly body: B;

  constructor(props: {
    readonly url: string;
    readonly status: number;
    readonly body: B;
    readonly message: string;
  }) {
    super(props.message);
    this.url = props.url;
    this.status = props.status;
    this.body = props.body;
    this.name = "HttpStatusError";
    Object.setPrototypeOf(this, new.target.prototype);
  }

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

/**
 * Server returned a non-OK status but the response body did not match the
 * provided `errorSchema`. Carries the raw text + the parse failure cause so
 * callers can still log / inspect / fall back. Status is preserved so
 * retry predicates can still classify by HTTP code.
 *
 * Only raised when `errorSchema` was opted into. Without it, an unparseable
 * error body comes back as `HttpStatusError<string>` with the raw text.
 */
export class HttpUnknownError extends TaggedError("HttpUnknownError")<{
  readonly url: string;
  readonly status: number;
  readonly body: string;
  readonly parseError: unknown;
  readonly message: string;
}>() {
  /** 5xx or 429 — retryable by default. */
  get isRetryable(): boolean {
    return this.status >= 500 || this.status === 429;
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
  | HttpUnknownError
  | HttpParseError;

/**
 * Default "transient" predicate — retry 5xx, 429, timeouts, network errors,
 * including unknown-shape error responses with retryable status codes.
 * Caller bugs (4xx other than 429) and parse errors do NOT retry.
 */
export const HTTP_RETRYABLE = (error: HttpClientError): boolean =>
  error._tag === "HttpTimeoutError" ||
  error._tag === "HttpNetworkError" ||
  (error._tag === "HttpStatusError" && error.isRetryable) ||
  (error._tag === "HttpUnknownError" && error.isRetryable);
