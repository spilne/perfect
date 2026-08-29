// @spilne/perfect-http — Phases 1, 2 & 3.
//
// Roadmap in `docs/plan-http.md`.
//   Phase 1: typed errors, transport, decoders, httpFetch*/httpRequest*
//   Phase 2: AbstractHttpClient / DefaultHttpClient with withOverrides,
//            middleware hooks, HttpClient service tag for Layer DI
//   Phase 3: withRetryAll (outcome ADT).
//            For polling use core's .repeatUntil / .repeatUntilWithBackoff.
//   Phase 4: httpStreamText / httpStreamLines / httpStreamNDJSON / httpStreamSSE
//   Phase 5: MockHttpClient — route-matched test double with call recording

export {
  HttpNetworkError,
  HttpTimeoutError,
  HttpStatusError,
  HttpUnknownError,
  HttpParseError,
  HTTP_RETRYABLE,
} from "./errors";
export type { HttpClientError } from "./errors";

export { FetchTransport, defaultTransport } from "./transport";
export type { HttpTransport, HttpRequestOptions, HttpProxyConfig } from "./transport";

export {
  binaryDecoder,
  textDecoder,
  jsonDecoder,
  arrayBufferDecoder,
  blobDecoder,
} from "./response";
export type { HttpResponse, ResponseDecoder, ResponseParser } from "./response";

export { httpFetch, httpFetchOk, httpRequest, httpRequestJson, httpRequestText } from "./fetch";
export type { AcceptStatus, WithTransport } from "./fetch";

// ── Phase 2 ──────────────────────────────────────────────────────
export { AbstractHttpClient, DefaultHttpClient, identityParser } from "./client";
export type {
  HttpClient,
  HttpClientConfig,
  HttpRequestParams,
  RequestOptions,
  RequestBodyOptions,
  MultipartOptions,
} from "./client";
export type { HttpMiddleware, HttpRequestContext } from "./middleware";
/** Service tag for Layer-based DI. Re-exported as `HttpClientService` to
 *  avoid clashing with the `HttpClient` interface type. */
export { HttpClient as HttpClientService } from "./service";

// ── Phase 3 ──────────────────────────────────────────────────────
// withRetryAll = full outcome ADT.
// For polling use core's `.repeatUntil` / `.repeatUntilWithBackoff` — they
// subsume the `poll` helper promin has separately.
export {
  withRetryAll,
  withRetryAllBy,
  retryHttp,
  Retry,
  RetryAttempt,
  RetryDecision,
} from "./retry";
export type {
  RetryHttpOptions,
  RetryAllOptions,
  RetryAllByOptions,
  RetryAttemptHandler,
} from "./retry";

// ── Phase 4 — streaming ──────────────────────────────────────────
// One base (httpStream) + composable pipes (parseSSE, parseNDJSON), with
// 4 thin wrappers (httpStreamText / Lines / NDJSON / SSE) for ergonomics.
//
//   httpStream(opts).through(Pipes.utf8Decode).through(Pipes.lines).through(parseSSE)
export {
  httpStream,
  httpStreamText,
  httpStreamLines,
  httpStreamNDJSON,
  httpStreamSSE,
  parseSSE,
  parseNDJSON,
} from "./stream";
export type { SSEvent } from "./stream";

// ── Phase 5 — test utilities ─────────────────────────────────────
export { MockHttpClient, mockHttpClient } from "./mock";
export type { RecordedCall, ResponseHandler } from "./mock";
