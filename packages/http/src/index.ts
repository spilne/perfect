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
} from "./errors.js";
export type { HttpClientError } from "./errors.js";

export { FetchTransport, defaultTransport } from "./transport.js";
export type { HttpTransport, HttpRequestOptions, HttpProxyConfig } from "./transport.js";

export {
  binaryDecoder,
  textDecoder,
  jsonDecoder,
  arrayBufferDecoder,
  blobDecoder,
} from "./response.js";
export type { HttpResponse, ResponseDecoder, ResponseParser } from "./response.js";

export { httpFetch, httpFetchOk, httpRequest, httpRequestJson, httpRequestText } from "./fetch.js";
export type { AcceptStatus, WithTransport } from "./fetch.js";

// ── Phase 2 ──────────────────────────────────────────────────────
export { AbstractHttpClient, DefaultHttpClient, identityParser } from "./client.js";
export type {
  HttpClient,
  HttpClientConfig,
  HttpRequestParams,
  RequestOptions,
  RequestBodyOptions,
  MultipartOptions,
} from "./client.js";
export type { HttpMiddleware, HttpRequestContext } from "./middleware.js";
/** Service tag for Layer-based DI. Re-exported as `HttpClientService` to
 *  avoid clashing with the `HttpClient` interface type. */
export { HttpClient as HttpClientService } from "./service.js";

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
} from "./retry.js";
export type {
  RetryHttpOptions,
  RetryAllOptions,
  RetryAllByOptions,
  RetryAttemptHandler,
} from "./retry.js";

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
} from "./stream.js";
export type { SSEvent } from "./stream.js";

// ── Phase 5 — test utilities ─────────────────────────────────────
export { MockHttpClient, mockHttpClient } from "./mock.js";
export type { RecordedCall, ResponseHandler } from "./mock.js";
