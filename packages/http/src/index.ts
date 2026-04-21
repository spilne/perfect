// @perfect/http — Phases 1 & 2.
//
// Roadmap in `docs/plan-http.md`.
//   Phase 1: typed errors, transport, decoders, httpFetch*/httpRequest*
//   Phase 2: AbstractHttpClient / DefaultHttpClient with withOverrides,
//            middleware hooks, HttpClient service tag for Layer DI

export {
  HttpNetworkError,
  HttpTimeoutError,
  HttpStatusError,
  HttpParseError,
  HTTP_RETRYABLE,
} from "./errors";
export type { HttpClientError } from "./errors";

export {
  FetchTransport,
  defaultTransport,
} from "./transport";
export type { HttpTransport, HttpRequestOptions, HttpProxyConfig } from "./transport";

export {
  binaryDecoder,
  textDecoder,
  jsonDecoder,
  arrayBufferDecoder,
  blobDecoder,
} from "./response";
export type { HttpResponse, ResponseDecoder, ResponseParser } from "./response";

export {
  httpFetch,
  httpFetchOk,
  httpRequest,
  httpRequestJson,
  httpRequestText,
} from "./fetch";
export type { AcceptStatus, WithTransport } from "./fetch";

// ── Phase 2 ──────────────────────────────────────────────────────
export {
  AbstractHttpClient,
  DefaultHttpClient,
  identityParser,
} from "./client";
export type {
  HttpClient,
  HttpClientConfig,
  HttpRequestParams,
  RequestOptions,
  RequestBodyOptions,
} from "./client";
export type { HttpMiddleware, HttpRequestContext } from "./middleware";
/** Service tag for Layer-based DI. Re-exported as `HttpClientService` to
 *  avoid clashing with the `HttpClient` interface type. */
export { HttpClient as HttpClientService } from "./service";
