// @perfect/http — Phase 1: foundation.
//
// Roadmap in `docs/plan-http.md`. Phase 1 exports:
//   - Typed errors (HttpClientError tagged union)
//   - HttpTransport interface + FetchTransport default
//   - httpFetch / httpFetchOk / httpRequest / httpRequestJson / httpRequestText
//   - Response decoders (binary / text / json / arrayBuffer / blob)
//   - HttpResponse<T> metadata wrapper
//   - HTTP_RETRYABLE default "transient" predicate

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
