// HttpTransport — pluggable "send bytes, get Response" layer.
//
// Everything above the transport (URL resolution, header merging, JSON
// parsing, validation, retry, streaming) is generic orchestration that
// doesn't care HOW bytes get sent. Isolating transport behind an interface
// gives us:
//
//   1. Swappable runtimes — default is `FetchTransport` (Bun/Node global
//      fetch). A future `@perfect/http-otel` can implement the same
//      interface with OpenTelemetry tracing / test-time mocking.
//
//   2. Transport-level testing — inject a custom transport that returns
//      canned `Response` objects; test the full pipeline (status checks,
//      JSON parsing, validation) without a running server.
//
//   3. Separation of concerns — AbortController + timeout handling lives
//      INSIDE the transport. Higher layers only handle "I have a Response,
//      what now?"

import { type Eff, type Throws, sync, tryPromise, scoped } from "@perfect/core";
import { HttpNetworkError, HttpTimeoutError, type HttpClientError } from "./errors";

const DEFAULT_TIMEOUT_MS = 30_000;

/**
 * TLS-intercepting proxy config. Each transport translates to its native
 * format (Bun's native `proxy`/`tls` fetch options, undici agents, etc.).
 */
export interface HttpProxyConfig {
  /** Proxy endpoint URL (`http://user:pass@proxy.corp:8080`). */
  readonly url: string;
  /** Custom CA certificate for MITM proxies (PEM format). */
  readonly ca?: string;
}

export interface HttpRequestOptions {
  /** Full URL (string or URL object). */
  readonly url: string | URL;
  /** HTTP method. Defaults to `"GET"`. */
  readonly method?: string;
  /** Request headers. */
  readonly headers?: Record<string, string>;
  /** JSON-serializable request body. Auto-sets `Content-Type: application/json`. */
  readonly json?: unknown;
  /** Raw body (FormData, stream, Blob, ArrayBuffer). Mutually exclusive with `json`. */
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  /** Per-request timeout in ms. Defaults to 30 000. */
  readonly timeoutMs?: number;
  /** External abort signal combined with the timeout signal. */
  readonly signal?: AbortSignal;
  /** Proxy for this request. */
  readonly proxy?: HttpProxyConfig;
}

/**
 * Transport contract. Implementations:
 *   - `FetchTransport` (this package, uses global fetch)
 *   - `MockTransport` (this package, for tests)
 *   - `OtelTransport` (future, separate package)
 *
 * `execute()` returns a scoped effect — AbortController is tied to the
 * surrounding scope so interrupts abort the underlying request.
 */
export interface HttpTransport {
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>>;
}

/**
 * Default transport — delegates to global `fetch`. An AbortController is
 * acquired as a scoped resource; on fiber interrupt (timeout / race /
 * manual interrupt), the controller aborts the fetch, killing the TCP
 * connection immediately rather than waiting for the server.
 */
export class FetchTransport implements HttpTransport {
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    const {
      url,
      method = "GET",
      headers = {},
      json,
      body,
      timeoutMs = DEFAULT_TIMEOUT_MS,
      signal,
      proxy,
    } = options;

    const urlStr = typeof url === "string" ? url : url.toString();

    // Build final headers / body
    const finalHeaders: Record<string, string> = { ...headers };
    let finalBody: string | ArrayBuffer | ReadableStream | Blob | FormData | undefined = body;
    if (json !== undefined) {
      finalHeaders["Content-Type"] ??= "application/json";
      finalBody = JSON.stringify(json);
    }

    return scoped(
      // `settled` guards the release. The scope closes as soon as this effect
      // yields the Response — which is when the HEADERS have arrived, not the
      // body. Aborting unconditionally there killed the connection out from
      // under every caller that reads the body afterwards (httpRequestJson,
      // client.get, …); it only appeared to work because a small body usually
      // won the race. Once the fetch has resolved the caller owns the Response,
      // so there is nothing left for us to cancel.
      sync(() => ({ controller: new AbortController(), settled: false }))
        .acquireRelease((state) =>
          sync(() => {
            if (!state.settled) state.controller.abort();
          }),
        )
        .flatMap((state) => {
          const controller = state.controller;
          // Combine: our controller + timeout + optional external signal
          const signals: AbortSignal[] = [controller.signal, AbortSignal.timeout(timeoutMs)];
          if (signal) signals.push(signal);

          // Bun supports `proxy` + `tls` in fetch; Node fetch does not (safely ignored there).
          const fetchOptions: RequestInit & { proxy?: string; tls?: { ca?: string } } = {
            method,
            headers: finalHeaders,
            body: finalBody,
            signal: AbortSignal.any(signals),
          };
          if (proxy) {
            fetchOptions.proxy = proxy.url;
            if (proxy.ca) fetchOptions.tls = { ca: proxy.ca };
          }

          return tryPromise(
            () =>
              fetch(urlStr, fetchOptions).then(
                (response) => {
                  // Headers are in and the body stream belongs to the caller.
                  state.settled = true;
                  return response;
                },
                (cause) => {
                  // Nothing left to abort on the failure path either.
                  state.settled = true;
                  throw cause;
                },
              ),
            (cause): HttpClientError => {
              // DOMException with name "TimeoutError" → our timeout fired
              if (cause instanceof DOMException && cause.name === "TimeoutError") {
                return new HttpTimeoutError({
                  url: urlStr,
                  timeoutMs,
                  message: `Request to ${urlStr} timed out after ${timeoutMs}ms`,
                });
              }
              // Our controller aborted → interrupt surfacing, not a timeout
              if (controller.signal.aborted) {
                return new HttpNetworkError({
                  url: urlStr,
                  cause,
                  message: `Request to ${urlStr} was aborted`,
                });
              }
              return new HttpNetworkError({
                url: urlStr,
                cause,
                message: `Fetch to ${urlStr} failed: ${cause instanceof Error ? cause.message : String(cause)}`,
              });
            },
          );
        }),
    ) as any;
  }
}

/** Shared default transport — used when no custom `transport` is passed. */
export const defaultTransport: HttpTransport = new FetchTransport();
