// HttpClient — interface + AbstractHttpClient base + DefaultHttpClient.
//
// Three complementary entry points:
//   1. Direct:   new DefaultHttpClient({ baseUrl, headers, ... })
//   2. Derive:   client.withOverrides({ headers: extra })
//   3. Subclass: class MyApi extends DefaultHttpClient { fetchUser(id) {...} }
//
// The interface is `Eff`-typed so distributed / proxy / test implementations
// drop in wherever an HttpClient is needed (typically via Layer).

import {
  type Eff,
  type Throws,
  type Needs,
  succeed,
  sync,
} from "@perfect/core";
import type { HttpClientError } from "./errors";
import type {
  HttpProxyConfig,
  HttpRequestOptions,
  HttpTransport,
} from "./transport";
import type { HttpMiddleware, HttpRequestContext } from "./middleware";
import {
  type HttpResponse,
  type ResponseDecoder,
  type ResponseParser,
  binaryDecoder,
} from "./response";
import { httpFetchOk, httpRequest, httpRequestText } from "./fetch";

/** Options common to every request (no body). */
export interface RequestOptions {
  readonly headers?: Record<string, string>;
  readonly timeoutMs?: number;
  /** Label for metrics/logs — avoids high-cardinality raw URLs. */
  readonly tag?: string;
  readonly acceptStatus?: (status: number) => boolean;
}

/** Adds body options for POST/PUT/PATCH/DELETE. */
export interface RequestBodyOptions extends RequestOptions {
  readonly json?: unknown;
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
}

/** Core request parameters consumed by `request(params)`. */
export interface HttpRequestParams<T> {
  readonly path: string | URL;
  readonly method: string;
  readonly schema: ResponseParser<T>;
  readonly json?: unknown;
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  readonly headers?: Record<string, string>;
  readonly timeoutMs?: number;
  readonly acceptStatus?: (status: number) => boolean;
  readonly tag?: string;
}

/** Identity parser — passes data through unchanged. Used by getJson/postJson. */
export const identityParser: ResponseParser<unknown> = {
  safeParse: (data) => ({ success: true as const, data }),
};

// ── HttpClient interface ───────────────────────────────────────────

export interface HttpClient {
  // Convenience methods (validated)
  get<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): Eff<T, Throws<HttpClientError>>;
  post<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>>;
  put<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>>;
  patch<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>>;
  delete<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>>;

  // Unvalidated convenience
  getJson(path: string | URL, options?: RequestOptions): Eff<unknown, Throws<HttpClientError>>;
  postJson(
    path: string | URL,
    options?: RequestBodyOptions,
  ): Eff<unknown, Throws<HttpClientError>>;
  getText(path: string | URL, options?: RequestOptions): Eff<string, Throws<HttpClientError>>;

  /** Get response + metadata. Decoder defaults to `binaryDecoder`. */
  getResponse<T = ReadableStream<Uint8Array>>(
    path: string | URL,
    options?: RequestOptions & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>>;

  /** Low-level: pass the full params — everything else delegates here. */
  request<T>(params: HttpRequestParams<T>): Eff<T, Throws<HttpClientError>>;

  /** Return a new client with merged config. Headers spread-merge; others fall back. */
  withOverrides(overrides: Partial<HttpClientConfig>): HttpClient;
}

// ── Config ─────────────────────────────────────────────────────────

export interface HttpClientConfig {
  /** Base URL prepended to relative paths. */
  readonly baseUrl?: string;
  /** Default headers sent with every request. */
  readonly headers?: Record<string, string>;
  /** Default per-request timeout in ms. Defaults to 30 000. */
  readonly timeoutMs?: number;
  /** Sync observability hooks applied to every request. */
  readonly middleware?: readonly HttpMiddleware[];
  /** Override the transport layer (default: global fetch). */
  readonly transport?: HttpTransport;
  /** Default proxy for every request. */
  readonly proxy?: HttpProxyConfig;
}

// ── AbstractHttpClient — shared convenience methods ────────────────

/**
 * Shared convenience methods. Subclasses only implement `request`,
 * `getText`, `getResponse`, and `withOverrides`.
 */
export abstract class AbstractHttpClient implements HttpClient {
  get<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): Eff<T, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "GET",
      schema,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  post<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "POST",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  put<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "PUT",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  patch<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "PATCH",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  delete<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Eff<T, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "DELETE",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  getJson(
    path: string | URL,
    options?: RequestOptions,
  ): Eff<unknown, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "GET",
      schema: identityParser,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  postJson(
    path: string | URL,
    options?: RequestBodyOptions,
  ): Eff<unknown, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "POST",
      schema: identityParser,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
    });
  }

  abstract getText(
    path: string | URL,
    options?: RequestOptions,
  ): Eff<string, Throws<HttpClientError>>;

  abstract getResponse<T = ReadableStream<Uint8Array>>(
    path: string | URL,
    options?: RequestOptions & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>>;

  abstract request<T>(params: HttpRequestParams<T>): Eff<T, Throws<HttpClientError>>;
  abstract withOverrides(overrides: Partial<HttpClientConfig>): HttpClient;
}

// ── DefaultHttpClient — real HTTP via fetch ───────────────────────

export class DefaultHttpClient extends AbstractHttpClient {
  constructor(private readonly config: HttpClientConfig = {}) {
    super();
  }

  withOverrides(overrides: Partial<HttpClientConfig>): DefaultHttpClient {
    return new DefaultHttpClient({
      baseUrl: overrides.baseUrl ?? this.config.baseUrl,
      headers: { ...this.config.headers, ...overrides.headers },
      timeoutMs: overrides.timeoutMs ?? this.config.timeoutMs,
      middleware: overrides.middleware
        ? [...(this.config.middleware ?? []), ...overrides.middleware]
        : this.config.middleware,
      transport: overrides.transport ?? this.config.transport,
      proxy: overrides.proxy ?? this.config.proxy,
    });
  }

  // ── Core ────────────────────────────────────────────────────────

  request<T>(params: HttpRequestParams<T>): Eff<T, Throws<HttpClientError>> {
    const url = this.resolveUrl(params.path);
    const context: HttpRequestContext = {
      method: params.method,
      url,
      tag: params.tag,
    };
    const eff = httpRequest<T>({
      url,
      method: params.method,
      headers: this.mergeHeaders(params.headers),
      json: params.json,
      body: params.body,
      timeoutMs: params.timeoutMs ?? this.config.timeoutMs,
      schema: params.schema,
      acceptStatus: params.acceptStatus,
      transport: this.config.transport,
      proxy: this.config.proxy,
    });
    return this.instrument(eff, context);
  }

  getText(
    path: string | URL,
    options?: RequestOptions,
  ): Eff<string, Throws<HttpClientError>> {
    const url = this.resolveUrl(path);
    const context: HttpRequestContext = { method: "GET", url, tag: options?.tag };
    const eff = httpRequestText({
      url,
      method: "GET",
      headers: this.mergeHeaders(options?.headers),
      timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
      acceptStatus: options?.acceptStatus,
      transport: this.config.transport,
      proxy: this.config.proxy,
    });
    return this.instrument(eff, context);
  }

  getResponse<T = ReadableStream<Uint8Array>>(
    path: string | URL,
    options?: RequestOptions & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>> {
    const decoder = (options?.decoder ?? (binaryDecoder as unknown as ResponseDecoder<T>));
    const url = this.resolveUrl(path);
    const context: HttpRequestContext = { method: "GET", url, tag: options?.tag };
    const inner = httpFetchOk({
      url,
      method: "GET",
      headers: this.mergeHeaders(options?.headers),
      timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
      acceptStatus: options?.acceptStatus,
      transport: this.config.transport,
      proxy: this.config.proxy,
    })
      .flatMap((response) =>
        (sync(() => ({
          status: response.status,
          headers: response.headers,
          contentType: response.headers.get("content-type"),
          contentLength: (() => {
            const l = response.headers.get("content-length");
            return l === null ? null : Number(l);
          })(),
          response,
        })) as any).flatMap((meta: any) =>
          // Decoder returns a Promise — bridge via tryPromise
          succeed(null).flatMap(() => {
            return decodeResponse(meta.response, decoder).map(
              (body: T): HttpResponse<T> => ({
                status: meta.status,
                headers: meta.headers,
                contentType: meta.contentType,
                contentLength: meta.contentLength,
                body,
              }),
            );
          }),
        ),
      ) as Eff<HttpResponse<T>, Throws<HttpClientError>>;
    return this.instrument(inner, context);
  }

  // ── Internals ───────────────────────────────────────────────────

  private resolveUrl(path: string | URL): string {
    const p = typeof path === "string" ? path : path.toString();
    const base = this.config.baseUrl;
    if (!base) return p;
    if (p.startsWith("http://") || p.startsWith("https://")) return p;
    const b = base.endsWith("/") ? base.slice(0, -1) : base;
    const rel = p.startsWith("/") ? p : `/${p}`;
    return `${b}${rel}`;
  }

  private mergeHeaders(extra?: Record<string, string>): Record<string, string> | undefined {
    const defaults = this.config.headers;
    if (!defaults && !extra) return undefined;
    return { ...(defaults ?? {}), ...(extra ?? {}) };
  }

  /** Wrap an effect with middleware hooks + duration tracking.
   *
   *  The SAME context object is passed to onRequest / onResponse / onError —
   *  not a spread copy — so middleware can key per-request state off the
   *  context reference (e.g. a WeakMap<HttpRequestContext, Span>). The
   *  `durationMs` field is mutated on the existing context before
   *  onResponse / onError fire.
   */
  private instrument<A, E extends HttpClientError>(
    eff: Eff<A, Throws<E>>,
    context: HttpRequestContext,
  ): Eff<A, Throws<E>> {
    const middleware = this.config.middleware;
    if (!middleware || middleware.length === 0) return eff;
    // Mutable twin of the context, shared across the lifecycle.
    const mut = context as any as {
      method: string;
      url: string;
      tag?: string;
      durationMs: number;
    };
    return sync(() => {
      for (const mw of middleware) mw.onRequest?.(context);
      return performance.now();
    }).flatMap((start: number) =>
      (eff as any)
        .tap((_value: A) =>
          sync(() => {
            mut.durationMs = performance.now() - start;
            for (const mw of middleware) mw.onResponse?.(context as any);
          }),
        )
        .tapError((error: E) =>
          sync(() => {
            mut.durationMs = performance.now() - start;
            for (const mw of middleware) mw.onError?.(context as any, error);
          }),
        ),
    ) as Eff<A, Throws<E>>;
  }
}

// ── Helper: decode response body via a ResponseDecoder ────────────

import { tryPromise } from "@perfect/core";
import { HttpParseError } from "./errors";

function decodeResponse<T>(
  response: Response,
  decoder: ResponseDecoder<T>,
): Eff<T, Throws<HttpClientError>> {
  return tryPromise(
    () => decoder(response),
    (cause): HttpClientError =>
      new HttpParseError({
        url: response.url || "<unknown>",
        cause,
        message: `Decoder failed: ${cause instanceof Error ? cause.message : String(cause)}`,
      }),
  ) as Eff<T, Throws<HttpClientError>>;
}
