// HttpClient — interface + AbstractHttpClient base + DefaultHttpClient.
//
// Three complementary entry points:
//   1. Direct:   new DefaultHttpClient({ baseUrl, headers, ... })
//   2. Derive:   client.withOverrides({ headers: extra })
//   3. Subclass: class MyApi extends DefaultHttpClient { fetchUser(id) {...} }
//
// The interface is `Eff`-typed so distributed / proxy / test implementations
// drop in wherever an HttpClient is needed (typically via Layer).

import { type Eff, type Throws, succeed, sync } from "@perfect/core";
import type { HttpClientError } from "./errors";
import type { HttpProxyConfig, HttpTransport } from "./transport";
import type { HttpMiddleware, HttpRequestContext } from "./middleware";
import {
  type HttpResponse,
  type ResponseDecoder,
  type ResponseParser,
  binaryDecoder,
} from "./response";
import { httpFetchOk, httpRequest, httpRequestText } from "./fetch";

/** Options common to every request (no body). */
export interface RequestOptions<E = string> {
  readonly headers?: Record<string, string>;
  readonly timeoutMs?: number;
  /** Label for metrics/logs — avoids high-cardinality raw URLs. */
  readonly tag?: string;
  readonly acceptStatus?: (status: number) => boolean;
  /**
   * Parse the body of non-OK responses into a typed `HttpStatusError<E>`.
   * Parse failures fall back gracefully: `body` keeps the raw text and
   * `parseError` is set. Use `HttpStatusError.isParsed(e)` to narrow.
   */
  readonly errorSchema?: ResponseParser<E>;
}

/** Adds body options for POST/PUT/PATCH/DELETE. */
export interface RequestBodyOptions<E = string> extends RequestOptions<E> {
  readonly json?: unknown;
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
}

/** Core request parameters consumed by `request(params)`. */
export interface HttpRequestParams<T, E = string> {
  readonly path: string | URL;
  readonly method: string;
  readonly schema: ResponseParser<T>;
  readonly json?: unknown;
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  readonly headers?: Record<string, string>;
  readonly timeoutMs?: number;
  readonly acceptStatus?: (status: number) => boolean;
  readonly tag?: string;
  /** Parse non-OK response body into typed `HttpStatusError<E>`. */
  readonly errorSchema?: ResponseParser<E>;
}

/** Identity parser — passes data through unchanged. Used by getJson/postJson. */
export const identityParser: ResponseParser<unknown> = {
  safeParse: (data) => ({ success: true as const, data }),
};

// ── HttpClient interface ───────────────────────────────────────────

export interface HttpClient {
  // Convenience methods (validated)
  get<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions<E>,
  ): Eff<T, Throws<HttpClientError>>;
  post<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
  ): Eff<T, Throws<HttpClientError>>;
  put<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
  ): Eff<T, Throws<HttpClientError>>;
  patch<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
  ): Eff<T, Throws<HttpClientError>>;
  delete<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
  ): Eff<T, Throws<HttpClientError>>;

  // Unvalidated convenience
  getJson<E = string>(
    path: string | URL,
    options?: RequestOptions<E>,
  ): Eff<unknown, Throws<HttpClientError>>;
  postJson<E = string>(
    path: string | URL,
    options?: RequestBodyOptions<E>,
  ): Eff<unknown, Throws<HttpClientError>>;
  getText<E = string>(
    path: string | URL,
    options?: RequestOptions<E>,
  ): Eff<string, Throws<HttpClientError>>;

  /** Get response + metadata. Decoder defaults to `binaryDecoder`. */
  getResponse<T = ReadableStream<Uint8Array>, E = string>(
    path: string | URL,
    options?: RequestOptions<E> & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>>;

  /** Low-level: pass the full params — everything else delegates here. */
  request<T, E = string>(params: HttpRequestParams<T, E>): Eff<T, Throws<HttpClientError>>;

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
  /**
   * Default error-body schema applied to every request. Per-request
   * `errorSchema` (on `get`/`post`/`request`/etc.) overrides this.
   * Typed as `ResponseParser<unknown>` so a single client can serve
   * multiple APIs — narrow via your schema if you want `HttpStatusError<E>`.
   */
  readonly errorSchema?: ResponseParser<unknown>;
}

// ── AbstractHttpClient — shared convenience methods ────────────────

/**
 * Shared convenience methods. Subclasses only implement `request`,
 * `getText`, `getResponse`, and `withOverrides`.
 */
export abstract class AbstractHttpClient implements HttpClient {
  get<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions<E>,
  ): Eff<T, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "GET",
      schema,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
      errorSchema: options?.errorSchema,
    });
  }

  post<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
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
      errorSchema: options?.errorSchema,
    });
  }

  put<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
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
      errorSchema: options?.errorSchema,
    });
  }

  patch<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
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
      errorSchema: options?.errorSchema,
    });
  }

  delete<T, E = string>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions<E>,
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
      errorSchema: options?.errorSchema,
    });
  }

  getJson<E = string>(
    path: string | URL,
    options?: RequestOptions<E>,
  ): Eff<unknown, Throws<HttpClientError>> {
    return this.request({
      path,
      method: "GET",
      schema: identityParser,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      acceptStatus: options?.acceptStatus,
      tag: options?.tag,
      errorSchema: options?.errorSchema,
    });
  }

  postJson<E = string>(
    path: string | URL,
    options?: RequestBodyOptions<E>,
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
      errorSchema: options?.errorSchema,
    });
  }

  abstract getText<E = string>(
    path: string | URL,
    options?: RequestOptions<E>,
  ): Eff<string, Throws<HttpClientError>>;

  abstract getResponse<T = ReadableStream<Uint8Array>, E = string>(
    path: string | URL,
    options?: RequestOptions<E> & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>>;

  abstract request<T, E = string>(params: HttpRequestParams<T, E>): Eff<T, Throws<HttpClientError>>;
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
      errorSchema: overrides.errorSchema ?? this.config.errorSchema,
    });
  }

  // ── Core ────────────────────────────────────────────────────────

  request<T, E = string>(params: HttpRequestParams<T, E>): Eff<T, Throws<HttpClientError>> {
    const url = this.resolveUrl(params.path);
    const context: HttpRequestContext = {
      method: params.method,
      url,
      tag: params.tag,
    };
    const eff = httpRequest<T, E>({
      url,
      method: params.method,
      headers: this.mergeHeaders(params.headers),
      json: params.json,
      body: params.body,
      timeoutMs: params.timeoutMs ?? this.config.timeoutMs,
      schema: params.schema,
      acceptStatus: params.acceptStatus,
      errorSchema: (params.errorSchema ?? this.config.errorSchema) as ResponseParser<E> | undefined,
      transport: this.config.transport,
      proxy: this.config.proxy,
    });
    return this.instrument(eff, context);
  }

  getText<E = string>(
    path: string | URL,
    options?: RequestOptions<E>,
  ): Eff<string, Throws<HttpClientError>> {
    const url = this.resolveUrl(path);
    const context: HttpRequestContext = { method: "GET", url, tag: options?.tag };
    const eff = httpRequestText<E>({
      url,
      method: "GET",
      headers: this.mergeHeaders(options?.headers),
      timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
      acceptStatus: options?.acceptStatus,
      errorSchema: (options?.errorSchema ?? this.config.errorSchema) as
        | ResponseParser<E>
        | undefined,
      transport: this.config.transport,
      proxy: this.config.proxy,
    });
    return this.instrument(eff, context);
  }

  getResponse<T = ReadableStream<Uint8Array>, E = string>(
    path: string | URL,
    options?: RequestOptions<E> & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>> {
    const decoder = options?.decoder ?? (binaryDecoder as unknown as ResponseDecoder<T>);
    const url = this.resolveUrl(path);
    const context: HttpRequestContext = { method: "GET", url, tag: options?.tag };
    const inner = httpFetchOk<E>({
      url,
      method: "GET",
      headers: this.mergeHeaders(options?.headers),
      timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
      acceptStatus: options?.acceptStatus,
      errorSchema: (options?.errorSchema ?? this.config.errorSchema) as
        | ResponseParser<E>
        | undefined,
      transport: this.config.transport,
      proxy: this.config.proxy,
    }).flatMap((response) =>
      (
        sync(() => ({
          status: response.status,
          headers: response.headers,
          contentType: response.headers.get("content-type"),
          contentLength: (() => {
            const l = response.headers.get("content-length");
            return l === null ? null : Number(l);
          })(),
          response,
        })) as any
      ).flatMap((meta: any) =>
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
    return { ...defaults, ...extra };
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
