// MockHttpClient — an `HttpClient` implementation for tests.
//
// Records every call; responds per registered route. Supports:
//   .on(method, path, value)       — static response (value or typed error)
//   .onFn(method, path, handler)   — dynamic handler(call) → value/error
//   .onSequence(method, path, [...]) — ordered queue; last is reused after exhaust
//   .respondWith(default)          — fallback for unmatched routes
//
// Assertions:
//   .calledWith(method, path)      — at least once?
//   .calledTimes(method, path)     — count
//   .calledWithJson(method, path, body) — matched with specific JSON body
//   .callsFor(method, path)        — all matching calls
//   .lastCall                      — most recent
//
// Cleanup:
//   .resetCalls()                  — clear recorded calls only
//   .reset()                       — clear calls + routes + default
//
// For streaming mocks, pair MockHttpClient with a MockTransport in the
// consumer — the streaming helpers (`httpStreamText`, `httpStreamSSE`, etc.)
// are standalone functions that read a `Response`, so you control the body
// at the transport layer.

import { type Eff, type Throws, succeed, fail, suspend } from "@perfect/core";
import { type HttpClientError, HttpParseError, HttpStatusError } from "./errors";
import {
  AbstractHttpClient,
  type HttpClient,
  type HttpClientConfig,
  type HttpRequestParams,
  type RequestOptions,
} from "./client";
import { type HttpResponse, type ResponseDecoder, type ResponseParser } from "./response";

// ── Types ─────────────────────────────────────────────────────────

/** A recorded call — subset of HttpRequestParams, omitting schema/etc. */
export interface RecordedCall {
  readonly method: string;
  readonly path: string;
  readonly json?: unknown;
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  readonly headers?: Record<string, string>;
  readonly tag?: string;
}

/** Handler: receives the call, returns a value or a typed error. */
export type ResponseHandler = (call: RecordedCall) => unknown | HttpClientError;

type RouteEntry =
  | { readonly type: "static"; readonly value: unknown | HttpClientError }
  | { readonly type: "handler"; readonly fn: ResponseHandler }
  | {
      readonly type: "queue";
      readonly responses: (unknown | HttpClientError)[];
      readonly fallback?: unknown | HttpClientError;
    };

interface RegisteredRoute {
  readonly method: string;
  readonly matcher: string | RegExp;
  readonly entry: RouteEntry;
}

function pathMatches(matcher: string | RegExp, path: string): boolean {
  return typeof matcher === "string" ? path === matcher : matcher.test(path);
}

// Test whether an arbitrary value is a typed HttpClientError.
const HTTP_ERROR_TAGS: Record<HttpClientError["_tag"], true> = {
  HttpNetworkError: true,
  HttpTimeoutError: true,
  HttpStatusError: true,
  HttpUnknownError: true,
  HttpParseError: true,
};
function isHttpClientError(value: unknown): value is HttpClientError {
  if (value == null || typeof value !== "object" || !("_tag" in value)) return false;
  return (value as { _tag: string })._tag in HTTP_ERROR_TAGS;
}

// ── MockHttpClient ────────────────────────────────────────────────

export class MockHttpClient extends AbstractHttpClient {
  /** All recorded calls, oldest first. */
  readonly calls: RecordedCall[] = [];

  private defaultResponse: unknown = {};
  private readonly routes: RegisteredRoute[] = [];

  // ── Setup ───────────────────────────────────────────────────────

  /** Default response for unmatched routes. */
  respondWith<T>(value: T | HttpClientError): this {
    this.defaultResponse = value;
    return this;
  }

  /** Static response for a method + path (exact string or RegExp). */
  on(method: string, path: string | RegExp, response: unknown | HttpClientError): this {
    this.routes.push({ method, matcher: path, entry: { type: "static", value: response } });
    return this;
  }

  /** Dynamic response handler — receives the call, returns value/error. */
  onFn(method: string, path: string | RegExp, handler: ResponseHandler): this {
    this.routes.push({ method, matcher: path, entry: { type: "handler", fn: handler } });
    return this;
  }

  /**
   * Queue of responses consumed in order. After exhaust, the last item is
   * reused for subsequent calls.
   */
  onSequence(
    method: string,
    path: string | RegExp,
    responses: (unknown | HttpClientError)[],
  ): this {
    this.routes.push({
      method,
      matcher: path,
      entry: {
        type: "queue",
        responses: [...responses],
        fallback: responses[responses.length - 1],
      },
    });
    return this;
  }

  // ── Static helpers ──────────────────────────────────────────────

  /** Convenience: build an `HttpStatusError` for use with `.on`. */
  static fail(status: number, body = ""): HttpStatusError {
    return new HttpStatusError({ url: "mock", status, body, message: `Mock ${status}` });
  }

  // ── Assertions ──────────────────────────────────────────────────

  /** Was a method + path called at least once? */
  calledWith(method: string, path: string | RegExp): boolean {
    return this.calls.some((c) => c.method === method && pathMatches(path, c.path));
  }

  /** Count matching calls. */
  calledTimes(method: string, path: string | RegExp): number {
    return this.calls.filter((c) => c.method === method && pathMatches(path, c.path)).length;
  }

  /** Was a method + path called with a specific JSON body? (deep equality) */
  calledWithJson(method: string, path: string | RegExp, expectedJson: unknown): boolean {
    const target = JSON.stringify(expectedJson);
    return this.calls.some(
      (c) => c.method === method && pathMatches(path, c.path) && JSON.stringify(c.json) === target,
    );
  }

  /** All calls matching method + path. */
  callsFor(method: string, path: string | RegExp): RecordedCall[] {
    return this.calls.filter((c) => c.method === method && pathMatches(path, c.path));
  }

  /** Most recent call (or undefined). */
  get lastCall(): RecordedCall | undefined {
    return this.calls[this.calls.length - 1];
  }

  // ── Reset ───────────────────────────────────────────────────────

  /** Clear recorded calls; keep routes + default. */
  resetCalls(): this {
    this.calls.length = 0;
    return this;
  }

  /** Clear calls + routes + default. */
  reset(): this {
    this.calls.length = 0;
    this.routes.length = 0;
    this.defaultResponse = {};
    return this;
  }

  // ── AbstractHttpClient impl ─────────────────────────────────────

  /** Returns the same mock — overrides don't apply to a test double. */
  withOverrides(_overrides: Partial<HttpClientConfig>): MockHttpClient {
    return this;
  }

  request<T, _E = string>(params: HttpRequestParams<T, _E>): Eff<T, Throws<HttpClientError>> {
    return suspend(() => {
      const path = typeof params.path === "string" ? params.path : params.path.toString();
      this.recordCall({
        method: params.method,
        path,
        json: params.json,
        body: params.body,
        headers: params.headers,
        tag: params.tag,
      });
      const entry = this.findRoute(params.method, path);
      const raw = entry
        ? this.resolveEntry(entry, this.calls[this.calls.length - 1]!)
        : this.defaultResponse;
      if (isHttpClientError(raw)) return fail(raw) as Eff<T, Throws<HttpClientError>>;
      return this.validate<T>(raw, params.schema, path);
    });
  }

  getText<_E = string>(
    path: string | URL,
    options?: RequestOptions<_E>,
  ): Eff<string, Throws<HttpClientError>> {
    return suspend(() => {
      const p = typeof path === "string" ? path : path.toString();
      this.recordCall({
        method: "GET",
        path: p,
        headers: options?.headers,
        tag: options?.tag,
      });
      const entry = this.findRoute("GET", p);
      const raw = entry
        ? this.resolveEntry(entry, this.calls[this.calls.length - 1]!)
        : this.defaultResponse;
      if (isHttpClientError(raw)) return fail(raw) as Eff<string, Throws<HttpClientError>>;
      return succeed(typeof raw === "string" ? raw : JSON.stringify(raw));
    });
  }

  getResponse<T = ReadableStream<Uint8Array>, _E = string>(
    path: string | URL,
    options?: RequestOptions<_E> & { decoder?: ResponseDecoder<T> },
  ): Eff<HttpResponse<T>, Throws<HttpClientError>> {
    return suspend(() => {
      const p = typeof path === "string" ? path : path.toString();
      this.recordCall({
        method: "GET",
        path: p,
        headers: options?.headers,
        tag: options?.tag,
      });
      const entry = this.findRoute("GET", p);
      const raw = entry
        ? this.resolveEntry(entry, this.calls[this.calls.length - 1]!)
        : this.defaultResponse;
      if (isHttpClientError(raw)) return fail(raw) as Eff<HttpResponse<T>, Throws<HttpClientError>>;
      return succeed<HttpResponse<T>>({
        status: 200,
        headers: new Headers(),
        contentType: null,
        contentLength: null,
        body: raw as T,
      });
    });
  }

  // ── Internals ───────────────────────────────────────────────────

  private recordCall(call: RecordedCall): void {
    // Omit undefined fields so assertions are tidy
    const entry: any = { method: call.method, path: call.path };
    if (call.json !== undefined) entry.json = call.json;
    if (call.body !== undefined) entry.body = call.body;
    if (call.headers !== undefined) entry.headers = call.headers;
    if (call.tag !== undefined) entry.tag = call.tag;
    this.calls.push(entry);
  }

  private findRoute(method: string, path: string): RouteEntry | undefined {
    for (const route of this.routes) {
      if (route.method === method && pathMatches(route.matcher, path)) return route.entry;
    }
    return undefined;
  }

  private resolveEntry(entry: RouteEntry, call: RecordedCall): unknown | HttpClientError {
    switch (entry.type) {
      case "static":
        return entry.value;
      case "handler":
        return entry.fn(call);
      case "queue":
        // Mutating shift is fine — each mock is instance-scoped and tests are serial.
        return entry.responses.length > 0 ? entry.responses.shift()! : (entry.fallback ?? {});
    }
  }

  private validate<T>(
    raw: unknown,
    schema: ResponseParser<T> | undefined,
    path: string,
  ): Eff<T, Throws<HttpClientError>> {
    if (!schema) return succeed(raw as T);
    const result = schema.safeParse(raw);
    if (result.success) return succeed(result.data);
    return fail(
      new HttpParseError({
        url: path,
        cause: result.error,
        message: "Mock response parse error",
      }),
    ) as Eff<T, Throws<HttpClientError>>;
  }
}

/**
 * Shorthand: return a `MockHttpClient` typed as `HttpClient` so it slots
 * into places expecting the interface.
 */
export function mockHttpClient(): MockHttpClient & HttpClient {
  return new MockHttpClient();
}
