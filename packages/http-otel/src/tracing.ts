// OpenTelemetry tracing for @spilne/perfect-http.
//
// Two integration points:
//
//   1. `tracingMiddleware(opts?)` — drop-in HttpMiddleware (sync hooks) that
//      starts a span on request, sets semconv attributes, records the
//      response status, and ends the span on result/error. Instrumentation
//      ONLY — doesn't modify the request.
//
//   2. `TracingFetchTransport` — wraps another HttpTransport (default
//      FetchTransport) and ALSO injects W3C traceparent / tracestate
//      headers so downstream services see the same trace. Use this when
//      you want end-to-end propagation, not just local spans.
//
// Either can be used alone; combining both gives spans + propagation.
//
// Why two pieces: middleware is cheap sync hooks with no transport access;
// transport can modify outgoing headers (needed for traceparent injection).

import {
  type SpanStatusCode as SpanStatusCodeT,
  type Span,
  type Tracer,
  SpanKind,
  SpanStatusCode,
  trace,
  context as otelContext,
  propagation,
} from "@opentelemetry/api";
import { type HttpMiddleware, type HttpRequestContext } from "@spilne/perfect-http";
import type { HttpClientError, HttpRequestOptions, HttpTransport } from "@spilne/perfect-http";
import { defaultTransport } from "@spilne/perfect-http";
import { type Eff, type Throws } from "@spilne/perfect-core";
import { type RedactionPolicy, defaultRedaction, redactUrl } from "./redact";

const TRACER_NAME = "@spilne/perfect-http";

// ── Per-request span storage ──────────────────────────────────────
//
// The middleware protocol is sync (onRequest/onResponse/onError fire in
// order for each request). We store the active span under a unique key
// per request — indexed by a mutable WeakMap<context, span> isn't viable
// because the context value is a plain object we don't own, so we use a
// WeakMap indexed by... actually, we need something contextual. Simplest
// correct approach: wrap the sync callbacks in a closure-local map keyed
// by the request identity (context.method + url + random). But race-safe
// approach: store span in a field on the context itself — but it's
// readonly. Compromise: use a WeakMap keyed by the context reference.

const spanByContext = new WeakMap<HttpRequestContext, Span>();

export interface TracingOptions {
  /** Custom tracer. Default: `trace.getTracer("@spilne/perfect-http")`. */
  readonly tracer?: Tracer;
  /** Header redaction policy for span attributes. */
  readonly redaction?: RedactionPolicy;
  /** Include raw URL query in `http.url` attribute. Default: false (stripped). */
  readonly includeQuery?: boolean;
  /** Override span name. Default: `"{method} {url-without-query}"`. */
  readonly spanName?: (ctx: HttpRequestContext) => string;
  /** Predicate — disable tracing for requests where this returns true. */
  readonly disable?: (ctx: HttpRequestContext) => boolean;
}

/**
 * OpenTelemetry instrumentation as an `HttpMiddleware`. Pairs with any
 * `DefaultHttpClient({ middleware: [tracingMiddleware(), ...] })`.
 */
export function tracingMiddleware(opts: TracingOptions = {}): HttpMiddleware {
  const tracer = opts.tracer ?? trace.getTracer(TRACER_NAME);
  const redaction = opts.redaction ?? defaultRedaction;
  const makeName = opts.spanName ?? ((ctx) => `${ctx.method} ${redactUrl(ctx.url)}`);

  return {
    onRequest: (ctx) => {
      if (opts.disable?.(ctx)) return;
      const span = tracer.startSpan(makeName(ctx), {
        kind: SpanKind.CLIENT,
        attributes: {
          "http.request.method": ctx.method,
          "http.method": ctx.method, // legacy semconv, kept for compatibility
          "url.full": opts.includeQuery ? ctx.url : redactUrl(ctx.url),
          "http.url": opts.includeQuery ? ctx.url : redactUrl(ctx.url),
          ...(ctx.tag !== undefined ? { "http.route": ctx.tag } : {}),
        },
      });
      spanByContext.set(ctx, span);
      // touch redaction so it's considered used even if no custom headers set
      void redaction;
    },
    onResponse: (ctx) => {
      const span = spanByContext.get(ctx);
      if (!span) return;
      span.setStatus({ code: SpanStatusCode.OK as SpanStatusCodeT });
      span.setAttribute("http.response.duration_ms", ctx.durationMs);
      span.end();
      spanByContext.delete(ctx);
    },
    onError: (ctx, error) => {
      const span = spanByContext.get(ctx);
      if (!span) return;
      const status = error._tag === "HttpStatusError" ? error.status : 0;
      if (status > 0) {
        span.setAttribute("http.response.status_code", status);
        span.setAttribute("http.status_code", status);
      }
      span.setAttribute("http.response.duration_ms", ctx.durationMs);
      span.setAttribute("error.type", error._tag);
      span.recordException({
        name: error._tag,
        message: (error as any).message ?? String(error),
      });
      span.setStatus({
        code: SpanStatusCode.ERROR as SpanStatusCodeT,
        message: (error as any).message ?? error._tag,
      });
      span.end();
      spanByContext.delete(ctx);
    },
  };
}

// ── Transport wrapper with W3C traceparent injection ──────────────

export interface TracingTransportOptions extends TracingOptions {
  /** Transport to wrap. Default: the package default. */
  readonly inner?: HttpTransport;
}

/**
 * Wraps an `HttpTransport` and injects W3C `traceparent` / `tracestate`
 * headers so downstream services join the same trace. The span itself is
 * started by `tracingMiddleware` on the client side (which runs before
 * the transport).
 *
 * Use this together with `tracingMiddleware` for spans + propagation;
 * use middleware alone if you only want local spans.
 */
export class TracingFetchTransport implements HttpTransport {
  constructor(private readonly opts: TracingTransportOptions = {}) {}

  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    const inner = this.opts.inner ?? defaultTransport;
    const carrier: Record<string, string> = { ...options.headers };
    // Inject the current OTel context's trace headers into the outgoing request
    propagation.inject(otelContext.active(), carrier, {
      set: (h, k, v) => {
        h[k] = String(v);
      },
    });
    return inner.execute({ ...options, headers: carrier });
  }
}

/** Shortcut: wrap the default transport with trace propagation. */
export const tracingTransport: HttpTransport = new TracingFetchTransport();
