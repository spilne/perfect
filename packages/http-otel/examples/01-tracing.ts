// OpenTelemetry tracing — middleware (spans) + transport (W3C propagation).
//
// Uses an in-memory tracer so the example runs without an OTel collector.
//
// Run: bun packages/http-otel/examples/01-tracing.ts

import {
  type Span,
  type SpanContext,
  type Tracer,
  SpanKind,
  SpanStatusCode,
} from "@opentelemetry/api";
import {
  type Eff,
  type Throws,
  succeed,
  fail,
  run,
} from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  DefaultHttpClient,
} from "@perfect/http";
import {
  defaultRedaction,
  makeRedaction,
  redactHeaders,
  tracingMiddleware,
} from "../src";
import { assertEq, assertContains } from "./_assert";

// ── In-memory tracer (so this runs without an OTel collector) ─────

interface RecordedSpan {
  name: string;
  kind: SpanKind;
  attributes: Record<string, unknown>;
  status: { code: SpanStatusCode };
  ended: boolean;
}

function inMemTracer(): { tracer: Tracer; spans: RecordedSpan[] } {
  const spans: RecordedSpan[] = [];
  const tracer: Tracer = {
    startSpan(name: string, options?: any): Span {
      const rec: RecordedSpan = {
        name,
        kind: options?.kind ?? SpanKind.INTERNAL,
        attributes: { ...(options?.attributes ?? {}) },
        status: { code: SpanStatusCode.UNSET },
        ended: false,
      };
      spans.push(rec);
      const span: Span = {
        setAttribute(k, v) { rec.attributes[k] = v; return span; },
        setAttributes(a) { Object.assign(rec.attributes, a); return span; },
        addEvent() { return span; },
        setStatus(s) { rec.status = s; return span; },
        updateName(n) { rec.name = n; return span; },
        end() { rec.ended = true; },
        isRecording() { return !rec.ended; },
        recordException() {},
        addLink() { return span; },
        addLinks() { return span; },
        spanContext() {
          return { traceId: "0".repeat(32), spanId: "0".repeat(16), traceFlags: 0 } as SpanContext;
        },
      } as Span;
      return span;
    },
    startActiveSpan: (_n: string, ...args: any[]): any => {
      const fn = args.find((a) => typeof a === "function");
      return fn?.(tracer.startSpan(_n));
    },
  } as Tracer;
  return { tracer, spans };
}

// ── Stub transport ────────────────────────────────────────────────

class StubTransport implements HttpTransport {
  constructor(private readonly reply: () => Response) {}
  execute(_o: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return succeed(this.reply());
  }
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

interface User { id: number; name: string }
const UserSchema: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d }
      : { success: false, error: "no" },
};

// >>> example: tracing-success
// tracingMiddleware starts a CLIENT span on every request, fills semantic
// HTTP attributes (http.request.method, url.full, http.response.status_code,
// http.response.duration_ms), and ends the span on result.
const { tracer, spans } = inMemTracer();
const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
  middleware: [tracingMiddleware({ tracer })],
});

await run(client.get("/users/1", UserSchema, { tag: "user.lookup" }));

assertEq(spans.length, 1);
assertEq(spans[0]!.name, "GET https://api.example.com/users/1");
assertEq(spans[0]!.kind, SpanKind.CLIENT);
assertEq(spans[0]!.attributes["http.request.method"], "GET");
assertEq(spans[0]!.attributes["http.route"], "user.lookup");
assertEq(spans[0]!.status.code, SpanStatusCode.OK);
assertEq(spans[0]!.ended, true);
// <<< example

// >>> example: tracing-error
// On error, the span status flips to ERROR, http.response.status_code is
// recorded, and error.type carries the typed error tag for filtering.
const { tracer: t2, spans: errSpans } = inMemTracer();
const failing = new DefaultHttpClient({
  transport: new StubTransport(() => new Response("down", { status: 503 })),
  middleware: [tracingMiddleware({ tracer: t2 })],
});

let caught: any;
try { await run(failing.get("/u", UserSchema)); } catch (e) { caught = e; }
assertEq(caught._tag, "HttpStatusError");
assertEq(errSpans[0]!.status.code, SpanStatusCode.ERROR);
assertEq(errSpans[0]!.attributes["http.response.status_code"], 503);
assertEq(errSpans[0]!.attributes["error.type"], "HttpStatusError");
// <<< example

// >>> example: tracing-redaction
// URL queries are stripped from url.full by default to keep span attributes
// PII-free. Pass includeQuery: true to keep them. Header redaction is
// pluggable via makeRedaction({ extra, override }) — defaults cover
// authorization, cookie, x-api-key, and similar.
const r = makeRedaction({ extra: ["x-secret"] });
const out = redactHeaders(
  { Authorization: "Bearer xyz", "X-Secret": "shh", "Content-Type": "application/json" },
  r,
);
assertEq(out.Authorization, "<redacted>");
assertEq(out["X-Secret"], "<redacted>");
assertEq(out["Content-Type"], "application/json");
// <<< example
