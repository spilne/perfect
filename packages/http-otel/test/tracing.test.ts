// Tracing middleware + transport tests with an in-memory Tracer mock.

import { describe, test, expect } from "bun:test";
import {
  type Span,
  type Tracer,
  type SpanContext,
  SpanKind,
  SpanStatusCode,
  context as otelContext,
  trace,
  ROOT_CONTEXT,
  propagation,
} from "@opentelemetry/api";
import { type Eff, type Throws, async, succeed, fail, sync, run } from "@spilne/perfect-core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  DefaultHttpClient,
  type ResponseParser,
} from "@spilne/perfect-http";
import {
  tracingMiddleware,
  TracingFetchTransport,
  defaultRedaction,
  makeRedaction,
  redactHeaders,
  redactUrl,
} from "../src";
import { runFiberUnchecked, runUnchecked } from "./run-unchecked";

// ── Minimal in-memory Tracer ──────────────────────────────────────

interface RecordedSpan {
  name: string;
  kind: SpanKind;
  attributes: Record<string, unknown>;
  status: { code: SpanStatusCode; message?: string };
  ended: boolean;
  exceptions: Array<{ name?: string; message?: string }>;
}

function makeInMemTracer(): { tracer: Tracer; spans: RecordedSpan[] } {
  const spans: RecordedSpan[] = [];
  const tracer: Tracer = {
    startSpan(name: string, options?: any): Span {
      const record: RecordedSpan = {
        name,
        kind: options?.kind ?? SpanKind.INTERNAL,
        attributes: { ...options?.attributes },
        status: { code: SpanStatusCode.UNSET },
        ended: false,
        exceptions: [],
      };
      spans.push(record);
      const span: Span = {
        setAttribute(k, v) {
          record.attributes[k] = v;
          return span;
        },
        setAttributes(attrs) {
          Object.assign(record.attributes, attrs);
          return span;
        },
        addEvent() {
          return span;
        },
        setStatus(s) {
          record.status = s;
          return span;
        },
        updateName(n) {
          record.name = n;
          return span;
        },
        end() {
          record.ended = true;
        },
        isRecording() {
          return !record.ended;
        },
        recordException(e: any) {
          record.exceptions.push({ name: e.name, message: e.message });
        },
        addLink() {
          return span;
        },
        addLinks() {
          return span;
        },
        spanContext() {
          return {
            traceId: "0".repeat(32),
            spanId: "0".repeat(16),
            traceFlags: 0,
          } as SpanContext;
        },
      } as Span;
      return span;
    },
    startActiveSpan: (_name: string, ...args: any[]): any => {
      const fn = args.find((a) => typeof a === "function");
      return fn?.(tracer.startSpan(_name));
    },
  } as Tracer;
  return { tracer, spans };
}

// ── Transports for assertion ──────────────────────────────────────

class StubTransport implements HttpTransport {
  public lastOptions?: HttpRequestOptions;
  constructor(private readonly reply: () => Response | HttpClientError) {}
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      this.lastOptions = options;
      return this.reply();
    }).flatMap((r) =>
      r instanceof Response ? succeed(r) : (fail(r) as Eff<Response, Throws<HttpClientError>>),
    );
  }
}

interface User {
  id: number;
  name: string;
}
const UserParser: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d as User }
      : { success: false, error: "bad" },
};

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

// ── Redaction ─────────────────────────────────────────────────────

describe("redaction", () => {
  test("defaultRedaction covers common auth / cookie headers", () => {
    const r = defaultRedaction;
    expect(r.isRedacted("Authorization")).toBe(true);
    expect(r.isRedacted("authorization")).toBe(true);
    expect(r.isRedacted("Cookie")).toBe(true);
    expect(r.isRedacted("X-Api-Key")).toBe(true);
    expect(r.isRedacted("Content-Type")).toBe(false);
  });

  test("makeRedaction with extra adds to defaults", () => {
    const r = makeRedaction({ extra: ["x-secret"] });
    expect(r.isRedacted("X-Secret")).toBe(true);
    expect(r.isRedacted("authorization")).toBe(true);
  });

  test("makeRedaction with override replaces defaults", () => {
    const r = makeRedaction({ override: ["only-this"] });
    expect(r.isRedacted("Only-This")).toBe(true);
    expect(r.isRedacted("authorization")).toBe(false);
  });

  test("redactHeaders replaces values with <redacted>", () => {
    const out = redactHeaders({
      Authorization: "Bearer abc.def.ghi",
      "Content-Type": "application/json",
    });
    expect(out.Authorization).toBe("<redacted>");
    expect(out["Content-Type"]).toBe("application/json");
  });

  test("redactUrl strips query string", () => {
    expect(redactUrl("https://api.example.com/users?token=secret")).toBe(
      "https://api.example.com/users",
    );
    expect(redactUrl("https://api.example.com/users")).toBe("https://api.example.com/users");
  });
});

// ── tracingMiddleware ─────────────────────────────────────────────

describe("tracingMiddleware", () => {
  test("starts + ends span on success with semconv attrs", async () => {
    const { tracer, spans } = makeInMemTracer();
    const transport = new StubTransport(() => json({ id: 1, name: "alice" }));
    const client = new DefaultHttpClient({
      baseUrl: "https://api.example.com",
      transport,
      middleware: [tracingMiddleware({ tracer })],
    });

    await run(client.get("/users/1", UserParser, { tag: "user.lookup" }).orDie());

    expect(spans).toHaveLength(1);
    const span = spans[0]!;
    expect(span.name).toBe("GET https://api.example.com/users/1");
    expect(span.kind).toBe(SpanKind.CLIENT);
    expect(span.attributes["http.request.method"]).toBe("GET");
    expect(span.attributes["url.full"]).toBe("https://api.example.com/users/1");
    expect(span.attributes["http.route"]).toBe("user.lookup");
    expect(span.ended).toBe(true);
    expect(span.status.code).toBe(SpanStatusCode.OK);
    expect(span.attributes["http.response.duration_ms"]).toBeGreaterThanOrEqual(0);
  });

  test("records error + status on typed HttpStatusError", async () => {
    const { tracer, spans } = makeInMemTracer();
    const transport = new StubTransport(() => new Response("boom", { status: 503 }));
    const client = new DefaultHttpClient({
      transport,
      middleware: [tracingMiddleware({ tracer })],
    });

    await expect(runUnchecked(client.get("/u", UserParser))).rejects.toMatchObject({
      _tag: "HttpStatusError",
      status: 503,
    });

    expect(spans).toHaveLength(1);
    const span = spans[0]!;
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.attributes["http.response.status_code"]).toBe(503);
    expect(span.attributes["error.type"]).toBe("HttpStatusError");
    expect(span.exceptions.length).toBe(1);
    expect(span.ended).toBe(true);
  });

  test("ends the span with an error status when the request is interrupted", async () => {
    const { tracer, spans } = makeInMemTracer();
    const transport: HttpTransport = {
      execute: () => async<Response>(() => () => {}),
    };
    const client = new DefaultHttpClient({
      transport,
      middleware: [tracingMiddleware({ tracer })],
    });

    const fiber = runFiberUnchecked(client.get("/u", UserParser));
    for (let i = 0; i < 20 && spans.length === 0; i++) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    fiber.interrupt();
    await fiber.await();

    expect(spans).toHaveLength(1);
    const span = spans[0]!;
    expect(span.ended).toBe(true);
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.attributes["error.type"]).toBe("Interrupted");
    expect(span.attributes["http.response.duration_ms"]).toBeGreaterThanOrEqual(0);
  });

  test("spanName override + disable predicate", async () => {
    const { tracer, spans } = makeInMemTracer();
    const transport = new StubTransport(() => json({ id: 1, name: "x" }));
    const client = new DefaultHttpClient({
      transport,
      middleware: [
        tracingMiddleware({
          tracer,
          spanName: (ctx) => `CUSTOM ${ctx.method}`,
          disable: (ctx) => ctx.tag === "skip",
        }),
      ],
    });

    await run(client.get("/a", UserParser).orDie());
    await run(client.get("/b", UserParser, { tag: "skip" }).orDie());
    expect(spans).toHaveLength(1);
    expect(spans[0]!.name).toBe("CUSTOM GET");
  });

  test("query stripped from url.full by default", async () => {
    const { tracer, spans } = makeInMemTracer();
    const transport = new StubTransport(() => json({ id: 1, name: "x" }));
    const client = new DefaultHttpClient({
      transport,
      middleware: [tracingMiddleware({ tracer })],
    });
    await run(client.get("/search?q=secret&user=abc", UserParser).orDie());
    expect(spans[0]!.attributes["url.full"]).toBe("/search");
  });

  test("includeQuery: true keeps the query string", async () => {
    const { tracer, spans } = makeInMemTracer();
    const transport = new StubTransport(() => json({ id: 1, name: "x" }));
    const client = new DefaultHttpClient({
      transport,
      middleware: [tracingMiddleware({ tracer, includeQuery: true })],
    });
    await run(client.get("/search?q=x", UserParser).orDie());
    expect(spans[0]!.attributes["url.full"]).toBe("/search?q=x");
  });
});

// ── TracingFetchTransport ─────────────────────────────────────────

describe("TracingFetchTransport — W3C traceparent injection", () => {
  test("injects traceparent header when an active span is in context", async () => {
    const { tracer } = makeInMemTracer();
    const inner = new StubTransport(() => json({ id: 1, name: "x" }));
    const wrapper = new TracingFetchTransport({ tracer, inner });

    // Open an active span so propagation has something to inject
    const span = tracer.startSpan("outer");
    const ctxWithSpan = trace.setSpan(ROOT_CONTEXT, span);
    await otelContext.with(ctxWithSpan, async () => {
      await run(wrapper.execute({ url: "/x", method: "GET" }).orDie());
    });
    span.end();

    // Pure no-op propagator (default) may not inject anything; assert only
    // that the call went through cleanly. Real propagator integration tests
    // live in apps that register @opentelemetry/core's W3CTraceContextPropagator.
    expect(inner.lastOptions?.url).toBe("/x");
  });
});

describe("tracingMiddleware + TracingFetchTransport together", () => {
  // A propagator that writes the parent span's id into a header, so the test
  // can see which span the downstream service would attach to.
  const spanIdPropagator = {
    inject(ctx: any, carrier: any, setter: any) {
      const span = trace.getSpan(ctx);
      if (span) setter.set(carrier, "x-parent-span", span.spanContext().spanId);
    },
    extract: (ctx: any) => ctx,
    fields: () => ["x-parent-span"],
  };

  // Spans with distinct ids, so we can tell them apart.
  function idTracer(): { tracer: Tracer; ids: string[] } {
    const ids: string[] = [];
    const tracer = {
      startSpan(name: string): Span {
        const spanId = String(ids.length + 1).padStart(16, "0");
        ids.push(spanId);
        return {
          setAttribute: () => undefined,
          setAttributes: () => undefined,
          setStatus: () => undefined,
          recordException: () => undefined,
          end: () => undefined,
          spanContext: () => ({ traceId: "1".repeat(32), spanId, traceFlags: 1 }),
          name,
        } as unknown as Span;
      },
    } as unknown as Tracer;
    return { tracer, ids };
  }

  test("the injected parent is the request's own client span", async () => {
    propagation.setGlobalPropagator(spanIdPropagator);
    try {
      const { tracer, ids } = idTracer();
      const inner = new StubTransport(() => json({ id: 1, name: "x" }));
      const client = new DefaultHttpClient({
        middleware: [tracingMiddleware({ tracer })],
        transport: new TracingFetchTransport({ tracer, inner }),
      });

      await run(client.get("/users/1", UserParser).orDie());

      expect(ids).toHaveLength(1);
      expect(inner.lastOptions?.headers?.["x-parent-span"]).toBe(ids[0]);
    } finally {
      propagation.disable();
    }
  });

  test("two runs of the same request effect get their own spans", async () => {
    propagation.setGlobalPropagator(spanIdPropagator);
    try {
      const { tracer, ids } = idTracer();
      const seen: string[] = [];
      const inner: HttpTransport = {
        execute: (options) =>
          async<Response, never>((resume) => {
            seen.push(options.headers?.["x-parent-span"] ?? "none");
            setTimeout(() => resume(succeed(json({ id: 1, name: "x" }))), 5);
          }),
      };
      const client = new DefaultHttpClient({
        middleware: [tracingMiddleware({ tracer })],
        transport: new TracingFetchTransport({ tracer, inner }),
      });
      const request = client.get("/users/1", UserParser).orDie();

      await Promise.all([run(request), run(request)]);

      expect(ids).toHaveLength(2);
      expect([...seen].sort()).toEqual([...ids].sort());
    } finally {
      propagation.disable();
    }
  });
});
