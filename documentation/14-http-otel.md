# HTTP — OpenTelemetry

`@perfect/http-otel` provides drop-in OpenTelemetry tracing for
`@perfect/http`. Two integration points compose independently:

| Piece | What it does |
|---|---|
| `tracingMiddleware(opts?)` | `HttpMiddleware` — starts/ends a CLIENT span per request with semantic HTTP attributes |
| `TracingFetchTransport` | `HttpTransport` wrapper — injects W3C `traceparent` / `tracestate` headers so downstream services join the trace |
| `tracingTransport` | the default — `TracingFetchTransport` wrapping `FetchTransport` |

Either alone is useful; combining both gives you spans **and** propagation.

```bash
bun add @perfect/http-otel @opentelemetry/api
```

## Spans on every request

<!-- @embed packages/http-otel/examples/01-tracing.ts#tracing-success -->
```ts
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { DefaultHttpClient } from "@perfect/http";

// tracingMiddleware starts a CLIENT span on every request, fills semantic
// HTTP attributes (http.request.method, url.full, http.response.status_code,
// http.response.duration_ms), and ends the span on result.
const { tracer, spans } = inMemTracer();
const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
  middleware: [tracingMiddleware({ tracer })],
});

await client.get("/users/1", UserSchema, { tag: "user.lookup" }).run();

console.log(spans.length); // → 1
console.log(spans[0]!.name); // → "GET https://api.example.com/users/1"
console.log(spans[0]!.kind); // → SpanKind.CLIENT
console.log(spans[0]!.attributes["http.request.method"]); // → "GET"
console.log(spans[0]!.attributes["http.route"]); // → "user.lookup"
console.log(spans[0]!.status.code); // → SpanStatusCode.OK
console.log(spans[0]!.ended); // → true
```
<!-- @end -->

The request `tag` (when provided to `client.get`/`post`/etc.) becomes
`http.route` — a low-cardinality label suitable for grouping in dashboards.

### Errors

<!-- @embed packages/http-otel/examples/01-tracing.ts#tracing-error -->
```ts
import { SpanStatusCode } from "@opentelemetry/api";
import { DefaultHttpClient } from "@perfect/http";
import { tracingMiddleware } from "@perfect/core";

// On error, the span status flips to ERROR, http.response.status_code is
// recorded, and error.type carries the typed error tag for filtering.
const { tracer: t2, spans: errSpans } = inMemTracer();
const failing = new DefaultHttpClient({
  transport: new StubTransport(() => new Response("down", { status: 503 })),
  middleware: [tracingMiddleware({ tracer: t2 })],
});

let caught: any;
try { await failing.get("/u", UserSchema).run(); } catch (e) { caught = e; }
console.log(caught._tag); // → "HttpStatusError"
console.log(errSpans[0]!.status.code); // → SpanStatusCode.ERROR
console.log(errSpans[0]!.attributes["http.response.status_code"]); // → 503
console.log(errSpans[0]!.attributes["error.type"]); // → "HttpStatusError"
```
<!-- @end -->

## W3C trace propagation

`TracingFetchTransport` wraps another transport and injects the active
span's `traceparent` / `tracestate` headers into outgoing requests. Use it
when you want downstream services to join the same trace, not just
client-side observability.

```ts
import { tracingTransport, TracingFetchTransport } from "@perfect/http-otel";
import { FetchTransport } from "@perfect/http";

// Default: wraps FetchTransport.
const transport = tracingTransport;

// Or wrap a custom inner transport:
const custom = new TracingFetchTransport({ inner: new FetchTransport() });

const client = new DefaultHttpClient({
  transport,
  middleware: [tracingMiddleware()],
});
```

The injection uses `@opentelemetry/api`'s `propagation.inject` against the
active context, so it respects whatever propagator your runtime registers
(`W3CTraceContextPropagator` is conventional).

## Redaction

URL queries are stripped from `url.full` by default to avoid PII leaks into
spans. Header redaction is pluggable.

<!-- @embed packages/http-otel/examples/01-tracing.ts#tracing-redaction -->
```ts
import { makeRedaction, redactHeaders } from "@perfect/core";

// URL queries are stripped from url.full by default to keep span attributes
// PII-free. Pass includeQuery: true to keep them. Header redaction is
// pluggable via makeRedaction({ extra, override }) — defaults cover
// authorization, cookie, x-api-key, and similar.
const r = makeRedaction({ extra: ["x-secret"] });
const out = redactHeaders(
  { Authorization: "Bearer xyz", "X-Secret": "shh", "Content-Type": "application/json" },
  r,
);
console.log(out.Authorization); // → "<redacted>"
console.log(out["X-Secret"]); // → "<redacted>"
console.log(out["Content-Type"]); // → "application/json"
```
<!-- @end -->

## Options

| Option | Default | Purpose |
|---|---|---|
| `tracer` | `trace.getTracer("@perfect/http")` | custom Tracer instance |
| `redaction` | `defaultRedaction` | header redaction policy |
| `includeQuery` | `false` | keep query string in `url.full` |
| `spanName` | `"{method} {url-no-query}"` | override per-request |
| `disable` | `() => false` | predicate to skip tracing for matched requests |

## Combine middleware + transport

```ts
import { DefaultHttpClient } from "@perfect/http";
import { tracingMiddleware, tracingTransport } from "@perfect/http-otel";

const client = new DefaultHttpClient({
  transport: tracingTransport,            // injects traceparent
  middleware: [tracingMiddleware()],      // emits spans locally
});
```

That's the full integration — spans on every call, plus end-to-end trace
propagation to downstream services.
