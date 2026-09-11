# HTTP — OpenTelemetry

`@spilne/perfect-http-otel` provides drop-in OpenTelemetry tracing for
`@spilne/perfect-http`. Two integration points compose independently:

| Piece | What it does |
|---|---|
| `tracingMiddleware(opts?)` | `HttpMiddleware` — starts/ends a CLIENT span per request with semantic HTTP attributes |
| `TracingFetchTransport` | `HttpTransport` wrapper — injects W3C `traceparent` / `tracestate` headers so downstream services join the trace |
| `tracingTransport` | the default — `TracingFetchTransport` wrapping `FetchTransport` |

Either can be used independently. The middleware records client spans; the
transport propagates the context active at request time. The middleware does
not install its newly-created span as the active context, so combining them
does not make that span the parent of the downstream request.

Configure an OpenTelemetry provider and propagator in your application for
exported spans and outgoing trace headers. The excerpts below use shared
in-memory fixtures from the [full tracing example](../packages/http-otel/examples/01-tracing.ts).

```bash
bun add @spilne/perfect-http-otel @opentelemetry/api
```

## Spans on every request

<!-- @embed packages/http-otel/examples/01-tracing.ts#tracing-success -->

```ts
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { DefaultHttpClient } from "@spilne/perfect-http";
import { tracingMiddleware } from "@spilne/perfect-http-otel";

// tracingMiddleware starts a CLIENT span on every request, fills semantic
// HTTP attributes (http.request.method, url.full, http.response.status_code,
// http.response.duration_ms), and ends the span on result.
const { tracer, spans } = inMemTracer();
const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
  middleware: [tracingMiddleware({ tracer })],
});

await client.get("/users/1", UserSchema, { tag: "user.lookup" }).orDie().run();

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
import { DefaultHttpClient, HttpStatusError } from "@spilne/perfect-http";
import { tracingMiddleware } from "@spilne/perfect-http-otel";

// On error, the span status flips to ERROR, http.response.status_code is
// recorded, and error.type carries the typed error tag for filtering.
const { tracer: t2, spans: errSpans } = inMemTracer();
const failing = new DefaultHttpClient({
  transport: new StubTransport(() => new Response("down", { status: 503 })),
  middleware: [tracingMiddleware({ tracer: t2 })],
});

let caught: unknown;
try {
  await failing.get("/u", UserSchema).orDie().run();
} catch (e) {
  caught = e;
}
if (!(caught instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
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
import { tracingMiddleware, tracingTransport, TracingFetchTransport } from "@spilne/perfect-http-otel";
import { DefaultHttpClient, FetchTransport } from "@spilne/perfect-http";

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

URL queries are stripped from `url.full` by default. This is not a general
privacy guarantee: URL paths, fragments, error messages, and custom span names
may still contain sensitive data. Use `disable` or a safe `spanName` where
appropriate, and avoid placing secrets in URLs.

The middleware currently does not record headers. `redactHeaders` is an
explicit helper for custom instrumentation; passing the `redaction` option
does not sanitize arbitrary attributes or error messages.

<!-- @embed packages/http-otel/examples/01-tracing.ts#tracing-redaction -->

```ts
import { makeRedaction, redactHeaders } from "@spilne/perfect-http-otel";

// Query stripping does not sanitize paths or error messages. For custom
// header attributes, apply redactHeaders explicitly; the middleware itself
// does not record headers.
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
| `tracer` | `trace.getTracer("@spilne/perfect-http")` | custom Tracer instance |
| `redaction` | `defaultRedaction` | reserved header policy; current middleware does not record headers |
| `includeQuery` | `false` | keep query string in `url.full` |
| `spanName` | `"{method} {url-no-query}"` | override per-request |
| `disable` | `() => false` | predicate to skip tracing for matched requests |

## Combine middleware + transport

```ts
import { DefaultHttpClient } from "@spilne/perfect-http";
import { tracingMiddleware, tracingTransport } from "@spilne/perfect-http-otel";

const client = new DefaultHttpClient({
  transport: tracingTransport,            // injects traceparent
  middleware: [tracingMiddleware()],      // emits spans locally
});
```

This records request spans and propagates the application's active context.
Provider/exporter setup and context activation remain application responsibilities.
