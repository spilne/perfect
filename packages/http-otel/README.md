# @spilne/perfect-http-otel

Drop-in OpenTelemetry tracing for `@spilne/perfect-http`. Two integration points
that compose independently: `tracingMiddleware` starts a CLIENT span per
request with semantic HTTP attributes, and `TracingFetchTransport` injects
W3C `traceparent` / `tracestate` headers so downstream services join the
trace. Used together, the request's CLIENT span is sent as the parent, so
downstream services appear as children of the HTTP call. Configure an
OpenTelemetry provider and propagator before using these integrations.

This package is HTTP-specific. For the general bridge — running `@spilne/perfect-core`'s
`Tracer` service and `MetricsRegistry` on OpenTelemetry — use `@spilne/perfect-otel`.

## Install

```bash
bun add @spilne/perfect-http-otel @opentelemetry/api
```

## Quickstart

```ts
import { DefaultHttpClient, identityParser } from "@spilne/perfect-http";
import { tracingMiddleware, tracingTransport } from "@spilne/perfect-http-otel";

const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: tracingTransport, // W3C traceparent injection
  middleware: [tracingMiddleware()], // CLIENT span per request
});

await client.get("/users/1", identityParser, { tag: "user.lookup" }).orDie().run();
// span "GET https://api.example.com/users/1", kind CLIENT:
//   http.request.method = "GET"
//   http.route          = "user.lookup"   (the low-cardinality request tag)
//   http.response.duration_ms, url.full
// Non-OK status errors also record http.response.status_code.
```

The default tracer is `trace.getTracer("@spilne/perfect-http")`; pass your own via
`tracingMiddleware({ tracer })`. On error the span status flips to ERROR,
the status code is recorded, and `error.type` carries the typed error tag
(`"HttpStatusError"`, `"HttpTimeoutError"`, …) for filtering.

## Redaction

URL queries are stripped from `url.full` by default (`includeQuery: true`
opts back in). Paths, fragments, and error messages are not sanitized, so this
is not a guarantee that spans contain no sensitive data. The middleware does
not currently record headers. Apply `redactHeaders` explicitly when custom
instrumentation records them:

```ts
import { makeRedaction, redactHeaders } from "@spilne/perfect-http-otel";

const policy = makeRedaction({ extra: ["x-secret"] });
redactHeaders({ Authorization: "Bearer xyz", "X-Secret": "shh" }, policy);
// → { Authorization: "<redacted>", "X-Secret": "<redacted>" }
```

## Features

- `tracingMiddleware(opts?)` — `HttpMiddleware`; CLIENT span per request with
  semantic HTTP attributes; options for `tracer`, `redaction`, `includeQuery`,
  `spanName`, and a `disable` predicate
- `TracingFetchTransport` — wraps any `HttpTransport`, injects `traceparent` /
  `tracestate`
- `tracingTransport` — the default instance, wrapping `FetchTransport`
- `defaultRedaction` / `makeRedaction` / `redactHeaders` / `redactUrl` —
  pluggable `RedactionPolicy`

## Links

- Repo: https://github.com/spilne/perfect
- Full guide: [`documentation/14-http-otel.md`](../../documentation/14-http-otel.md)
- Runnable example (no collector needed): [`examples/01-tracing.ts`](./examples/01-tracing.ts)
- General core Tracer/Metrics bridge: `@spilne/perfect-otel`
