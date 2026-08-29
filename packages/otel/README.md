# @spilne/perfect-otel

OpenTelemetry bridge for `@spilne/perfect-core` observability. Two pieces:
`OtelTracer` implements core's `Tracer` service on top of an OTel tracer, so
every `withSpan` in your program becomes a real OTel span (with correct
parentage); `OtelMetricsExporter` pushes `MetricsRegistry` snapshots into
OTel instruments.

This is the general bridge — it knows nothing about HTTP. For HTTP-specific
client tracing (CLIENT spans per request, `traceparent` propagation on
`@spilne/perfect-http`), use `@spilne/perfect-http-otel`.

## Install

```bash
bun add @spilne/perfect-otel @opentelemetry/api
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

### Tracing

```ts
import { trace } from "@opentelemetry/api";
import { provide, run, succeed, withSpan, Tracer } from "@spilne/perfect-core";
import { OtelTracer } from "@spilne/perfect-otel";

const program = withSpan(
  succeed(21).map((n) => n * 2),
  "handle",
  { "app.tenant": "acme" },
);

const tracer = new OtelTracer(trace.getTracer("my-app"));
await run(provide(program, Tracer, tracer));
```

Span parentage carries over: core's `withSpan` passes the enclosing Perfect
span as the parent, and the bridge maps it back to its OTel span — traces
nest identically in both worlds. Typed failures set span status to ERROR and
record the exception; interruption sets ERROR with a `perfect.interrupted`
attribute. When no tracer is provided, `withSpan` is zero-cost.

### Metrics

```ts
import { metrics } from "@opentelemetry/api";
import { defaultMetricsRegistry } from "@spilne/perfect-core";
import { OtelMetricsExporter } from "@spilne/perfect-otel";

const exporter = new OtelMetricsExporter(metrics.getMeter("my-app"), defaultMetricsRegistry);

// Call on your export cadence — a Stream.tick loop, a
// PeriodicExportingMetricReader callback, a shutdown hook.
exporter.export();
```

Counters and histograms are exported as deltas between calls, so repeated
exports don't double-count; gauges are absolute. `export()` is safe to call
repeatedly and returns the snapshot it exported.

## Features

- `OtelTracer` — core `Tracer` service backed by any `@opentelemetry/api`
  tracer; parentage, error status, exception recording, interruption marking
- `OtelMetricsExporter` — `Counter` / `Gauge` / `Histogram` from core's
  `MetricsRegistry` mapped to OTel instruments, labels preserved,
  delta-correct on repeat export

## Links

- Repo: https://github.com/spilne/perfect
- Core observability APIs (`Log`, `withSpan`, `Metrics`):
  [`documentation/15-observability.md`](../../documentation/15-observability.md)
- HTTP client tracing: `@spilne/perfect-http-otel`
