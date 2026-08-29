# Observability

Perfect exposes logging, tracing, and metrics as core services. The default
runtime includes a console logger, a no-op tracer, and an in-memory metrics
registry; application code can replace any of them with `provide` or `Layer`.

## Structured logging

`Log` creates effects, so timestamps come from `Clock` and annotations follow
the effect context across fibers.

```ts
import { Log } from "@spilne/perfect-core";

const program = Log.annotated(
  Log.info("order accepted", { amount: 42 }),
  { requestId: "req-7", tenant: "acme" },
);

await program.run();
```

`ConsoleLogger` writes readable lines, `JsonLogger` writes one JSON object per
line, and `TestLogger` captures entries without touching the console:

```ts
import { Log, Logger, TestLogger, provide } from "@spilne/perfect-core";

const logger = new TestLogger();
await provide(Log.warn("slow request", { elapsedMs: 900 }), Logger, logger).run();

console.log(logger.messages); // ["slow request"]
```

The available levels are `trace`, `debug`, `info`, `warn`, `error`, and
`fatal`. Logger filtering happens before reading the clock or allocating an
entry.

## Tracing

`withSpan(effect, name, attributes?)` scopes a span around an effect. Nested
spans inherit their parent through the effect context; success, typed failure,
defect, and interruption all end the span exactly once.

```ts
import { TestTracer, Tracer, provide, succeed, withSpan } from "@spilne/perfect-core";

const tracer = new TestTracer();
const program = withSpan(
  withSpan(succeed(42), "load-user", { "user.id": "u-1" }),
  "request",
);

await provide(program, Tracer, tracer).run();
console.log(tracer.finished.map((span) => span.name));
// ["load-user", "request"] — children end first
```

The default tracer is a no-op, and `withSpan` returns the original effect
without creating a span when tracing is disabled.

## Metrics

Counters, gauges, and histograms are cheap mutable instruments obtained once
from the `Metrics` service. Mutation is synchronous; snapshots are effects.

```ts
import { Metrics, eff } from "@spilne/perfect-core";

const program = eff(function* () {
  const requests = yield* Metrics.counter("requests", { route: "/users" });
  const inflight = yield* Metrics.gauge("inflight");
  const latency = yield* Metrics.histogram("latency_seconds");

  requests.inc();
  inflight.adjust(1);
  latency.record(0.018);
  inflight.adjust(-1);

  return yield* Metrics.snapshot;
});
```

Histogram buckets are cumulative and include an implicit `+Inf` bucket.
Labels are sorted before registry lookup, so label insertion order does not
create duplicate instruments.

## OpenTelemetry bridge

Install the general bridge when Perfect spans and metrics should feed an OTel
SDK:

```bash
bun add @spilne/perfect-otel @opentelemetry/api
```

- `OtelTracer` implements the core `Tracer` interface with an OTel tracer.
- `OtelMetricsExporter` exports registry snapshots; counters and histograms
  use deltas between exports, while gauges remain absolute.
- `@spilne/perfect-http-otel` separately supplies HTTP client spans and W3C
  `traceparent` propagation.

See the [`@spilne/perfect-otel` package guide](../packages/otel/README.md) and
[HTTP OpenTelemetry](./14-http-otel.md).

## Next

- [Messaging contracts and Kafka](./16-messaging.md)
- [Testing](./10-testing.md)
