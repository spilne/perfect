# Package Map

Perfect is split into small packages so applications can install the runtime,
connectors, and compiler integrations independently. This page maps every
workspace package to its detailed guide and package reference.

## Public packages

<!-- package-coverage:start -->

| Package | Purpose | Detailed guide |
| --- | --- | --- |
| `@spilne/perfect-core` | `Eff`, typed errors/dependencies, fibers, scopes, streams, concurrency, observability services, and connector contracts | [Core guide](./02-effects.md), [Streams](./09-streams.md), [package reference](../packages/core/README.md) |
| `@spilne/perfect-http` | Typed HTTP client, middleware, retries, streaming decoders, and mocks | [HTTP](./13-http.md), [package reference](../packages/http/README.md) |
| `@spilne/perfect-http-otel` | HTTP client spans, W3C trace propagation, and attribute redaction | [HTTP OpenTelemetry](./14-http-otel.md), [package reference](../packages/http-otel/README.md) |
| `@spilne/perfect-otel` | General OpenTelemetry bridge for the core `Tracer` and `Metrics` services | [Observability](./15-observability.md), [package reference](../packages/otel/README.md) |
| `@spilne/perfect-kafka` | Driver-neutral Kafka topic, replay, acknowledgement, batched commits, brands, and topology shuffle transport | [Messaging and Kafka](./16-messaging.md), [package reference](../packages/kafka/README.md) |
| `@spilne/perfect-kafka-kafkajs` | KafkaJS driver adapter for Bun and Node.js | [Kafka drivers](./16-messaging.md#drivers), [package reference](../packages/kafka-kafkajs/README.md) |
| `@spilne/perfect-kafka-platformatic` | Platformatic Kafka driver adapter and bounded native-stream bridge for Node.js | [Kafka drivers](./16-messaging.md#drivers), [package reference](../packages/kafka-platformatic/README.md) |
| `@spilne/perfect-redis` | Distributed coordination primitives, cache/state backends, Redis Streams, and Pub/Sub | [Redis backend](./17-distributed-backends.md#redis), [package reference](../packages/redis/README.md) |
| `@spilne/perfect-postgres` | PostgreSQL coordination, queues, change streams, state, and the `@spilne/perfect-postgres/pgmq` subpath | [PostgreSQL backend](./17-distributed-backends.md#postgresql), [package reference](../packages/postgres/README.md) |
| `@spilne/perfect-topology` | Stateful event-processing DAGs, windows, joins, shuffle stages, partition state, and delivery guarantees | [Stateful topologies](./18-topologies.md), [package reference](../packages/topology/README.md) |
| `@spilne/perfect-swc-plugin` | Canonical SWC WASM compiler for `eff(($) => ...)` | [Syntax](./03-syntax.md#eff-rewriter), [package reference](../packages/swc-plugin/README.md) |
| `@spilne/perfect-transform` | Bun preload/plugin and source-text compiler for `eff($)` and `for { ... } yield` | [Syntax](./03-syntax.md#eff-rewriter), [package reference](../packages/transform/README.md) |

## Internal validation workspace

| Package | Purpose | Reference |
| --- | --- | --- |
| `@spilne/perfect-integration` | Private Testcontainers harness for KafkaJS/Redpanda, Platformatic/Apache Kafka, Redis, and PostgreSQL | [Integration test reference](../packages/integration/README.md) |

<!-- package-coverage:end -->

`@spilne/perfect-integration` is private and is never published. Its real-service
tests are opt-in because they require Docker; normal unit and package tests
remain broker/database-free.

Directories without a `package.json` are not workspace packages and are not
part of this map.

## Subpath exports

| Export | Contents |
| --- | --- |
| `@spilne/perfect-core/connect` | Backend-neutral messaging, codecs, checkpoints, transactional sinks, and partitioned-state contracts |
| `@spilne/perfect-core/retry` | `RetryPolicy`, `Schedule`, `retryWith`, and scheduled repetition |
| `@spilne/perfect-core/stream` | `Stream`, `Chunk`, `Sink`, and `Pipes` |
| `@spilne/perfect-core/syntax` | Comprehension syntax and fluent syntax installation |
| `@spilne/perfect-core/worker` | Worker executors and `WorkerPool` |
| `@spilne/perfect-postgres/pgmq` | Typed `PgmqQueue` plus low-level PGMQ queue, read, send, acknowledgement, metrics, notify, and FIFO helpers |
| `@spilne/perfect-transform/preload` | Bun preload with both source syntaxes and automatic imports |
| `@spilne/perfect-transform/plugin` | Bun plugin for `eff($)` with automatic imports |
| `@spilne/perfect-transform/bun-plugin` | Bun plugin for both source syntaxes without automatic imports |

The documentation build checks this package map against every
`packages/*/package.json` and requires each workspace package to have a
README. Adding or removing a package without updating this page fails
`bun run documentation:check`.

## Next

- [Getting started](./01-getting-started.md)
- [Comparison](./comparison.md)
