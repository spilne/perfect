# @perfect/integration

Testcontainers-based integration tests for the Kafka, Redis, and Postgres backends.
Private package — never published, and deliberately **not** wired into CI or the
default release/test pipelines.

## Running

Requires a running Docker daemon. First run pulls images (may take minutes).

```bash
bun test packages/integration        # from the repo root
cd packages/integration && bun test  # or from the package
```

Without Docker the suites skip cleanly (a note is printed), so a plain
`bun test --recursive packages/` stays green anywhere. In CI the suites also
skip unless `PERFECT_INTEGRATION=1` is set — this package is opt-in.

`KAFKA_FULL=1` additionally runs the `@platformatic/kafka` adapter suite
against real Apache Kafka (slow JVM startup; Redpanda has API gaps that
adapter needs). That test bundles the probe and runs it with the package-local
Node.js 24 binary because Platformatic Kafka 2.9 does not support Bun. The
default Redpanda lane exercises the KafkaJS adapter.

## Containers

| Backend              | Image                           | Used by                                         |
| -------------------- | ------------------------------- | ----------------------------------------------- |
| Kafka (default)      | `redpandadata/redpanda:v24.3.7` | `withKafka`                                     |
| Kafka (full, opt-in) | `confluentinc/cp-kafka:8.2.2`   | `withApacheKafka` (`KAFKA_FULL=1`)              |
| Redis                | `redis:7-alpine`                | `withRedis`; `@perfect/redis` integration tests |
| Postgres             | `postgres:17-alpine`            | `withPostgres`; `@perfect/postgres` tests       |

## Layout

- `src/infra.ts` — container lifecycle wrappers (`withKafka`, `withRedis`,
  `withPostgres`, `withAll`), Docker gating, `eventually()`, `uniqueName()`.
- `@perfect/kafka-kafkajs` — KafkaJS → `KafkaClient`, branding identifiers at
  the driver boundary.
- `@perfect/kafka-platformatic` — Platformatic's event-emitting
  `MessagesStream` → bounded Perfect stream bridge.
- `test/kafka.test.ts` — round-trip, batched-commit, and shuffle-transport
  suites against a real broker.
