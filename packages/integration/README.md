# @perfect/integration

Testcontainers-based integration tests that run `@perfect/kafka` against real
backends. Private package — never published, and deliberately **not** wired
into CI or the default release/test pipelines.

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
adapter needs).

## Containers

| Backend              | Image                           | Used by                                         |
| -------------------- | ------------------------------- | ----------------------------------------------- |
| Kafka (default)      | `redpandadata/redpanda:v24.3.7` | `withKafka`                                     |
| Kafka (full, opt-in) | `confluentinc/cp-kafka:7.9.1`   | `withApacheKafka` (`KAFKA_FULL=1`)              |
| Redis                | `redis:7-alpine`                | `withRedis` (reserved for future redis work)    |
| Postgres             | `postgres:17-alpine`            | `withPostgres` (reserved for the postgres work) |

## Layout

- `src/infra.ts` — container lifecycle wrappers (`withKafka`, `withRedis`,
  `withPostgres`, `withAll`), Docker gating, `eventually()`, `uniqueName()`.
- `src/adapters/kafkajs-adapter.ts` — kafkajs → `KafkaClient`, branding
  identifiers (`TopicName`/`PartitionId`/`KafkaOffset`) at the driver boundary.
- `src/adapters/stream-adapter.ts` — `@platformatic/kafka` → `KafkaClient`.
- `test/kafka.test.ts` — round-trip, batched-commit, and shuffle-transport
  suites against a real broker.
