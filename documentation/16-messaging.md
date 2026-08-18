# Messaging Contracts and Kafka

`@perfect/core/connect` is the queue-agnostic boundary between applications,
connectors, and `@perfect/topology`. Backends implement only the capabilities
they actually support.

## Capability interfaces

| Capability | Contract |
| --- | --- |
| Consume values | `Streamable<T, S>` |
| Publish values | `Sinkable<T, S>` / `KeyedSinkable<T, S>` |
| Manual acknowledgement | `Acknowledgeable<T, S>` → `Envelope<T, S>` |
| Replay from a position | `Replayable<T, S>` |
| Partition selection | `Partitionable<T, S>` |
| Save/restore source position | `Checkpointable<T, S>` |
| Managed assignment lifecycle | `ManagedAcknowledgeable<T, S>` |
| Atomic publication | `TransactionalSinkable<T, S, Tx>` |

The `S` parameter is the backend effect union. A generic connector function
therefore preserves both source and sink failures:

```ts
import type { Eff } from "@perfect/core";
import type { Sinkable, Streamable } from "@perfect/core/connect";

function copy<T, SourceS, SinkS>(
  source: Streamable<T, SourceS>,
  sink: Sinkable<T, SinkS>,
): Eff<void, SourceS | SinkS> {
  return source.subscribe().evalMap((value) => sink.publish(value)).drain();
}
```

## Codecs and identifiers

Every endpoint carries a `Codec<T>`. Core supplies `JsonCodec`,
`LosslessJsonCodec`, schema-derived codecs, tuple/record/array combinators,
canonical JSON, and stable payload hashes.

Confusable durable identifiers are branded: `ConsumerGroup`, `Partition`,
`TopologyId`, `StageId`, and related constructors prevent accidentally passing
one string/number identity where another is required. Backend-specific offsets
remain opaque at the shared boundary.

## Acknowledgement

Use `subscribe()` when successful delivery can be acknowledged by the
connector automatically. Use `subscribeAck()` for at-least-once processing:

```ts
const handled = source.subscribeAck().evalMap((envelope) =>
  process(envelope.value).flatMap(() => envelope.ack()),
);
```

Acknowledgement happens after processing. If processing fails, do not `ack`;
use `nack` only when the backend's immediate-redelivery behavior is desired.
`autoCommitBatchWithin` is backend-neutral, while Kafka's
`commitBatchWithin` writes explicit Kafka offsets.

## Kafka

`@perfect/kafka` contains the connector and driver interfaces. Install one
driver adapter separately:

```bash
bun add @perfect/kafka @perfect/kafka-kafkajs kafkajs
# or, on supported Node versions:
bun add @perfect/kafka @perfect/kafka-platformatic @platformatic/kafka
```

```ts
import { kafkaConfig } from "@perfect/kafka";
import { createKafkajsClient } from "@perfect/kafka-kafkajs";

interface Order {
  id: string;
  amount: number;
}

const orders = kafkaConfig<Order>()
  .client(
    createKafkajsClient({
      brokers: ["localhost:9092"],
      clientId: "billing-service",
    }),
  )
  .topic("orders")
  .group("billing")
  .consumer({ sessionTimeout: 30_000 })
  .build();

await orders.publish({ id: "o-1", amount: 42 }, { key: "o-1" }).run();
const first = await orders.subscribe().take(1).toArray().run();
```

`KafkaConfigBuilder` validates that client, topic, and group are present and
brands their identifiers. `batchEmit(true)` preserves callback-driver fetch
batches as native Stream chunks.

For explicit batched commits, use
`subscribeAckWithHandle({ autoCommit: false })`, pass the returned joined
consumer to `commitBatchWithin`, and close the subscription in `finally`.
This avoids committing through a different consumer-group member.

### Drivers

| Package | Runtime | Consumption bridge | Configuration |
| --- | --- | --- | --- |
| `@perfect/kafka-kafkajs` | Bun and Node.js | callback `eachMessage` / `eachBatch` | broker string/array or KafkaJS `KafkaConfig`; `createKafkajsTopic` is available for bootstrap/tests |
| `@perfect/kafka-platformatic` | Node.js | native async message stream through a bounded pause/resume bridge | broker string/array or `PlatformaticAdapterConfig` with producer, consumer, admin, and buffer options |

Both drivers implement the same `KafkaClient` boundary, including producer,
consumer, admin, seek/replay, offset commits, and managed partition assignment.
Platformatic's consumer currently requires Node; use KafkaJS under Bun.

Both adapters are exercised against real brokers in the opt-in integration
suite. The core Kafka package keeps unit tests broker-free through the
`KafkaClient` interface.

## Next

- [Redis and PostgreSQL backends](./17-distributed-backends.md)
- [Stateful topologies](./18-topologies.md)
