# @perfect/kafka

Kafka backend for the `@perfect/core/connect` contracts. `KafkaTopic<T>`
implements `Partitionable`, `Replayable`, `Acknowledgeable`, `KeyedSinkable`,
and `Checkpointable`, so a topic plugs directly into anything that consumes
those contracts — `@perfect/topology` DAGs included. The Kafka-specific
machinery (offsets, partitions, consumer groups, the eachMessage/eachBatch
driver model) lives here; the queue-agnostic layer (`Envelope`,
`OffsetTracker`, `autoCommitBatchWithin`, …) stays in `@perfect/core/connect`.

## Install

```bash
bun add @perfect/kafka @perfect/kafka-kafkajs kafkajs
```

> Not yet published to npm — install from the workspace for now.

**No driver is bundled.** Install `@perfect/kafka-kafkajs` for KafkaJS on Bun
or Node.js, or `@perfect/kafka-platformatic` for Platformatic Kafka on Node.js.
You can also implement the small `KafkaClient` driver interface directly.

## Quickstart

```ts
import { fromPromise, run } from "@perfect/core";
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

// produce — JSON codec by default, pass codec: to override
await run(orders.publish({ id: "o-1", amount: 42 }, { key: "o-1" }));

// consume — decoded values as a core Stream; take() closes the consumer
const first = await orders.subscribe().take(1).toArray().run();

// at-least-once — Envelope<T> with ack/nack; acked offsets are committed
// contiguously in the background (commitIntervalMs, default 1s)
await run(
  orders
    .subscribeAck()
    .evalMap((env) =>
      fromPromise(
        () => handle(env.value),
        (cause) => cause,
      ).flatMap(() => env.ack()),
    )
    .drain()
    .orDie(),
);
```

For explicit fs2-kafka-style batched commits, create a
`subscribeAckWithHandle({ autoCommit: false })` subscription and pass its `consumer` and
`topic` to `commitBatchWithin`. The handle guarantees that consumption and commits use
the same joined consumer group member. Commit failures remain typed as
`Throws<KafkaCommitError>` instead of being swallowed. Handle-based subscriptions own
their consumer explicitly, so call `subscription.close()` in a `finally` block.

## Features

- `KafkaTopic<T>` — one class, all connect contracts:
  - `publish` / `publishBatch` (keyed, codec-encoded)
  - `subscribe` / `subscribeFrom` (earliest, latest, specific-offset, or timestamp replay)
  - `subscribeAck` — `Stream<Envelope<T>>` with background contiguous commits
  - `subscribeAckWithHandle` — explicit consumer ownership for external commit pipes
  - `batchEmit: true` — preserve callback-driver fetch batches as native Stream chunks
  - checkpoint support for `@perfect/topology`
- `commitBatchWithin` — typed batched offset-commit pipe (count or time window)
- `KafkaConfigBuilder` / `kafkaConfig<T>()` — validated fluent construction
  for the client, topic, group, codec, batch mode, and consumer tuning
- `KafkaShuffleTransport` — Kafka-backed `ShuffleTransport` for distributed
  topology stages
- Driver-agnostic types — `KafkaClient`, `KafkaConsumer`, `KafkaProducer`,
  `KafkaAdmin`, `KafkaConsumerOptions` (session/poll/heartbeat tuning knobs
  to avoid the slow-handler → rebalance → redelivery loop)
- Driver failures remain typed as `Throws<KafkaError>` through publish,
  consumption, acknowledgement, and commit operations

## Links

- Repo: https://github.com/spilne/perfect
- Connect contracts: `@perfect/core/connect`
- Messaging guide: [`documentation/16-messaging.md`](../../documentation/16-messaging.md)
- Stateful topology guide: [`documentation/18-topologies.md`](../../documentation/18-topologies.md)
