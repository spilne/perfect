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
bun add @perfect/kafka
```

> Not yet published to npm — install from the workspace for now.

**No driver is bundled.** You inject a client implementing the small
`KafkaClient` interface (`producer()` / `consumer()` / `admin()`) — kafkajs,
`@confluentinc/kafka-javascript` (an optional peer dependency, kafkajs-
compatible), and `@platformatic/kafka` all fit. Tests inject a fake, so no
broker is needed there either.

## Quickstart

```ts
import { fromPromise } from "@perfect/core";
import { KafkaTopic } from "@perfect/kafka";
import { KafkaJS } from "@confluentinc/kafka-javascript"; // or kafkajs, or your own

interface Order {
  id: string;
  amount: number;
}

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ["localhost:9092"] } });

const orders = new KafkaTopic<Order>({
  kafka,
  topic: "orders",
  groupId: "billing",
});

// produce — JSON codec by default, pass codec: to override
await orders.publish({ id: "o-1", amount: 42 }, { key: "o-1" });

// consume — decoded values as a core Stream; take() closes the consumer
const first = await orders.subscribe().take(1).toArray().run();

// at-least-once — Envelope<T> with ack/nack; acked offsets are committed
// contiguously in the background (commitIntervalMs, default 1s)
await orders
  .subscribeAck()
  .evalMap((env) =>
    fromPromise(
      () => handle(env.value).then(() => env.ack()),
      (e) => e,
    ),
  )
  .drain()
  .run();
```

For explicit fs2-kafka-style batched commits, pipe through
`commitBatchWithin({ maxBatchSize, maxWaitMs, consumer, topic })` — it batches
ack'd envelopes by count or time and commits offset groups via a shared
`OffsetTracker`, safe under `parEvalMap` reordering.

## Features

- `KafkaTopic<T>` — one class, all connect contracts:
  - `publish` / `publishBatch` (keyed, codec-encoded)
  - `subscribe` / `subscribeFrom` (offset or timestamp replay)
  - `subscribeAck` — `Stream<Envelope<T>>` with background contiguous commits
  - checkpoint support for `@perfect/topology`
- `commitBatchWithin` — batched offset-commit pipe (count or time window)
- `KafkaShuffleTransport` — Kafka-backed `ShuffleTransport` for distributed
  topology stages
- Driver-agnostic types — `KafkaClient`, `KafkaConsumer`, `KafkaProducer`,
  `KafkaAdmin`, `KafkaConsumerOptions` (session/poll/heartbeat tuning knobs
  to avoid the slow-handler → rebalance → redelivery loop)

## Links

- Repo: https://github.com/spilne/perfect
- Connect contracts: `@perfect/core/connect`
- Streams and the full guide: [`documentation/`](../../documentation/)
