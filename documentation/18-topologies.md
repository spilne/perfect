# Stateful Topologies

Use `Stream` for general pull-based transformations. Use
`@spilne/perfect-topology` for long-running event processing that needs keyed state,
windows, joins, deduplication, checkpoints, partition ownership, or distributed
shuffle stages.

## Build and run

A topology source must implement both `Streamable` and `Acknowledgeable`; its
sink implements `Sinkable`. Here both endpoints are Kafka topics, but a
`RedisStream`, `PgmqQueue`, or application-defined implementation of the same
contracts works without changing the topology:

```ts
import { kafkaConfig } from "@spilne/perfect-kafka";
import { createKafkajsClient } from "@spilne/perfect-kafka-kafkajs";
import { ConsumerGroup, StreamTopology, TopologyRunner } from "@spilne/perfect-topology";

interface Click {
  userId: string;
  bot: boolean;
}

interface ClickCount {
  key: string;
  window: { start: number; end: number };
  count: number;
}

const kafka = createKafkajsClient("localhost:9092");
const clicks = kafkaConfig<Click>()
  .client(kafka)
  .topic("clicks")
  .group("analytics-input")
  .build();
const counts = kafkaConfig<ClickCount>()
  .client(kafka)
  .topic("click-counts")
  .group("analytics-output")
  .build();

const topology = StreamTopology.source(clicks)
  .filter((event) => !event.bot)
  .keyBy((event) => event.userId)
  .tumbling(60_000)
  .count()
  .to(counts);

const handle = await TopologyRunner.run(topology, {
  group: ConsumerGroup("analytics"),
  maxBufferSize: 1_024,
  ackBatchSize: 100,
  ackMaxWaitMs: 1_000,
});

const shutdown = () => void handle.shutdown();
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
const exits = await handle.awaitExit();
await handle.shutdown();
```

`awaitExit()` exposes every branch exit, including typed sink,
acknowledgement, and checkpoint failures. A failing branch interrupts its
siblings. For an unbounded source it waits until shutdown or failure; the
second `shutdown()` above is an idempotent cleanup after `awaitExit()`.
The example assumes Kafka is running on `localhost:9092` and the two topics
exist (or broker-side topic auto-creation is enabled).

## Operators

| Stage | Operators |
| --- | --- |
| Stateless | `map`, `filter`, bounded `mapAsync` |
| Partition | `keyBy`, optional `shuffle` |
| Stateful | `process`, per-key `dedupe` |
| Windows | `tumbling`, `sliding`, `session` |
| Aggregation | `aggregate`, `count`, `sum` |
| Correlation | time-windowed keyed `join` |
| Terminal | `to(sink)` / `build()` |

`keyBy` is a logical key. In a multi-instance deployment, add `shuffle()` so
a `ShuffleTransport` physically routes equal keys to the same partition
before stateful operators.

## Stateful processing

```ts
const averages = StreamTopology.source(readings)
  .keyBy((reading) => reading.sensorId)
  .process({
    init: () => ({ average: 0 }),
    process: (state, reading) => {
      const average = state.average * 0.7 + reading.temperature * 0.3;
      return {
        state: { average },
        emit: { sensorId: reading.sensorId, average },
      };
    },
  })
  .to(output);
```

Durable state is namespaced by topology, stage, operator, source partition,
and key. Assignment restores a partition before delivery; revocation drains
in-flight work, checkpoints, and releases its fenced lease.

Use `RedisPartitionedStateBackend` or `PgPartitionedStateBackend` for
multi-instance state. A legacy unpartitioned `StateBackend` is suitable for a
single process but is rejected for stateful multi-stage distributed runs.

## Distributed stages

`DistributedRunner` plans a stage boundary at each `shuffle()` and connects
the stages through a `ShuffleTransport`. Kafka supplies
`KafkaShuffleTransport`. The topology passed here should include a
`keyBy(...).shuffle()` boundary before distributed stateful operators.

```ts
import { DistributedRunner } from "@spilne/perfect-topology";
import { KafkaShuffleTransport } from "@spilne/perfect-kafka";

const handle = await DistributedRunner.run(topology, {
  group: ConsumerGroup("analytics"),
  shuffleTransport: new KafkaShuffleTransport({ kafka }),
  partitionedStateBackend: state,
});
```

`planStages({ compiled: topology.compiled, group })` and
`analyzeTopology(topology.compiled)` are available for inspection. The
analyzer reports suspicious DAGs such as keyed state without a preceding
shuffle.

## Delivery guarantees

The default is `"at-least-once"`:

1. Process the source envelope.
2. Publish every sink output.
3. Persist state, source progress, checkpoint, and dedupe information.
4. Acknowledge the source.

A crash between these steps can replay an already-published output, so sinks
must be idempotent.

`"exactly-once"` is accepted only when source envelope, sinks, and partitioned
state backend advertise the same transaction domain. Today the complete
atomic path is PGMQ source + PGMQ sink + `PgPartitionedStateBackend` sharing
one Drizzle database object. Unsupported combinations fail at startup rather
than silently weakening the guarantee.

## Choosing Stream vs topology

| Need | Use |
| --- | --- |
| Finite transformation or local reactive pipeline | `Stream` |
| Local accumulator without recovery | `Stream.mapAccumulate` |
| Pluggable keyed state in one stream | `Stream.statefulMap` |
| Partition leases, fencing, restore, dedupe, checkpoints | `@spilne/perfect-topology` |
| Cross-stage repartitioning across instances | `DistributedRunner` |

## Next

- [Messaging contracts and Kafka](./16-messaging.md)
- [Redis and PostgreSQL backends](./17-distributed-backends.md)
