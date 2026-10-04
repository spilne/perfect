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

`keyBy` is a logical key. Keyed state is kept **per source partition**: with
`TopologyRunner`, a key that appears in two partitions of the source has two
separate counts or windows. Make sure the source is partitioned by the same
key (for Kafka, produce with that key), or use `DistributedRunner` with
`shuffle()` so a `ShuffleTransport` routes equal keys to the same partition
before stateful operators. (`TopologyRunner` ignores `shuffle()`.)

## Event time and when windows close

Windows and joins use each record's **event time**: when it happened, not
when it was processed. Say where it comes from with `eventTime`:

```ts
StreamTopology.source(clicks)
  .eventTime((click) => Date.parse(click.occurredAt))
  .keyBy((click) => click.userId)
  .tumbling(60_000)
  .count();
```

The time stays with the record, so a later `map` can drop the field. A
function that returns something other than a finite number fails the
topology. With `DistributedRunner`, call `eventTime` after `shuffle()`: only
the value travels through the repartition channel.

Without `eventTime`, the time is read from the value: the first of `ts`,
`timestamp` or `eventTime` that is a number, or `createdAt` as an ISO date
string, and otherwise the current time.

A key's windows close when **that key** gets a record whose time is past the
window's end:

- A key that stops receiving records keeps its last windows open (and in
  state) until it gets another record.
- Windows still open when a finite source ends are not emitted.
- A record older than windows that already closed opens them again, so they
  can be emitted twice. Sources should deliver records roughly in time order
  per key.
- A session window gets one session per key; a record arriving after a gap
  longer than `gapMs` closes the old session and starts a new one.

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

Each stage after a shuffle groups by the key set before that shuffle, so
windows, `aggregate`, `process` and `dedupe` work there as usual. To re-key,
end a stage with `process` (or a window), then `keyBy(...).shuffle()` again.
`DistributedRunner` takes the same options as `TopologyRunner`, plus
`shuffleTransport`.

A `join` can't be split into stages yet: `DistributedRunner` refuses a
topology that has both `shuffle()` and `join`. Run such a topology with
`TopologyRunner`.

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

The dedupe information (which source records were already processed) is kept
forever by default. Set `processedRetentionMs` to forget records older than
that: on `TopologyRunner.run` when it keeps state itself (the default
in-memory state or a plain `stateBackend`), or on the partitioned state
backend (`InMemoryPartitionedState`, `PgPartitionedStateBackend`,
`RedisPartitionedStateBackend`). Redeliveries older than the retention are no
longer detected as duplicates.

`"exactly-once"` is accepted only when source envelope, sinks, and partitioned
state backend advertise the same transaction domain. Today the complete
atomic path is PGMQ source + PGMQ sink + `PgPartitionedStateBackend` sharing
one Drizzle database object. Backend and sink compatibility is checked during
startup; source-envelope compatibility is checked when committing a record.
Unsupported combinations fail rather than silently weakening the guarantee.

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
