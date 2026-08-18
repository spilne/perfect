# @perfect/topology

Stateful stream processing on top of `@perfect/core`. Declare a processing
DAG — keyed state, time windows, stream joins, deduplication, checkpointing —
and run it over any source or sink that implements the
`@perfect/core/connect` contracts (`Streamable`, `Sinkable`,
`Acknowledgeable`, …). A `KafkaTopic` from `@perfect/kafka` plugs in
directly; so does an in-memory test double.

## Install

```bash
bun add @perfect/topology
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

```ts
import { ConsumerGroup, StreamTopology, TopologyRunner } from "@perfect/topology";

// clicks / counts: configured KafkaTopic, RedisStream, PgmqQueue, or any
// application endpoint implementing the connect contracts.
const topology = StreamTopology.source(clicks)
  .keyBy((e) => e.userId)
  .tumbling(60_000)
  .aggregate({
    init: () => ({ count: 0 }),
    add: (state) => ({ count: state.count + 1 }),
    emit: (key, window, state) => ({ key, window, count: state.count }),
  })
  .to(counts);

const handle = await TopologyRunner.run(topology, {
  group: ConsumerGroup("analytics"),
});

console.log(handle.metrics().itemsProcessed);
const exits = await handle.awaitExit();
await handle.shutdown();
```

Durable identities use distinct constructors (`TopologyId`, `StageId`,
`TopologyInstanceId`, `SourceRecordId`, and `StateCheckpointId`) so values
cannot be accidentally swapped across state and connector APIs. Connector
offsets remain plain strings because their representation is backend-specific.

Stateless steps chain like a stream; keyed steps unlock windows and state:

```ts
StreamTopology.source(readings)
  .filter((r) => r.temp != null)
  .mapAsync(5, enrich) // concurrency-bounded async map
  .keyBy((r) => r.sensorId)
  .process({
    // per-key state machine
    init: () => ({ avg: 0 }),
    process: (state, r) => {
      const avg = state.avg * 0.7 + r.temp * 0.3;
      return {
        state: { avg },
        emit: { sensorId: r.sensorId, movingAvg: avg },
      };
    },
  })
  .to(sink);
```

## Features

- **Builder** — `StreamTopology.source(...)` with `map` / `filter` /
  `mapAsync`, `keyBy` → `KeyedTopology`, `.to(sink)` or `.build()`
- **Windows** — `tumbling`, `sliding`, `session`, with `aggregate` and the
  `count()` / `sum()` shorthands; `WindowManager` underneath
- **Joins** — windowed key joins between two keyed topologies (`JoinBuffer`)
- **Dedup** — `.dedupe(keyFn)` per key
- **Execution** — `TopologyRunner.run(topology, { group })` →
  `TopologyHandle` with `shutdown()`, `awaitExit()`, `isRunning()`, and `metrics()`
  (throughput, buffer fill, backpressure stats)
- **Distribution** — `DistributedRunner` + `planStages` split the DAG at
  explicit `shuffle()` boundaries into stages connected by a `ShuffleTransport`
  (Kafka-backed one in `@perfect/kafka`)
- **Partition state** — state is namespaced by topology, stage, operator, and
  partition; fenced lease epochs prevent stale instances from committing.
  Redis and PostgreSQL provide durable atomic state/progress/dedupe commits
- **Rebalances** — assignment restores state before delivery; revocation drains
  in-flight records, checkpoints, and releases the partition lease
- **Delivery** — `at-least-once` publishes before committing state/progress and
  acknowledging the source. `exactly-once` is accepted only when source, sink,
  and state advertise the same transaction domain. PGMQ plus
  `PgPartitionedStateBackend` is the first fully atomic implementation
- **Supervision** — sink, acknowledgement, checkpoint, and branch failures are
  observable through `awaitExit()` and interrupt sibling branches
- **Analysis** — `analyzeTopology` returns `TopologyWarning`s for suspect
  DAGs before you run them

## Links

- Repo: https://github.com/spilne/perfect
- Connect contracts: `@perfect/core/connect`
- Kafka source/sink: `@perfect/kafka`
- Stateful topology guide: [`documentation/18-topologies.md`](../../documentation/18-topologies.md)
- Messaging contracts: [`documentation/16-messaging.md`](../../documentation/16-messaging.md)
- Redis/PostgreSQL state backends: [`documentation/17-distributed-backends.md`](../../documentation/17-distributed-backends.md)
