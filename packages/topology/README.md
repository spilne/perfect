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
import { StreamTopology, TopologyRunner } from "@perfect/topology";

// clicks / counts: anything implementing the connect contracts —
// e.g. new KafkaTopic({ kafka, topic: "clicks", groupId: "analytics" })
const topology = StreamTopology.source(clicks)
  .keyBy((e) => e.userId)
  .tumbling(60_000)
  .aggregate({
    init: () => ({ count: 0 }),
    add: (state) => ({ count: state.count + 1 }),
    emit: (key, window, state) => ({ key, window, count: state.count }),
  })
  .to(counts);

const handle = await TopologyRunner.run(topology, { group: "analytics" });

console.log(handle.metrics().itemsProcessed);
await handle.shutdown();
```

Stateless steps chain like a stream; keyed steps unlock windows and state:

```ts
StreamTopology.source(readings)
  .filter((r) => r.temp != null)
  .mapAsync(5, enrich) // concurrency-bounded async map
  .keyBy((r) => r.sensorId)
  .process({
    // per-key state machine
    init: () => ({ avg: 0 }),
    process: (state, r) => ({
      state: { avg: state.avg * 0.7 + r.temp * 0.3 },
      emit: { sensorId: r.sensorId, movingAvg: state.avg },
    }),
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
  `TopologyHandle` with `shutdown()`, `isRunning()`, and `metrics()`
  (throughput, buffer fill, backpressure stats)
- **Distribution** — `DistributedRunner` + `planStages` split the DAG at
  `keyBy` boundaries into stages connected by a `ShuffleTransport`
  (Kafka-backed one in `@perfect/kafka`)
- **State** — pluggable `StateBackend` (`InMemoryState` included) for keyed
  state and checkpoints
- **Analysis** — `analyzeTopology` returns `TopologyWarning`s for suspect
  DAGs before you run them

## Links

- Repo: https://github.com/spilne/perfect
- Connect contracts: `@perfect/core/connect`
- Kafka source/sink: `@perfect/kafka`
- Streams chapter of the guide: [`documentation/09-streams.md`](../../documentation/09-streams.md)
