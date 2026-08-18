# @perfect/postgres

PostgreSQL adapters for Perfect: PGMQ and `SKIP LOCKED` queues, change streams,
coordination primitives, and durable topology state.

```bash
bun add @perfect/postgres drizzle-orm postgres
```

> Not yet published to npm — install from the workspace for now.

## Atomic topology delivery

`PgPartitionedStateBackend` stores state by topology, stage, and partition. It
uses expiring owner leases and monotonically increasing fencing epochs, and
commits state mutations, source progress, checkpoints, and source-record
deduplication under a row lock.

`processedRetentionMs` can bound the deduplication table. Keep it at least as
long as the source system can replay a record; a shorter window weakens
duplicate suppression after old records are redelivered.

`PgmqQueue` exposes transactional source envelopes and sinks. When the input
queue, output queue, and `PgPartitionedStateBackend` use the same Drizzle
database object, `@perfect/topology` accepts `deliveryGuarantee:
"exactly-once"` and performs output publication, state/progress persistence,
and source delete/archive in one PostgreSQL transaction.

```ts
import { ConsumerGroup, StreamTopology, TopologyRunner } from "@perfect/topology";
import { PgPartitionedStateBackend } from "@perfect/postgres";
import { PgmqQueue } from "@perfect/postgres/pgmq";

const state = new PgPartitionedStateBackend({ db });
await state.ensureTables();

const input = await PgmqQueue.create<{ n: number }>(db, "input");
const output = await PgmqQueue.create<{ n: number }>(db, "output");
const topology = StreamTopology.source(input)
  .map(({ n }) => ({ n: n * 2 }))
  .to(output);

const handle = await TopologyRunner.run(topology, {
  group: ConsumerGroup("doubler"),
  partitionedStateBackend: state,
  deliveryGuarantee: "exactly-once",
});
```

For Kafka, Redis Streams, or mixed connector domains, use the default
`"at-least-once"` mode and make the sink idempotent. Perfect rejects
`"exactly-once"` when the source, sink, and state backend cannot prove that
they share one transaction domain.

## Other adapters

- `PgQueue` — `SKIP LOCKED` work queue
- `PgChangeStream` — LISTEN/NOTIFY with polling replay fallback
- `PgRateLimiter`, `PgThrottle`, `PgSingleflight`, `PgRef`
- `PgLeaderElection` — session advisory-lock leadership
- `PgStateBackend` / `PgPartitionedStateBackend` — durable stream/topology state

See [Redis and PostgreSQL backends](../../documentation/17-distributed-backends.md)
and [Stateful topologies](../../documentation/18-topologies.md).
