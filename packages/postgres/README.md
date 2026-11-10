# @spilne/perfect-postgres

PostgreSQL adapters for Perfect: PGMQ and `SKIP LOCKED` queues, change streams,
coordination primitives, and durable topology state.

```bash
bun add @spilne/perfect-postgres drizzle-orm postgres
```

## Transactions

`PgTransactor` runs effects inside one Postgres transaction. Store calls that
must run inside it say so in their type with `Tx<"pg">`, so forgetting
`transact` does not compile. Everything inside commits together; a typed
failure, a defect or an interrupt rolls it all back.

```ts
import { fromPromise } from "@spilne/perfect-core";
import { PgTransactor } from "@spilne/perfect-postgres";
import { PgmqQueue } from "@spilne/perfect-postgres/pgmq";

const pg = PgTransactor.make({ db });
const events = await PgmqQueue.create<{ paymentId: string }>(db, "payment_events");

const markPaid = (paymentId: string) =>
  pg.transact(function* () {
    yield* pg.withHandle(({ db }) =>
      fromPromise(
        () => db.update(payments).set({ status: "paid" }).where(eq(payments.id, paymentId)),
        (cause) => new PaymentWriteFailed({ cause }),
      ),
    );
    yield* events.publishIn(pg, { paymentId }); // sent only if the transaction commits
  });
// markPaid: (paymentId) => Eff<void, Commits<"pg"> | Throws<PaymentWriteFailed | PgmqQueueError | PostgresError>>
```

What to know:

- **Only `Tx<K>` work is all-or-nothing.** An HTTP call, a log line or a
  write to another system inside `transact` still happens if the transaction
  rolls back. Call external services before or after it.
- **One resource per transaction.** Nothing commits Postgres and something
  else together, so using another resource's transaction inside this one is a
  compile error:
  `"Cannot use another resource's transaction inside this one": "redis"`. It
  is also refused at run time (`MixedTransactionDefect`) if a cast hides it.
  A second database gets its own kind: `PgTransactor.make({ db: billingDb, kind: "billing" })`.
- **`Commits<"pg">` stays in the type** after `transact`. Written-out return
  types must include it; `run()` ignores it.
- **Nesting the same transactor joins the outer transaction.** There are no
  savepoints: an inner failure that is caught inside the outer transaction
  does not undo the inner writes.
- **Join forks before the transaction ends.** A fiber that uses the
  transaction after it committed dies with `TransactionClosedDefect`.
- **Waiting for a connection is not interruptible.** `transact` waits for a
  pool connection before it can begin, and that wait finishes even if a
  timeout fires, so size the pool for your concurrency. Once the transaction
  has begun, interrupts roll it back as usual.
- **An interrupt during commit still commits.** The commit runs to the end,
  but the caller sees an interrupt. Don't treat an interrupted `transact` as
  "not committed"; make the work safe to retry instead (for example with an
  event id).
- **Store calls on one transaction run one at a time.** A transaction is one
  connection, so running them in parallel (`all`, `race`, `forEachPar`) dies
  with `TransactionBusyDefect`. Parallel work that doesn't touch the
  transaction is fine.

The `TransactionalSinkable` / `publishInTransaction` contracts used by
topology exactly-once delivery below are the older, Promise-based way to share
a transaction. They will move onto `Tx<K>`; new application code should use
`PgTransactor`.

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
database object, `@spilne/perfect-topology` accepts `deliveryGuarantee:
"exactly-once"` and performs output publication, state/progress persistence,
and source delete/archive in one PostgreSQL transaction.

```ts
import { ConsumerGroup, StreamTopology, TopologyRunner } from "@spilne/perfect-topology";
import { PgPartitionedStateBackend } from "@spilne/perfect-postgres";
import { PgmqQueue } from "@spilne/perfect-postgres/pgmq";

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
