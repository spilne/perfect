// Integration tests — real Postgres via testcontainers.
//
// Gated on a runtime docker check: without a running Docker daemon every
// describe here fast-skips. Two containers:
//   - postgres:17-alpine        — everything except pgmq
//   - ghcr.io/pgmq/pg17-pgmq    — pgmq extension tests

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import { drizzle } from "drizzle-orm/postgres-js";
import { sql } from "drizzle-orm";
import postgres from "postgres";
import { run } from "@spilne/perfect-core";
import {
  CheckpointName,
  Partition,
  SourceRecordId,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
} from "@spilne/perfect-core/connect";
import type { DrizzleDb } from "../src/lib/drizzle-db";
import { PgQueue } from "../src/lib/pg-queue";
import { PgChangeStream } from "../src/lib/pg-change-stream";
import { PgLeaderElection } from "../src/lib/pg-leader-election";
import { PgStateBackend } from "../src/lib/pg-state-backend";
import { PgPartitionedStateBackend } from "../src/lib/pg-partitioned-state-backend";
import { PgRateLimiter } from "../src/lib/pg-rate-limiter";
import { PgRef } from "../src/lib/pg-ref";
import { PgSingleflight } from "../src/lib/pg-singleflight";
import { PgmqQueue } from "../src/pgmq/pgmq-queue";
import * as pgmq from "../src/pgmq/pgmq";

const dockerAvailable = (() => {
  try {
    return Bun.spawnSync(["docker", "info"], { stdout: "ignore", stderr: "ignore" }).exitCode === 0;
  } catch {
    return false;
  }
})();

function startPostgres(image: string): Promise<StartedTestContainer> {
  return new GenericContainer(image)
    .withExposedPorts(5432)
    .withEnvironment({ POSTGRES_USER: "test", POSTGRES_PASSWORD: "test", POSTGRES_DB: "test" })
    .withCommand(["postgres", "-c", "fsync=off", "-c", "synchronous_commit=off"])
    .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
    .withStartupTimeout(120_000)
    .start();
}

function connString(container: StartedTestContainer): string {
  return `postgres://test:test@${container.getHost()}:${container.getMappedPort(5432)}/test`;
}

// ---------------------------------------------------------------------------
// Plain Postgres — queue, CDC, coordination, state
// ---------------------------------------------------------------------------

describe.skipIf(!dockerAvailable)("integration — postgres:17-alpine", () => {
  let container: StartedTestContainer;
  let sqlClient: ReturnType<typeof postgres>;
  let db: DrizzleDb;

  beforeAll(async () => {
    container = await startPostgres("postgres:17-alpine");
    sqlClient = postgres(connString(container));
    db = drizzle(sqlClient);
  }, 180_000);

  afterAll(async () => {
    await sqlClient?.end();
    await container?.stop();
  });

  it("pg-queue: SKIP LOCKED claim → ack empties the queue", async () => {
    const queue = await PgQueue.create<{ userId: string }>(db, "jobs", { pollIntervalMs: 50 });

    await queue.publish({ userId: "u_1" });
    await queue.publish({ userId: "u_2" });

    const envelopes = await run(queue.subscribeAck().take(2).toArray().orDie());
    expect(envelopes.map((e) => e.value)).toEqual([{ userId: "u_1" }, { userId: "u_2" }]);

    for (const e of envelopes) await e.ack();

    const m = await queue.metrics();
    expect(m.pending).toBe(0);
    expect(m.total).toBe(0);
  }, 20_000);

  it("pg-queue: claimed messages are invisible to a second consumer", async () => {
    const queue = await PgQueue.create<string>(db, "claims", { pollIntervalMs: 50 });
    await queue.publish("only-once");

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    expect(envelope!.value).toBe("only-once");

    // Message is now 'processing' with a visibility timeout — nothing to claim
    const m = await queue.metrics();
    expect(m.pending).toBe(0);
    expect(m.processing).toBe(1);

    await envelope!.ack();
  }, 20_000);

  it("pg-queue: subscribe pops messages", async () => {
    const queue = await PgQueue.create<{ n: number }>(db, "pops", { pollIntervalMs: 50 });
    await queue.publish({ n: 1 });

    const items = await run(queue.subscribe().take(1).toArray().orDie());
    expect(items).toEqual([{ n: 1 }]);
    expect((await queue.metrics()).total).toBe(0);
  }, 20_000);

  it("pg-queue: a message whose consumer died is delivered again after its timeout", async () => {
    const queue = await PgQueue.create<string>(db, "redelivery", { pollIntervalMs: 20 });
    await queue.publish("retry-me");

    // First consumer claims the message for 1 second and never acks.
    const [first] = await run(queue.subscribeAck({ vtSeconds: 1 }).take(1).toArray().orDie());
    expect(first!.metadata.attemptCount).toBe(1);

    // Once the timeout runs out, a second consumer gets it.
    const [second] = await run(queue.subscribeAck({ vtSeconds: 30 }).take(1).toArray().orDie());
    expect(second!.value).toBe("retry-me");
    expect(second!.metadata.attemptCount).toBe(2);

    // The first consumer's late ack must not delete the second delivery.
    await first!.ack();
    expect((await queue.metrics()).processing).toBe(1);

    await second!.ack();
    expect((await queue.metrics()).total).toBe(0);
  }, 20_000);

  it("pg-queue: a message is marked dead after maxAttempts, and requeueDead brings it back", async () => {
    const queue = await PgQueue.create<string>(db, "dead-letters", {
      pollIntervalMs: 20,
      maxAttempts: 2,
    });
    await queue.publish("poison");

    for (let attempt = 1; attempt <= 2; attempt++) {
      const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
      expect(envelope!.metadata.attemptCount).toBe(attempt);
      await envelope!.nack();
    }

    let m = await queue.metrics();
    expect(m.dead).toBe(1);
    expect(m.pending).toBe(0);

    expect(await queue.requeueDead()).toBe(1);
    m = await queue.metrics();
    expect(m.dead).toBe(0);
    expect(m.pending).toBe(1);
  }, 20_000);

  it("pg-queue: works with a queue name that has capitals and dashes", async () => {
    const queue = await PgQueue.create<{ n: number }>(db, "Order-Events", { pollIntervalMs: 20 });
    await queue.publish({ n: 1 }, { headers: { "it's": "quoted" } });
    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    expect(envelope!.value).toEqual({ n: 1 });
    expect(envelope!.metadata.headers).toEqual({ "it's": "quoted" });
    await envelope!.ack();
    await queue.drop();
  }, 20_000);

  it("change-stream: NOTIFY round-trip reaches a live subscriber", async () => {
    await db.execute(
      sql.raw(`
      CREATE TABLE IF NOT EXISTS events (
        id BIGSERIAL PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `),
    );

    const stream = new PgChangeStream<{ type: string }>({
      db,
      sql: sqlClient,
      channel: "evt_channel",
      table: "events",
      payloadColumn: "payload",
      pollIntervalMs: 60_000, // poll fallback out of the picture — LISTEN only
    });

    const received = run(stream.subscribe().take(1).toArray().orDie());
    // Give LISTEN a moment to register before notifying
    await new Promise((r) => setTimeout(r, 500));
    await stream.notify({ type: "hello" });

    expect(await received).toEqual([{ type: "hello" }]);
  }, 20_000);

  it("change-stream: rows that share a timestamp are not skipped between polls", async () => {
    await db.execute(
      sql.raw(`
      CREATE TABLE same_time_events (
        id BIGSERIAL PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      );
      INSERT INTO same_time_events (payload, created_at)
      SELECT jsonb_build_object('n', n), '2026-01-01T00:00:00.123456Z'
      FROM generate_series(1, 5) AS n;
    `),
    );
    const stream = new PgChangeStream<{ n: number }>({
      db,
      sql: sqlClient,
      channel: "same_time_events",
      table: "same_time_events",
      payloadColumn: "payload",
      pollIntervalMs: 20,
      pollBatchSize: 2,
    });

    const values = await run(
      stream
        .subscribeFrom({ offset: { type: "earliest" } })
        .take(5)
        .toArray()
        .orDie(),
    );
    expect(values.map((v) => v.n)).toEqual([1, 2, 3, 4, 5]);
  }, 20_000);

  it("change-stream: poll fallback replays rows from an offset", async () => {
    await db.execute(
      sql.raw(`
      CREATE TABLE IF NOT EXISTS poll_events (
        id BIGSERIAL PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `),
    );
    await db.execute(sql.raw(`INSERT INTO poll_events (payload) VALUES ('{"n":1}'), ('{"n":2}')`));

    const stream = new PgChangeStream<{ n: number }>({
      db,
      sql: sqlClient,
      channel: "poll_channel",
      table: "poll_events",
      payloadColumn: "payload",
      pollIntervalMs: 100,
    });

    const items = await run(
      stream
        .subscribeFrom({ offset: { type: "earliest" } })
        .take(2)
        .toArray()
        .orDie(),
    );
    expect(items).toEqual([{ n: 1 }, { n: 2 }]);
  }, 20_000);

  it("leader election: only one session holds the lock; release hands it over", async () => {
    // Advisory locks are session-scoped — use two single-connection clients
    const a = postgres(connString(container), { max: 1 });
    const b = postgres(connString(container), { max: 1 });
    try {
      const leaderA = new PgLeaderElection({ db: drizzle(a), lockId: 7777 });
      const leaderB = new PgLeaderElection({ db: drizzle(b), lockId: 7777 });

      expect(await leaderA.tryAcquire()).toBe(true);
      expect(await leaderB.tryAcquire()).toBe(false);

      await leaderA.release();
      expect(await leaderB.tryAcquire()).toBe(true);
      await leaderB.release();
    } finally {
      await a.end();
      await b.end();
    }
  }, 20_000);

  it("state-backend: checkpoint / restore round-trip", async () => {
    const backend = new PgStateBackend({ db });
    await backend.ensureTable();
    await backend.clear();

    await backend.put("a", { count: 1 });
    await backend.put("b", "two");
    await backend.checkpoint({ name: CheckpointName("cp1") });

    await backend.put("a", { count: 99 });
    await backend.delete("b");
    expect(await backend.get("a")).toEqual({ count: 99 });
    expect(await backend.get("b")).toBeUndefined();

    await backend.restore({ name: CheckpointName("cp1") });
    expect(await backend.get("a")).toEqual({ count: 1 });
    expect(await backend.get("b")).toBe("two");
    expect(await backend.keys()).toEqual(["a", "b"]);
  }, 20_000);

  it("partitioned state: atomically fences owners and commits progress", async () => {
    const backend = new PgPartitionedStateBackend({ db, table: "integration_partition_state" });
    await backend.ensureTables();
    const scope = {
      topologyId: TopologyId("orders"),
      stageId: StageId("aggregate"),
      partition: Partition(0),
    };
    const first = await backend.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-a"),
      leaseMs: 30_000,
    });
    expect(first).toBeDefined();
    expect(
      await backend.acquire({
        scope,
        ownerId: TopologyInstanceId("worker-b"),
        leaseMs: 30_000,
      }),
    ).toBeUndefined();

    expect(
      await backend.commit({
        lease: first!,
        mutations: [{ type: "put", key: "count", value: 42 }],
        sourceId: SourceRecordId("orders:0:7"),
        sourceOffset: "7",
        checkpointId: StateCheckpointId("cp-7"),
      }),
    ).toBe("committed");
    expect(
      await backend.commit({
        lease: first!,
        mutations: [],
        sourceId: SourceRecordId("orders:0:7"),
      }),
    ).toBe("duplicate");
    expect((await backend.load(first!))?.values.get("count")).toBe(42);

    expect(await backend.release(first!)).toBe(true);
    const second = await backend.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-b"),
      leaseMs: 30_000,
    });
    expect(second?.epoch).toBe(first!.epoch + 1);
    expect(await backend.commit({ lease: first!, mutations: [] })).toBe("fenced");
    await backend.release(second!);
  }, 20_000);

  it("rate limiter: grants up to the limit, then fails typed", async () => {
    const rl = await PgRateLimiter.create({ db, key: "api", limit: 2, windowMs: 60_000 });

    expect(await run(rl.tryAcquire)).toBe(true);
    expect(await run(rl.tryAcquire)).toBe(true);

    const third = await run(rl.acquire.either());
    expect(third._tag).toBe("Left");
    if (third._tag === "Left") {
      expect(third.left._tag).toBe("RateLimitExceeded");
      expect(third.left.retryAfterMs).toBeGreaterThan(0);
    }
    expect(await run(rl.remaining)).toBe(0);
  }, 20_000);

  it("rate limiter: many callers at once never get more than the limit", async () => {
    const rl = await PgRateLimiter.create({ db, key: "burst", limit: 5, windowMs: 60_000 });

    const results = await Promise.all(Array.from({ length: 30 }, () => run(rl.tryAcquire)));

    expect(results.filter(Boolean)).toHaveLength(5);
  }, 20_000);

  it("pg-ref: transactional modify is atomic across concurrent updates", async () => {
    const ref = await PgRef.make<number>({ db, name: "counter", initial: 0 });

    await Promise.all(Array.from({ length: 10 }, () => run(ref.update((n) => n + 1))));
    expect(await run(ref.get)).toBe(10);

    const previous = await run(ref.getAndSet(100));
    expect(previous).toBe(10);
    expect(await run(ref.get)).toBe(100);
  }, 20_000);

  it("singleflight: concurrent callers share one execution", async () => {
    const sf = await PgSingleflight.make({ db, pollIntervalMs: 10 });
    let executions = 0;

    const work = async () => {
      executions += 1;
      await new Promise((r) => setTimeout(r, 50));
      return "result";
    };

    const [r1, r2, r3] = await Promise.all([
      sf.doAsync("job", work),
      sf.doAsync("job", work),
      sf.doAsync("job", work),
    ]);

    expect([r1, r2, r3]).toEqual(["result", "result", "result"]);
    expect(executions).toBe(1);
  }, 20_000);
});

// ---------------------------------------------------------------------------
// pgmq extension — requires the pgmq image
// ---------------------------------------------------------------------------

describe.skipIf(!dockerAvailable)("integration — pgmq (ghcr.io/pgmq/pg17-pgmq)", () => {
  let container: StartedTestContainer;
  let sqlClient: ReturnType<typeof postgres>;
  let db: DrizzleDb;

  beforeAll(async () => {
    container = await startPostgres("ghcr.io/pgmq/pg17-pgmq:latest");
    sqlClient = postgres(connString(container));
    db = drizzle(sqlClient);
    await sqlClient`CREATE EXTENSION IF NOT EXISTS pgmq`;
  }, 300_000);

  afterAll(async () => {
    await sqlClient?.end();
    await container?.stop();
  });

  it("send / read / ack (delete) drains the queue", async () => {
    const queue = await PgmqQueue.create<{ job: string }>(db, "work", {
      defaultPollIntervalMs: 50,
    });

    await queue.publish({ job: "a" });
    await queue.publish({ job: "b" });

    const envelopes = await run(queue.subscribeAck().take(2).toArray().orDie());
    expect(envelopes.map((e) => e.value)).toEqual([{ job: "a" }, { job: "b" }]);
    expect(envelopes[0]!.metadata.msgId).toBeGreaterThan(0);

    for (const e of envelopes) await e.ack();

    const m = await queue.metrics();
    expect(m.queueLength).toBe(0);
  }, 60_000);

  it("sendBatch sends a whole batch in one call, with and without headers", async () => {
    await pgmq.createQueue(db, "batched");
    const ids = await pgmq.sendBatch(db, "batched", [
      { data: { n: 1 } },
      { data: { n: 2 }, headers: { "x-pgmq-group": "g" } },
      { data: { n: 3 } },
    ]);
    expect(ids).toHaveLength(3);
    expect(new Set(ids).size).toBe(3);

    const records = await pgmq.read<{ n: number }>(db, "batched", {
      _tag: "standard",
      vt: 30,
      qty: 10,
    });
    expect(records.map((r) => r.message.n)).toEqual([1, 2, 3]);
    expect(records[1]!.headers).toEqual({ "x-pgmq-group": "g" });

    const plain = await pgmq.sendBatch(db, "batched", [{ data: { n: 4 } }, { data: { n: 5 } }]);
    expect(plain).toHaveLength(2);
  }, 60_000);

  it("publishBatch + subscribe skips messages that fail the schema", async () => {
    const queue = await PgmqQueue.create<{ n: number }>(db, "validated", {
      defaultPollIntervalMs: 20,
      schema: {
        safeParse: (data: unknown) =>
          typeof (data as { n?: unknown })?.n === "number"
            ? { success: true as const, data: data as { n: number } }
            : { success: false as const, error: "no n" },
      },
      onSchemaError: "skip",
    });
    await pgmq.sendBatch(db, "validated", [
      { data: { n: 1 } },
      { data: { bad: true } },
      { data: { n: 3 } },
    ]);

    const values = await run(queue.subscribe().take(2).toArray().orDie());
    expect(values).toEqual([{ n: 1 }, { n: 3 }]);
  }, 60_000);

  it("low-level send/read/deleteMessage round-trip", async () => {
    await pgmq.createQueue(db, "lowlevel");
    const msgId = await pgmq.send(db, "lowlevel", { data: { n: 42 } });
    expect(msgId).toBeGreaterThan(0);

    const records = await pgmq.read<{ n: number }>(db, "lowlevel", {
      _tag: "standard",
      vt: 30,
      qty: 10,
    });
    expect(records).toHaveLength(1);
    expect(records[0]!.message).toEqual({ n: 42 });
    expect(records[0]!.msgId).toBe(msgId);

    expect(await pgmq.deleteMessage(db, "lowlevel", msgId)).toBe(true);
    expect(await pgmq.read(db, "lowlevel", { _tag: "standard", vt: 30, qty: 10 })).toHaveLength(0);
  }, 60_000);

  it("PGMQ publish, partition progress, and source ack share one transaction", async () => {
    const input = await PgmqQueue.create<{ value: number }>(db, "atomic_input", {
      defaultPollIntervalMs: 50,
    });
    const output = await PgmqQueue.create<{ value: number }>(db, "atomic_output", {
      defaultPollIntervalMs: 50,
    });
    const state = new PgPartitionedStateBackend({ db, table: "atomic_topology_state" });
    await state.ensureTables();
    const scope = {
      topologyId: TopologyId("atomic"),
      stageId: StageId("stage-0"),
      partition: Partition(0),
    };
    const lease = await state.acquire({
      scope,
      ownerId: TopologyInstanceId("worker"),
      leaseMs: 30_000,
    });
    await input.publish({ value: 21 });
    const [envelope] = await run(input.subscribeAck().take(1).toArray().orDie());

    await state.transaction(async (transaction) => {
      await output.publishInTransaction(transaction, { value: envelope!.value.value * 2 });
      expect(
        await state.commitInTransaction(transaction, {
          lease: lease!,
          mutations: [{ type: "put", key: "last", value: envelope!.value.value }],
          sourceId: SourceRecordId(`atomic:0:${String(envelope!.metadata.offset)}`),
          sourceOffset: String(envelope!.metadata.offset),
        }),
      ).toBe("committed");
      await envelope!.ackInTransaction(transaction);
    });

    expect((await input.metrics()).queueLength).toBe(0);
    expect((await state.load(lease!))?.values.get("last")).toBe(21);
    expect(await run(output.subscribe().take(1).toArray().orDie())).toEqual([{ value: 42 }]);
    await state.release(lease!);
  }, 60_000);

  it("nack (setVt) makes the message re-readable", async () => {
    const queue = await PgmqQueue.create<string>(db, "nacks", { defaultPollIntervalMs: 50 });
    await queue.publish("retry-me");

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    await envelope!.nack(); // vt = 1s

    await new Promise((r) => setTimeout(r, 1_500));
    const [again] = await run(queue.subscribeAck().take(1).toArray().orDie());
    expect(again!.value).toBe("retry-me");
    expect(again!.metadata.readCt as number).toBeGreaterThanOrEqual(2);
    await again!.ack();
  }, 60_000);

  it("FIFO groups do not expose message 2 while message 1 is processing", async () => {
    type Job = { userId: string; sequence: number };
    const queue = await PgmqQueue.create<Job>(db, "ordered_users", {
      fifo: true,
      defaultQty: 10,
      defaultPollIntervalMs: 50,
    });
    await queue.publish({ userId: "x", sequence: 1 }, { group: "x" });
    await queue.publish({ userId: "x", sequence: 2 }, { group: "x" });
    await queue.publish({ userId: "y", sequence: 1 }, { group: "y" });

    const workerOne = PgmqQueue.wrap<Job>({
      db,
      queue: "ordered_users",
      fifo: true,
      defaultQty: 1,
      defaultPollIntervalMs: 50,
    });
    const workerTwo = PgmqQueue.wrap<Job>({
      db,
      queue: "ordered_users",
      fifo: true,
      defaultQty: 10,
      defaultPollIntervalMs: 50,
    });

    const [x1] = await run(workerOne.subscribeAck().take(1).toArray().orDie());
    expect(x1!.value).toEqual({ userId: "x", sequence: 1 });

    const [whileXIsBusy] = await run(workerTwo.subscribeAck().take(1).toArray().orDie());
    expect(whileXIsBusy!.value).toEqual({ userId: "y", sequence: 1 });
    await whileXIsBusy!.ack();

    await x1!.ack();
    const [x2] = await run(workerTwo.subscribeAck().take(1).toArray().orDie());
    expect(x2!.value).toEqual({ userId: "x", sequence: 2 });
    await x2!.ack();
  }, 60_000);
});
