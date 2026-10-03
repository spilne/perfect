import { describe, it, expect } from "bun:test";
import { run } from "@spilne/perfect-core";
import { PgQueue } from "../src/lib/pg-queue";
import { fakeDb } from "./fake-db";

const claimRow = (id: string, attemptCount = 1) => ({
  id,
  payload: { userId: "u_1" },
  attempt_count: attemptCount,
  created_at: new Date("2026-01-01T00:00:00Z"),
  headers: null,
});

describe("PgQueue (fake db)", () => {
  it("publish sends the payload as a parameter into the quoted table", async () => {
    const { db, fake } = fakeDb();
    const queue = PgQueue.wrap<{ userId: string }>({ db, queue: "jobs" });

    await queue.publish({ userId: "u_1" }, { delay: 5 });

    const insert = fake.queries[0]!;
    expect(insert.sql).toContain('INSERT INTO "pgq_jobs"');
    expect(insert.params).toContain('{"userId":"u_1"}');
    expect(insert.params).toContain(5);
  });

  it("a payload with quotes can't break out of the query", async () => {
    const { db, fake } = fakeDb();
    const queue = PgQueue.wrap<string>({ db, queue: "jobs" });

    await queue.publish("'); DROP TABLE users; --");

    expect(fake.allSql).not.toContain("DROP TABLE users");
  });

  it("a queue name with capitals or dashes is quoted", async () => {
    const { db, fake } = fakeDb();
    const queue = PgQueue.wrap<string>({ db, queue: "Order-Events" });

    await queue.publish("x");

    expect(fake.allSql).toContain('"pgq_Order-Events"');
  });

  it("subscribeAck claims with SKIP LOCKED, and ack only deletes its own delivery", async () => {
    let claimed = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("SKIP LOCKED") && sql.includes("UPDATE") && !claimed) {
        claimed = true;
        return [claimRow("7")];
      }
      return [];
    });
    const queue = PgQueue.wrap<{ userId: string }>({ db, queue: "jobs", pollIntervalMs: 5 });

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    expect(envelope!.value).toEqual({ userId: "u_1" });
    expect(envelope!.metadata.msgId).toBe(7);
    expect(envelope!.metadata.attemptCount).toBe(1);

    const claim = fake.queries.find((q) => q.sql.includes("SKIP LOCKED"))!;
    expect(claim.sql).toContain("FOR UPDATE SKIP LOCKED");
    // Visible messages with attempts left: pending ones, and processing ones
    // whose visibility timeout ran out (their consumer died).
    expect(claim.sql).toContain("status IN ('pending', 'processing')");
    expect(claim.sql).toContain("SET status = 'dead'");
    expect(claim.params).toContain(30); // visibility timeout
    expect(claim.params).toContain(3); // max attempts
    const lockToken = claim.params.find((p) => typeof p === "string" && p.length === 36);

    await envelope!.ack();
    const ack = fake.queries.at(-1)!;
    expect(ack.sql).toContain('DELETE FROM "pgq_jobs"');
    expect(ack.sql).toContain("locked_by =");
    expect(ack.params).toEqual([7, lockToken]);
  });

  it("nack makes the message visible again, or dead when out of attempts", async () => {
    let claimed = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("SKIP LOCKED") && sql.includes("UPDATE") && !claimed) {
        claimed = true;
        return [claimRow("3")];
      }
      return [];
    });
    const queue = PgQueue.wrap<unknown>({ db, queue: "jobs", pollIntervalMs: 5 });

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    await envelope!.nack();

    const nack = fake.queries.at(-1)!;
    expect(nack.sql).toContain("THEN 'dead' ELSE 'pending' END");
    expect(nack.sql).toContain("visible_at = NOW()");
    expect(nack.params[1]).toBe(3);
  });

  it("subscribe pops (read + delete) and decodes payloads", async () => {
    let popped = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes('DELETE FROM "pgq_jobs"') && sql.includes("SKIP LOCKED") && !popped) {
        popped = true;
        return [
          { id: "1", payload: { n: 1 } },
          { id: "2", payload: { n: 2 } },
        ];
      }
      return [];
    });
    const queue = PgQueue.wrap<{ n: number }>({ db, queue: "jobs", pollIntervalMs: 5 });

    const items = await run(queue.subscribe().take(2).toArray().orDie());
    expect(items).toEqual([{ n: 1 }, { n: 2 }]);
    expect(fake.allSql).toContain("FOR UPDATE SKIP LOCKED");
  });

  it("a codec that throws fails the stream with a PostgresError", async () => {
    const { db } = fakeDb((sql) => (sql.includes("DELETE") ? [{ id: "1", payload: "x" }] : []));
    const queue = PgQueue.wrap<string>({
      db,
      queue: "jobs",
      pollIntervalMs: 5,
      codec: {
        encode: (v) => v,
        decode: () => {
          throw new Error("bad payload");
        },
      },
    });

    await expect(run(queue.subscribe().take(1).toArray())).rejects.toMatchObject({
      _tag: "PostgresError",
    });
  });

  it("metrics coerces counts to numbers and counts dead messages", async () => {
    const { db } = fakeDb((sql) => {
      if (sql.includes("FILTER")) {
        return [{ pending: "2", processing: "1", completed: "0", dead: "4", total: "7" }];
      }
      return [];
    });
    const queue = PgQueue.wrap<string>({ db, queue: "jobs" });
    expect(await queue.metrics()).toEqual({
      pending: 2,
      processing: 1,
      completed: 0,
      dead: 4,
      total: 7,
    });
  });
});
