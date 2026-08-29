import { describe, it, expect } from "bun:test";
import { run } from "@spilne/perfect-core";
import { PgQueue } from "../src/lib/pg-queue";
import { fakeDb } from "./fake-db";

describe("PgQueue (fake db)", () => {
  it("publish inserts a JSON payload into the queue table", async () => {
    const { db, fake } = fakeDb();
    const queue = PgQueue.wrap<{ userId: string }>({ db, queue: "jobs" });

    await queue.publish({ userId: "u_1" });

    expect(fake.allSql).toContain("INSERT INTO pgq_jobs");
    expect(fake.allSql).toContain('{"userId":"u_1"}');
  });

  it("subscribeAck claims via SKIP LOCKED and ack deletes the row", async () => {
    const createdAt = new Date("2026-01-01T00:00:00Z");
    let dequeued = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("UPDATE pgq_jobs") && sql.includes("SKIP LOCKED") && !dequeued) {
        dequeued = true;
        return [
          {
            id: "7",
            payload: { userId: "u_1" },
            attempt_count: 1,
            created_at: createdAt,
            headers: null,
          },
        ];
      }
      return [];
    });
    const queue = PgQueue.wrap<{ userId: string }>({ db, queue: "jobs", pollIntervalMs: 5 });

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    expect(envelope!.value).toEqual({ userId: "u_1" });
    expect(envelope!.metadata.msgId).toBe(7);
    expect(envelope!.metadata.attemptCount).toBe(1);

    // The claim query must use SKIP LOCKED with a visibility timeout
    const claim = fake.queries.find((q) => q.sql.includes("SKIP LOCKED"))!;
    expect(claim.sql).toContain("FOR UPDATE SKIP LOCKED");
    expect(claim.sql).toContain("visible_at = NOW() + INTERVAL '30 seconds'");

    await envelope!.ack();
    expect(fake.allSql).toContain("DELETE FROM pgq_jobs WHERE id = 7");
  });

  it("nack makes the message visible again", async () => {
    let dequeued = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("SKIP LOCKED") && sql.includes("UPDATE") && !dequeued) {
        dequeued = true;
        return [{ id: "3", payload: "x", attempt_count: 1, created_at: new Date(), headers: null }];
      }
      return [];
    });
    const queue = PgQueue.wrap<string>({ db, queue: "jobs", pollIntervalMs: 5 });

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    await envelope!.nack();
    expect(fake.allSql).toContain(
      "SET status = 'pending', visible_at = NOW(), locked_by = NULL WHERE id = 3",
    );
  });

  it("subscribe pops (read + delete) and decodes payloads", async () => {
    let popped = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("DELETE FROM pgq_jobs") && sql.includes("SKIP LOCKED") && !popped) {
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

  it("metrics coerces counts to numbers", async () => {
    const { db } = fakeDb((sql) => {
      if (sql.includes("FILTER")) {
        return [{ pending: "2", processing: "1", completed: "0", total: "3" }];
      }
      return [];
    });
    const queue = PgQueue.wrap<string>({ db, queue: "jobs" });
    expect(await queue.metrics()).toEqual({ pending: 2, processing: 1, completed: 0, total: 3 });
  });
});
