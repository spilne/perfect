import { describe, expect, test } from "bun:test";
import { die, run, succeed, type Throws } from "@spilne/perfect-core";
import type { Stream } from "@spilne/perfect-core/stream";
import { PgmqQueue, PgmqSchemaValidationError, type PgmqQueueError } from "../src/pgmq/pgmq-queue";
import { PostgresError } from "../src/lib/postgres-error";
import { fakeDb } from "./fake-db";

describe("PgmqQueue errors", () => {
  test("exposes driver failures as PostgresError", async () => {
    const { db } = fakeDb(() => {
      throw new Error("database unavailable");
    });
    const queue = PgmqQueue.wrap<number>({ db, queue: "jobs", defaultPollIntervalMs: 1 });
    const typed: Stream<number, Throws<PgmqQueueError>> = queue.subscribe();

    const error = await run(
      typed
        .take(1)
        .toArray()
        .map(() => null as PostgresError | null)
        .catchTag("PostgresError", (failure) => succeed(failure))
        .catchTag("PgmqSchemaValidationError", (failure) => die(failure)),
    );

    expect(error).toBeInstanceOf(PostgresError);
    expect(error?.operation).toBe("pgmq.pop");
    expect(error?.cause).toBeInstanceOf(Error);
  });

  test("exposes schema failures as PgmqSchemaValidationError", async () => {
    let delivered = false;
    const { db } = fakeDb((sql) => {
      if (!delivered && sql.includes("pgmq.pop")) {
        delivered = true;
        return [
          {
            msg_id: "7",
            read_ct: 1,
            enqueued_at: new Date("2026-01-01T00:00:00Z"),
            vt: new Date("2026-01-01T00:00:00Z"),
            message: { id: 42 },
            headers: null,
          },
        ];
      }
      return [];
    });
    const queue = PgmqQueue.wrap<{ id: string }>({
      db,
      queue: "jobs",
      defaultPollIntervalMs: 1,
      schema: {
        safeParse: (value) =>
          typeof value === "object" &&
          value !== null &&
          typeof (value as { id?: unknown }).id === "string"
            ? { success: true as const, data: value as { id: string } }
            : { success: false as const, error: new TypeError("invalid id") },
      },
    });

    const error = await run(
      queue
        .subscribe()
        .take(1)
        .toArray()
        .map(() => null as PgmqSchemaValidationError | null)
        .catchTag("PgmqSchemaValidationError", (failure) => succeed(failure))
        .catchTag("PostgresError", (failure) => die(failure)),
    );

    expect(error).toBeInstanceOf(PgmqSchemaValidationError);
    expect(error?.queueName).toBe("jobs");
    expect(error?.msgId).toBe(7);
  });
});

describe("PgmqQueue FIFO", () => {
  test("uses head-of-group reads and exposes visibility extension", async () => {
    let delivered = false;
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("pgmq.read_grouped_head") && !delivered) {
        delivered = true;
        return [
          {
            msg_id: "11",
            read_ct: 1,
            enqueued_at: new Date("2026-01-01T00:00:00Z"),
            vt: new Date("2026-01-01T00:00:30Z"),
            message: { userId: "u_1", sequence: 1 },
            headers: { "x-pgmq-group": "u_1" },
          },
        ];
      }
      return [];
    });
    const queue = PgmqQueue.wrap<{ userId: string; sequence: number }>({
      db,
      queue: "ordered_jobs",
      fifo: true,
      defaultPollIntervalMs: 1,
    });

    const [envelope] = await run(queue.subscribeAck().take(1).toArray().orDie());
    expect(envelope!.value.sequence).toBe(1);
    expect(fake.allSql).toContain("pgmq.read_grouped_head");

    await envelope!.extendVisibility(60);
    const extension = fake.queries.find((query) => query.sql.includes("pgmq.set_vt"));
    expect(extension?.params).toContain(60);
  });

  test("publishes the identity as x-pgmq-group", async () => {
    const { db, fake } = fakeDb(() => [{ send: "1" }]);
    const queue = PgmqQueue.wrap<{ userId: string }>({ db, queue: "ordered_jobs", fifo: true });

    await queue.publish({ userId: "u_1" }, { group: "u_1" });

    const publish = fake.queries.find((query) => query.sql.includes("pgmq.send"));
    expect(publish?.params).toContain('{"x-pgmq-group":"u_1"}');
  });
});
