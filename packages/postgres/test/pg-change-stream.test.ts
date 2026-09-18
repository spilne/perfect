import { describe, it, expect } from "bun:test";
import { type Eff, type Stream, SyncScheduler, runFiber } from "@spilne/perfect-core";
import { PgChangeStream, offsetToDate } from "../src/lib/pg-change-stream";

describe("offsetToDate — Offset → poll cursor mapping", () => {
  it("earliest maps to the epoch", () => {
    expect(offsetToDate({ type: "earliest" }).getTime()).toBe(0);
  });

  it("latest maps to (approximately) now", () => {
    const before = Date.now();
    const d = offsetToDate({ type: "latest" }).getTime();
    expect(d).toBeGreaterThanOrEqual(before);
    expect(d).toBeLessThanOrEqual(Date.now());
  });

  it("timestamp maps to that millisecond", () => {
    expect(offsetToDate({ type: "timestamp", value: 1735689600000 }).getTime()).toBe(1735689600000);
  });

  it("specific is interpreted as an ISO timestamp string", () => {
    const iso = "2026-01-01T00:00:00.000Z";
    expect(offsetToDate({ type: "specific", value: iso }).toISOString()).toBe(iso);
  });
});

describe("PgChangeStream LISTEN setup", () => {
  it("unlistens when the subscriber is interrupted as LISTEN completes", async () => {
    let unlistened = 0;
    let listened!: () => void;
    const sql = {
      listen: () =>
        new Promise<{ unlisten(): Promise<void> }>((resolve) => {
          listened = () =>
            resolve({
              unlisten: async () => {
                unlistened++;
              },
            });
        }),
    };
    const changes = new PgChangeStream<unknown>({
      db: {} as never,
      sql: sql as never,
      channel: "changes",
      table: "events",
    });
    const scheduler = new SyncScheduler();
    const listen = (
      changes as unknown as { createListenStream(): Stream<unknown, unknown> }
    ).createListenStream();
    const fiber = runFiber(listen.drain() as Eff<void, never>, scheduler);
    scheduler.flush();

    listened();
    await Promise.resolve();
    await Promise.resolve();
    expect(fiber.status).toBe("ready");
    fiber.interrupt();
    scheduler.flush();
    await Promise.resolve();

    expect(fiber.result).toEqual({ ok: false, cause: { _tag: "Interrupt" } });
    expect(unlistened).toBe(1);
  });
});
