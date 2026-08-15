import { describe, test, expect } from "bun:test";
import {
  AsyncScheduler,
  BunScheduler,
  SyncScheduler,
  getDefaultScheduler,
  setDefaultScheduler,
} from "../src/scheduler";

const macrotask = () => new Promise((r) => setImmediate(r));

describe("AsyncScheduler", () => {
  test("schedule defers execution to an async tick, preserving order", async () => {
    const s = new AsyncScheduler();
    const ran: number[] = [];

    s.schedule(() => ran.push(1));
    s.schedule(() => ran.push(2));
    expect(ran).toEqual([]); // nothing runs synchronously

    await macrotask();
    expect(ran).toEqual([1, 2]);
  });

  test("tasks scheduled during a batch also run (follow-up drain)", async () => {
    const s = new AsyncScheduler();
    const ran: string[] = [];

    s.schedule(() => {
      ran.push("a");
      s.schedule(() => ran.push("b"));
    });

    await macrotask();
    await macrotask();
    expect(ran).toEqual(["a", "b"]);
  });

  test("flush runs everything queued synchronously", () => {
    const s = new AsyncScheduler();
    const ran: number[] = [];

    s.schedule(() => ran.push(1));
    s.schedule(() => {
      ran.push(2);
      s.schedule(() => ran.push(3));
    });

    s.flush();
    expect(ran).toEqual([1, 2, 3]);
  });

  test("shutdown drops queued tasks", async () => {
    const s = new AsyncScheduler();
    const ran: number[] = [];

    s.schedule(() => ran.push(1));
    s.shutdown();

    await macrotask();
    expect(ran).toEqual([]);
  });

  test("BunScheduler is an alias of AsyncScheduler", () => {
    expect(BunScheduler).toBe(AsyncScheduler);
  });
});

describe("SyncScheduler", () => {
  test("schedule only queues; flush runs synchronously in FIFO order", () => {
    const s = new SyncScheduler();
    const ran: number[] = [];

    s.schedule(() => ran.push(1));
    s.schedule(() => ran.push(2));
    expect(ran).toEqual([]);

    s.flush();
    expect(ran).toEqual([1, 2]);
  });

  test("flush also runs tasks scheduled during flush", () => {
    const s = new SyncScheduler();
    const ran: string[] = [];

    s.schedule(() => {
      ran.push("a");
      s.schedule(() => ran.push("b"));
    });

    s.flush();
    expect(ran).toEqual(["a", "b"]);
  });

  test("shutdown clears the queue", () => {
    const s = new SyncScheduler();
    const ran: number[] = [];

    s.schedule(() => ran.push(1));
    s.shutdown();
    s.flush();
    expect(ran).toEqual([]);
  });
});

describe("default scheduler", () => {
  test("setDefaultScheduler / getDefaultScheduler round-trip", () => {
    const original = getDefaultScheduler();
    const replacement = new SyncScheduler();
    try {
      setDefaultScheduler(replacement);
      expect(getDefaultScheduler()).toBe(replacement);
    } finally {
      setDefaultScheduler(original);
    }
    expect(getDefaultScheduler()).toBe(original);
  });
});
