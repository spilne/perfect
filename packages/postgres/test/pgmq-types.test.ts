import { describe, it, expect } from "bun:test";
import { fifoMessage, ReadMode } from "../src/pgmq/types";

describe("fifoMessage", () => {
  it("routes the group through the x-pgmq-group header", () => {
    const msg = fifoMessage({ data: { id: 1 }, group: "tenant-a", delay: 5 });
    expect(msg).toEqual({
      data: { id: 1 },
      delay: 5,
      headers: { "x-pgmq-group": "tenant-a" },
    });
  });
});

describe("ReadMode constructors", () => {
  it("tag each mode for the matching pgmq SQL function", () => {
    expect(ReadMode.standard({ vt: 30, qty: 10 })._tag).toBe("standard");
    expect(ReadMode.poll({ vt: 30, qty: 10, maxPollSeconds: 5 })._tag).toBe("poll");
    expect(ReadMode.grouped({ vt: 30, qty: 10 })._tag).toBe("grouped");
    expect(ReadMode.groupedPoll({ vt: 30, qty: 10 })._tag).toBe("grouped-poll");
    expect(ReadMode.groupedRoundRobin({ vt: 30, qty: 10 })._tag).toBe("grouped-round-robin");
    expect(ReadMode.groupedRoundRobinPoll({ vt: 30, qty: 10 })._tag).toBe(
      "grouped-round-robin-poll",
    );
    expect(ReadMode.groupedHead({ vt: 30, qty: 10 })._tag).toBe("grouped-head");
    expect(ReadMode.groupedHeadPoll({ vt: 30, qty: 10 })._tag).toBe("grouped-head-poll");
  });

  it("carries vt/qty through", () => {
    const m = ReadMode.standard({ vt: 7, qty: 3 });
    expect(m.vt).toBe(7);
    expect(m.qty).toBe(3);
  });
});
