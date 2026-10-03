import { describe, expect, test } from "bun:test";
import { AssignmentTracker } from "../src/assignment-tracker";

describe("AssignmentTracker", () => {
  test("tells assigned listeners about a partition before its first message", async () => {
    const tracker = new AssignmentTracker();
    const events: string[] = [];
    tracker.onAssigned(async (a) => {
      await new Promise((r) => setTimeout(r, 1));
      events.push(`assigned ${a.topic}/${a.partitions.join(",")}`);
    });

    const wait = tracker.ensureAssigned("orders", 3);
    expect(wait).toBeDefined();
    await wait;
    events.push("message");
    expect(events).toEqual(["assigned orders/3", "message"]);
  });

  test("a message from a partition we already hold needs no wait", async () => {
    const tracker = new AssignmentTracker();
    tracker.onAssigned(() => {});
    await tracker.ensureAssigned("orders", 1);
    expect(tracker.ensureAssigned("orders", 1)).toBeUndefined();
  });

  test("a listener that throws is reported once, then the tracker keeps working", async () => {
    const tracker = new AssignmentTracker();
    let fail = true;
    tracker.onAssigned(() => {
      if (fail) throw new Error("listener broke");
    });

    await expect(tracker.ensureAssigned("orders", 0)!).rejects.toThrow("listener broke");
    fail = false;
    // The next partition goes through: one bad listener call no longer
    // poisons every later message.
    await tracker.ensureAssigned("orders", 1);
    expect(tracker.ensureAssigned("orders", 1)).toBeUndefined();
  });

  test("changes reach listeners in order", async () => {
    const tracker = new AssignmentTracker();
    const events: string[] = [];
    tracker.onAssigned(async (a) => {
      await new Promise((r) => setTimeout(r, 2));
      events.push(`+${a.partitions.join(",")}`);
    });
    tracker.onRevoked((a) => {
      events.push(`-${a.partitions.join(",")}`);
    });

    tracker.assignAll([["orders", [0, 1]]], 1);
    tracker.revokeAll();
    tracker.assignAll([["orders", [2]]], 2);
    await tracker.settle();
    expect(events).toEqual(["+0,1", "-0,1", "+2"]);
  });

  test("unsubscribing stops the calls", async () => {
    const tracker = new AssignmentTracker();
    let calls = 0;
    const stop = tracker.onAssigned(() => {
      calls++;
    });
    await tracker.ensureAssigned("orders", 0);
    stop();
    tracker.assignAll([["orders", [5]]]);
    await tracker.settle();
    expect(calls).toBe(1);
  });
});
