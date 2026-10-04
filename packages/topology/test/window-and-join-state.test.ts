import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import type { Acknowledgeable, Sinkable, Streamable } from "@spilne/perfect-core/connect";
import {
  CheckpointName,
  ConsumerGroup,
  InMemoryState,
  JoinBuffer,
  StreamTopology,
  TopologyRunner,
  WindowManager,
} from "../src";

function createTestSource<T>(items: T[]): Streamable<T> & Acknowledgeable<T> {
  const codec = {
    encode: (v: T) => JSON.stringify(v),
    decode: (raw: unknown) => JSON.parse(raw as string),
  };
  return {
    codec,
    subscribe: () => Stream.fromIterable(items),
    subscribeAck: () =>
      Stream.fromIterable(
        items.map((value) => ({
          value,
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: {},
        })),
      ),
  };
}

function createTestSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return {
    items,
    codec: {
      encode: (v: T) => JSON.stringify(v),
      decode: (raw: unknown) => JSON.parse(raw as string),
    },
    publish: (value: T) => sync(() => void items.push(value)),
  };
}

const counting = {
  init: () => ({ count: 0 }),
  add: (state: { count: number }) => ({ count: state.count + 1 }),
  emit: (key: string, _w: unknown, state: { count: number }) => ({ key, count: state.count }),
};

describe("WindowManager keys", () => {
  test("keys containing ':' stay separate", () => {
    const manager = new WindowManager({ type: "tumbling", windowMs: 1000 }, counting);
    manager.add("a", {}, 100);
    manager.add("a:b", {}, 100);

    // flush("a") must not take the window of "a:b" (prefix "a:" matched it before)
    expect(manager.flush("a", 1100)).toEqual([{ key: "a", count: 1 }]);
    expect(manager.flushAll()).toEqual([{ key: "a:b", count: 1 }]);
  });

  test("restores snapshots written by older versions", () => {
    const manager = new WindowManager({ type: "tumbling", windowMs: 1000 }, counting);
    manager.restore([
      {
        windowKey: "tenant:7:0",
        entry: { window: { start: 0, end: 1000 }, state: { count: 4 }, lastActivity: 10 },
      },
    ]);
    expect(manager.flush("tenant:7", 1100)).toEqual([{ key: "tenant:7", count: 4 }]);
  });

  test("snapshotKey only contains that key's windows", () => {
    const manager = new WindowManager({ type: "tumbling", windowMs: 1000 }, counting);
    manager.add("a", {}, 100);
    manager.add("b", {}, 100);
    expect(manager.snapshotKey("a").map((s) => s.key)).toEqual(["a"]);
    expect(manager.size).toBe(2);
  });
});

describe("JoinBuffer", () => {
  test("items of a key that went quiet are removed after a window", () => {
    const buffer = new JoinBuffer<string, string>(100);
    buffer.addLeft("quiet", "old", 0);
    buffer.addLeft("busy", "x", 50);
    // Much later, only the busy key gets items.
    buffer.addLeft("busy", "y", 1_000);
    expect(buffer.stats().leftKeys).toBe(1);
  });
});

describe("aggregate state across a restart", () => {
  test("windows saved in the old single-entry format are picked up once, then never again", async () => {
    const group = ConsumerGroup("test-window-migration");
    const state = new InMemoryState<string, unknown>();
    // What an older version saved: every window of the partition in one entry.
    await state.put("window:0", [
      {
        windowKey: "a:0",
        entry: { window: { start: 0, end: 60_000 }, state: { count: 2 }, lastActivity: 10 },
      },
    ]);
    await state.checkpoint({ name: CheckpointName(`topology:${group}`) });

    const sink = createTestSink<{ key: string; count: number }>();
    const runWith = async (events: Array<{ userId: string; ts: number }>) => {
      const topology = StreamTopology.source(createTestSource(events))
        .keyBy((e) => e.userId)
        .tumbling(60_000)
        .aggregate({
          init: () => ({ count: 0 }),
          add: (s: { count: number }) => ({ count: s.count + 1 }),
          emit: (key: string, _w: unknown, s: { count: number }) => ({ key, count: s.count }),
        })
        .to(sink);
      const handle = await TopologyRunner.run(topology, { group, stateBackend: state });
      await handle.awaitExit();
      await handle.shutdown();
    };

    // Run 1: key "a" continues from its saved count of 2, and the end of the
    // run emits the window with 2 + 1.
    await runWith([{ userId: "a", ts: 30 }]);
    // Runs 2 and 3: the old saved window must not come back and be emitted
    // again. (Each run's end emits its own newer window, with a count of 1.)
    await runWith([{ userId: "a", ts: 61_000 }]);
    await runWith([{ userId: "a", ts: 62_000 }]);

    const oldWindow = sink.items.filter((item) => item.key === "a" && item.count === 3);
    expect(oldWindow).toHaveLength(1);
  });
});
