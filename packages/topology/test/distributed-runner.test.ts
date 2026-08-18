// DistributedRunner tests — everything runs against an in-memory
// ShuffleTransport; no real broker.

import { describe, test, expect } from "bun:test";
import { succeed, sync } from "@perfect/core";
import { Stream } from "@perfect/core/stream";
import type {
  Streamable,
  Acknowledgeable,
  Sinkable,
  Codec,
  ShuffleTransport,
} from "@perfect/core/connect";
import { StreamTopology, DistributedRunner, planStages, ConsumerGroup, ChannelName } from "../src";

// ---------------------------------------------------------------------------
// In-memory ShuffleTransport for testing
// ---------------------------------------------------------------------------

function createInMemoryTransport(): ShuffleTransport {
  const topics = new Map<string, unknown[]>();

  function getTopic(name: string) {
    if (!topics.has(name)) topics.set(name, []);
    return topics.get(name)!;
  }

  return {
    async getOrCreateRepartitionChannel(params) {
      const items = getTopic(params.name);
      return {
        source: {
          codec: params.codec,
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
        },
        sink: {
          codec: params.codec,
          publish: (value: unknown) => sync(() => void items.push(value)),
        },
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function createTestSource<T>(items: T[]): Streamable<T> & Acknowledgeable<T> {
  const codec: Codec<T> = { encode: (v) => v, decode: (v) => v as T };
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
    codec: { encode: (v) => v, decode: (v) => v as T },
    publish: (value: T) => sync(() => void items.push(value)),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("DistributedRunner", () => {
  test("single stage (no shuffle) delegates to TopologyRunner", async () => {
    const source = createTestSource([{ v: 1 }, { v: 2 }, { v: 3 }]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((e) => e.v * 2)
      .to(sink);

    const handle = await DistributedRunner.run(topology, {
      group: ConsumerGroup("no-shuffle"),
      shuffleTransport: createInMemoryTransport(),
    });

    // Wait for processing
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items.sort((a, b) => a - b)).toEqual([2, 4, 6]);
  });

  test("two-stage: planner correctly identifies stages", () => {
    const source = createTestSource([
      { userId: "u1", v: 10 },
      { userId: "u2", v: 20 },
      { userId: "u1", v: 30 },
    ]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.userId)
      .shuffle()
      .map((e) => e)
      .to(createTestSink());

    const plan = planStages({ compiled: topology.compiled, group: ConsumerGroup("two-stage") });

    expect(plan.stages).toHaveLength(2);
    expect(plan.repartitionTopics).toEqual([ChannelName("two-stage-repartition-0")]);

    // Stage 0 reads from original, writes to repartition
    expect(plan.stages[0]!.source).toBe("original");
    expect(plan.stages[0]!.sink).toEqual({
      repartitionTopic: ChannelName("two-stage-repartition-0"),
    });
    expect(plan.stages[0]!.keyFn).toBeDefined();

    // Stage 1 reads from repartition, writes to terminal
    expect(plan.stages[1]!.source).toEqual({
      repartitionTopic: ChannelName("two-stage-repartition-0"),
    });
    expect(plan.stages[1]!.sink).toBe("terminal");
  });

  test("metrics are aggregated across stages", async () => {
    const source = createTestSource([{ v: 1 }]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((e) => e.v)
      .to(sink);

    const handle = await DistributedRunner.run(topology, {
      group: ConsumerGroup("metrics-test"),
      shuffleTransport: createInMemoryTransport(),
    });

    await new Promise((r) => setTimeout(r, 200));

    const metrics = handle.metrics();
    expect(metrics.itemsProcessed).toBeGreaterThanOrEqual(0);

    await handle.shutdown();
    expect(handle.isRunning()).toBe(false);
  });
});
