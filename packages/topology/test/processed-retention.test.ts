import { describe, expect, test } from "bun:test";
import { InMemoryPartitionedState, SourceRecordId } from "@spilne/perfect-core/connect";
import { ConsumerGroup, InMemoryState, StreamTopology, TopologyRunner } from "../src";
import { succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";

const scope = { topologyId: "t", stageId: "s", partition: 0 } as never;

describe("InMemoryPartitionedState processedRetentionMs", () => {
  test("forgets processed records older than the retention", async () => {
    const state = new InMemoryPartitionedState({ processedRetentionMs: 20 });
    const lease = (await state.acquire({ scope, ownerId: "me" as never, leaseMs: 60_000 }))!;
    await state.commit({ lease, mutations: [], sourceId: SourceRecordId("a") });
    expect(await state.isProcessed({ lease, sourceId: SourceRecordId("a") })).toBe(true);

    await new Promise((r) => setTimeout(r, 30));
    // The next commit cleans up the old record.
    await state.commit({ lease, mutations: [], sourceId: SourceRecordId("b") });
    expect(await state.isProcessed({ lease, sourceId: SourceRecordId("a") })).toBe(false);
    expect(await state.isProcessed({ lease, sourceId: SourceRecordId("b") })).toBe(true);
  });

  test("without a retention, records are kept", async () => {
    const state = new InMemoryPartitionedState();
    const lease = (await state.acquire({ scope, ownerId: "me" as never, leaseMs: 60_000 }))!;
    await state.commit({ lease, mutations: [], sourceId: SourceRecordId("a") });
    await new Promise((r) => setTimeout(r, 30));
    await state.commit({ lease, mutations: [], sourceId: SourceRecordId("b") });
    expect(await state.isProcessed({ lease, sourceId: SourceRecordId("a") })).toBe(true);
  });

  test("rejects a retention that isn't a positive integer", () => {
    expect(() => new InMemoryPartitionedState({ processedRetentionMs: 0 })).toThrow(RangeError);
  });
});

describe("topology with a plain stateBackend and processedRetentionMs", () => {
  const sourceOf = (offsets: number[]) => ({
    codec: { encode: JSON.stringify, decode: JSON.parse },
    subscribe: () => Stream.fromIterable(offsets.map((id) => ({ id }))),
    subscribeAck: () =>
      Stream.fromIterable(
        offsets.map((offset) => ({
          value: { id: offset },
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: { topic: "t", partition: 0, offset: String(offset) },
        })),
      ),
  });
  const sink = {
    codec: { encode: JSON.stringify, decode: JSON.parse },
    publish: () => sync(() => undefined),
  };
  const runOnce = async (store: InMemoryState<string, unknown>, offsets: number[]) => {
    const topology = StreamTopology.source(sourceOf(offsets) as never).to(sink as never);
    const handle = await TopologyRunner.run(topology, {
      group: ConsumerGroup("retention"),
      stateBackend: store,
      processedRetentionMs: 25,
    });
    await handle.awaitExit();
    await handle.shutdown();
  };
  const seenKeys = async (store: InMemoryState<string, unknown>) =>
    [...(await store.entries())].filter(([key]) => key.includes("@seen:")).map(([key]) => key);

  test("seen keys older than the retention are deleted, including ones from an earlier run", async () => {
    const store = new InMemoryState<string, unknown>();
    await runOnce(store, [0, 1, 2]);
    expect(await seenKeys(store)).toHaveLength(3);

    await new Promise((r) => setTimeout(r, 40));
    // The second run loads the old keys and deletes them on its commits.
    await runOnce(store, [3, 4]);
    const left = await seenKeys(store);
    expect(left).toHaveLength(2);
    expect(left.every((key) => key.endsWith(":3") || key.endsWith(":4"))).toBe(true);
  });
});
