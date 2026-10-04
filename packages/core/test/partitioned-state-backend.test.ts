import { describe, expect, test } from "bun:test";
import {
  InMemoryPartitionedState,
  Partition,
  SourceRecordId,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
  type StatePartitionScope,
} from "../src/connect";

const scope: StatePartitionScope = {
  topologyId: TopologyId("orders"),
  stageId: StageId("totals"),
  partition: Partition(2),
};

describe("InMemoryPartitionedState", () => {
  test("atomically commits state, source progress, and duplicate detection", async () => {
    const backend = new InMemoryPartitionedState<number>();
    const lease = await backend.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-a"),
      leaseMs: 60_000,
    });
    expect(lease).toBeDefined();

    expect(
      await backend.commit({
        lease: lease!,
        sourceId: SourceRecordId("orders:2:41"),
        sourceOffset: "42",
        checkpointId: StateCheckpointId("cp-1"),
        mutations: [{ type: "put", key: "user-7", value: 3 }],
      }),
    ).toBe("committed");
    expect(
      await backend.commit({
        lease: lease!,
        sourceId: SourceRecordId("orders:2:41"),
        mutations: [{ type: "put", key: "user-7", value: 99 }],
      }),
    ).toBe("duplicate");

    const snapshot = await backend.load(lease!);
    expect(snapshot?.values.get("user-7")).toBe(3);
    expect(snapshot?.sourceOffset).toBe("42");
    expect<string | undefined>(snapshot?.checkpointId).toBe("cp-1");

    // A batch commit marks all its source records at once...
    expect(
      await backend.commit({
        lease: lease!,
        mutations: [{ type: "put", key: "batch", value: 1 }],
        sourceIds: [SourceRecordId("orders:2:50"), SourceRecordId("orders:2:51")],
      }),
    ).toBe("committed");
    // ...and a batch with one record that was already processed changes nothing.
    expect(
      await backend.commit({
        lease: lease!,
        mutations: [{ type: "put", key: "batch", value: 2 }],
        sourceIds: [SourceRecordId("orders:2:51"), SourceRecordId("orders:2:52")],
      }),
    ).toBe("duplicate");
    expect(
      await backend.isProcessed({ lease: lease!, sourceId: SourceRecordId("orders:2:50") }),
    ).toBe(true);
    expect(
      await backend.isProcessed({ lease: lease!, sourceId: SourceRecordId("orders:2:52") }),
    ).toBe(false);
    expect((await backend.load(lease!))?.values.get("batch")).toBe(1);
  });

  test("increments the fence and rejects a stale owner", async () => {
    const backend = new InMemoryPartitionedState<number>();
    const first = await backend.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-a"),
      leaseMs: 60_000,
    });
    expect(
      await backend.acquire({
        scope,
        ownerId: TopologyInstanceId("worker-b"),
        leaseMs: 60_000,
      }),
    ).toBeUndefined();
    expect(await backend.release(first!)).toBe(true);

    const second = await backend.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-b"),
      leaseMs: 60_000,
    });
    expect<number | undefined>(second?.epoch).toBe((first?.epoch ?? 0) + 1);
    expect(
      await backend.commit({
        lease: first!,
        mutations: [{ type: "put", key: "stale", value: 1 }],
      }),
    ).toBe("fenced");
  });
});
