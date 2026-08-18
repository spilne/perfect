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
    expect(snapshot?.checkpointId).toBe("cp-1");
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
    expect(second?.epoch).toBe((first?.epoch ?? 0) + 1);
    expect(
      await backend.commit({
        lease: first!,
        mutations: [{ type: "put", key: "stale", value: 1 }],
      }),
    ).toBe("fenced");
  });
});
