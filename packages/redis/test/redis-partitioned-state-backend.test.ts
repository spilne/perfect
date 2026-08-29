import { describe, expect, test } from "bun:test";
import {
  LeaseEpoch,
  Partition,
  SourceRecordId,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
} from "@spilne/perfect-core/connect";
import type { RedisClient } from "../src/redis-client";
import { RedisPartitionedStateBackend } from "../src/redis-partitioned-state-backend";

const scope = {
  topologyId: TopologyId("orders"),
  stageId: StageId("totals"),
  partition: Partition(2),
};

describe("RedisPartitionedStateBackend", () => {
  test("keeps the atomic key family in one cluster hash slot", async () => {
    const calls: Array<{ numKeys: number; args: Array<string | number> }> = [];
    const results: unknown[] = [
      ["worker-a", "1", String(Date.now() + 60_000)],
      "committed",
      ["7", "cp-7", "count", "7"],
    ];
    const redis = {
      eval: async (_script: string, numKeys: number, ...args: Array<string | number>) => {
        calls.push({ numKeys, args });
        return results.shift();
      },
    } as unknown as RedisClient;
    const backend = new RedisPartitionedStateBackend<number>({ redis, key: "topology-state" });
    const lease = await backend.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-a"),
      leaseMs: 60_000,
    });
    expect(
      await backend.commit({
        lease: lease!,
        mutations: [{ type: "put", key: "count", value: 7 }],
        sourceId: SourceRecordId("orders:2:7"),
        sourceOffset: "7",
        checkpointId: StateCheckpointId("cp-7"),
      }),
    ).toBe("committed");
    expect((await backend.load(lease!))?.values.get("count")).toBe(7);

    for (const call of calls) {
      for (const key of call.args.slice(0, call.numKeys)) {
        expect(String(key).startsWith("{topology-state}:")).toBe(true);
      }
    }
    expect(calls[1]?.numKeys).toBe(3);
  });

  test("rejects invalid retention and lease durations", async () => {
    const redis = { eval: async () => [] } as unknown as RedisClient;
    expect(() => new RedisPartitionedStateBackend({ redis, processedRetentionMs: 0 })).toThrow(
      "processedRetentionMs",
    );
    const backend = new RedisPartitionedStateBackend({ redis });
    await expect(
      backend.renew({
        lease: {
          scope,
          ownerId: TopologyInstanceId("worker"),
          epoch: LeaseEpoch(1),
          expiresAt: Date.now() + 1,
        },
        leaseMs: 0,
      }),
    ).rejects.toThrow("leaseMs");
  });
});
