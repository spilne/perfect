import { describe, expect, test } from "bun:test";
import {
  Partition,
  LeaseEpoch,
  SourceRecordId,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
} from "@perfect/core/connect";
import { PgPartitionedStateBackend } from "../src/lib/pg-partitioned-state-backend";
import { fakeDb } from "./fake-db";

const scope = {
  topologyId: TopologyId("orders"),
  stageId: StageId("totals"),
  partition: Partition(2),
};

describe("PgPartitionedStateBackend", () => {
  test("uses a row lock to atomically commit dedupe, state, and progress", async () => {
    const { db, fake } = fakeDb((query) => {
      if (query.includes('INSERT INTO "partition_state"') && query.includes("RETURNING")) {
        return [
          { owner_id: "worker-a", epoch: "1", lease_expires_at: new Date(Date.now() + 60_000) },
        ];
      }
      if (query.includes("SELECT 1") && query.includes("FOR UPDATE")) return [{ owned: 1 }];
      if (query.includes('INSERT INTO "partition_seen"')) return [{ source_id: "orders:2:7" }];
      return [];
    });
    const backend = new PgPartitionedStateBackend({
      db,
      table: "partition_state",
      processedTable: "partition_seen",
    });
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

    expect(fake.allSql).toContain("FOR UPDATE");
    expect(fake.allSql).toContain("INTERVAL '1 millisecond'");
    expect(fake.allSql).toContain("ON CONFLICT DO NOTHING");
    expect(fake.allSql).toContain("jsonb_set");
    expect(fake.allSql).toContain("source_offset = COALESCE");
  });

  test("returns fenced before applying mutations when ownership is stale", async () => {
    const { db, fake } = fakeDb(() => []);
    const backend = new PgPartitionedStateBackend({ db });
    const result = await backend.commit({
      lease: {
        scope,
        ownerId: TopologyInstanceId("stale"),
        epoch: LeaseEpoch(1),
        expiresAt: Date.now() + 60_000,
      },
      mutations: [{ type: "put", key: "count", value: 99 }],
    });

    expect(result).toBe("fenced");
    expect(fake.allSql).not.toContain("jsonb_set");
  });

  test("validates generated table identifiers", () => {
    const { db } = fakeDb();
    expect(() => new PgPartitionedStateBackend({ db, table: "state; DROP TABLE state" })).toThrow(
      "Invalid PostgreSQL identifier",
    );
  });
});
