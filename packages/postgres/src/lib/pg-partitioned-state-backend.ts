import { sql, type SQL } from "drizzle-orm";
import {
  JsonCodec,
  LeaseEpoch,
  StateCheckpointId,
  TopologyInstanceId,
  type Codec,
  type PartitionCommitResult,
  type PartitionStateCommit,
  type PartitionStateSnapshot,
  type StatePartitionLease,
  type StatePartitionScope,
  type TransactionalPartitionedStateBackend,
  type SourceRecordId,
} from "@spilne/perfect-core/connect";
import { execRaw, type DrizzleDb } from "./drizzle-db";

export interface PgPartitionedStateBackendConfig<V = unknown> {
  db: DrizzleDb;
  table?: string;
  processedTable?: string;
  codec?: Codec<V>;
  processedRetentionMs?: number;
}

export class PgPartitionedStateBackend<V = unknown> implements TransactionalPartitionedStateBackend<
  V,
  DrizzleDb
> {
  readonly transactionDomain: object;
  private readonly db: DrizzleDb;
  private readonly table: string;
  private readonly processedTable: string;
  private readonly codec: Codec<V>;
  private readonly processedRetentionMs?: number;

  constructor(config: PgPartitionedStateBackendConfig<V>) {
    this.db = config.db;
    this.transactionDomain = config.db;
    this.table = identifier(config.table ?? "perfect_partition_state");
    this.processedTable = identifier(
      config.processedTable ?? `${config.table ?? "perfect_partition_state"}_processed`,
    );
    this.codec = config.codec ?? (JsonCodec as Codec<V>);
    this.processedRetentionMs = config.processedRetentionMs;
    validateRetention(config.processedRetentionMs);
  }

  async ensureTables(): Promise<void> {
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS "${this.table}" (
          topology_id TEXT NOT NULL,
          stage_id TEXT NOT NULL,
          partition INTEGER NOT NULL,
          owner_id TEXT,
          epoch BIGINT NOT NULL DEFAULT 0,
          lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
          state JSONB NOT NULL DEFAULT '{}'::jsonb,
          source_offset TEXT,
          checkpoint_id TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (topology_id, stage_id, partition)
        )
      `),
    );
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS "${this.processedTable}" (
          topology_id TEXT NOT NULL,
          stage_id TEXT NOT NULL,
          partition INTEGER NOT NULL,
          source_id TEXT NOT NULL,
          processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (topology_id, stage_id, partition, source_id)
        )
      `),
    );
    await this.db.execute(
      sql.raw(
        `CREATE INDEX IF NOT EXISTS "${this.processedTable}_processed_at" ON "${this.processedTable}" (processed_at)`,
      ),
    );
  }

  async acquire(params: {
    scope: StatePartitionScope;
    ownerId: TopologyInstanceId;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    validateLeaseMs(params.leaseMs);
    const table = sql.raw(`"${this.table}"`);
    const rows = await execRaw(
      this.db,
      sql`
        INSERT INTO ${table}
          (topology_id, stage_id, partition, owner_id, epoch, lease_expires_at)
        VALUES
          (${params.scope.topologyId}, ${params.scope.stageId}, ${params.scope.partition}, ${params.ownerId}, 1, NOW() + (${params.leaseMs} * INTERVAL '1 millisecond'))
        ON CONFLICT (topology_id, stage_id, partition) DO UPDATE SET
          owner_id = EXCLUDED.owner_id,
          epoch = CASE
            WHEN ${table}.owner_id = EXCLUDED.owner_id
              AND ${table}.lease_expires_at > NOW()
            THEN ${table}.epoch
            ELSE ${table}.epoch + 1
          END,
          lease_expires_at = EXCLUDED.lease_expires_at,
          updated_at = NOW()
        WHERE ${table}.owner_id = EXCLUDED.owner_id
           OR ${table}.lease_expires_at <= NOW()
        RETURNING owner_id, epoch, lease_expires_at
      `,
    );
    return rows[0] ? leaseFromRow(params.scope, rows[0]) : undefined;
  }

  async renew(params: {
    lease: StatePartitionLease;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    validateLeaseMs(params.leaseMs);
    const rows = await execRaw(
      this.db,
      sql`
        UPDATE ${sql.raw(`"${this.table}"`)}
        SET lease_expires_at = NOW() + (${params.leaseMs} * INTERVAL '1 millisecond'),
            updated_at = NOW()
        WHERE topology_id = ${params.lease.scope.topologyId}
          AND stage_id = ${params.lease.scope.stageId}
          AND partition = ${params.lease.scope.partition}
          AND owner_id = ${params.lease.ownerId}
          AND epoch = ${params.lease.epoch}
          AND lease_expires_at > NOW()
        RETURNING owner_id, epoch, lease_expires_at
      `,
    );
    return rows[0] ? leaseFromRow(params.lease.scope, rows[0]) : undefined;
  }

  async load(lease: StatePartitionLease): Promise<PartitionStateSnapshot<V> | undefined> {
    const rows = await execRaw(
      this.db,
      sql`
        SELECT state, source_offset, checkpoint_id
        FROM ${sql.raw(`"${this.table}"`)}
        WHERE topology_id = ${lease.scope.topologyId}
          AND stage_id = ${lease.scope.stageId}
          AND partition = ${lease.scope.partition}
          AND owner_id = ${lease.ownerId}
          AND epoch = ${lease.epoch}
          AND lease_expires_at > NOW()
      `,
    );
    if (!rows[0]) return undefined;
    const rawState = parseJsonObject(rows[0].state);
    const values = new Map<string, V>();
    for (const [key, value] of Object.entries(rawState)) {
      values.set(key, this.codec.decode(value));
    }
    return {
      values,
      ...(rows[0].source_offset == null ? {} : { sourceOffset: String(rows[0].source_offset) }),
      ...(rows[0].checkpoint_id == null
        ? {}
        : { checkpointId: StateCheckpointId(String(rows[0].checkpoint_id)) }),
    };
  }

  async isProcessed(params: {
    lease: StatePartitionLease;
    sourceId: SourceRecordId;
  }): Promise<boolean> {
    const rows = await execRaw(
      this.db,
      sql`
        SELECT EXISTS (
          SELECT 1
          FROM ${sql.raw(`"${this.table}"`)} s
          JOIN ${sql.raw(`"${this.processedTable}"`)} p
            ON p.topology_id = s.topology_id
           AND p.stage_id = s.stage_id
           AND p.partition = s.partition
          WHERE s.topology_id = ${params.lease.scope.topologyId}
            AND s.stage_id = ${params.lease.scope.stageId}
            AND s.partition = ${params.lease.scope.partition}
            AND s.owner_id = ${params.lease.ownerId}
            AND s.epoch = ${params.lease.epoch}
            AND s.lease_expires_at > NOW()
            AND p.source_id = ${params.sourceId}
        ) AS processed
      `,
    );
    return rows[0]?.processed === true;
  }

  transaction<A>(work: (transaction: DrizzleDb) => Promise<A>): Promise<A> {
    return this.db.transaction((transaction) => work(transaction as DrizzleDb));
  }

  commit(commit: PartitionStateCommit<V>): Promise<PartitionCommitResult> {
    return this.transaction((transaction) => this.commitInTransaction(transaction, commit));
  }

  async commitInTransaction(
    transaction: DrizzleDb,
    commit: PartitionStateCommit<V>,
  ): Promise<PartitionCommitResult> {
    const owned = await execRaw(
      transaction,
      sql`
        SELECT 1
        FROM ${sql.raw(`"${this.table}"`)}
        WHERE topology_id = ${commit.lease.scope.topologyId}
          AND stage_id = ${commit.lease.scope.stageId}
          AND partition = ${commit.lease.scope.partition}
          AND owner_id = ${commit.lease.ownerId}
          AND epoch = ${commit.lease.epoch}
          AND lease_expires_at > NOW()
        FOR UPDATE
      `,
    );
    if (!owned[0]) return "fenced";

    if (commit.sourceId !== undefined) {
      const inserted = await execRaw(
        transaction,
        sql`
          INSERT INTO ${sql.raw(`"${this.processedTable}"`)}
            (topology_id, stage_id, partition, source_id)
          VALUES
            (${commit.lease.scope.topologyId}, ${commit.lease.scope.stageId}, ${commit.lease.scope.partition}, ${commit.sourceId})
          ON CONFLICT DO NOTHING
          RETURNING source_id
        `,
      );
      if (!inserted[0]) return "duplicate";
    }

    let stateExpression: SQL = sql`state`;
    for (const mutation of commit.mutations) {
      if (mutation.type === "delete") {
        stateExpression = sql`(${stateExpression} - ${mutation.key})`;
      } else {
        const encoded = JSON.stringify(this.codec.encode(mutation.value));
        if (encoded === undefined)
          throw new TypeError(`State value for ${mutation.key} is not JSON`);
        stateExpression = sql`jsonb_set(${stateExpression}, ARRAY[${mutation.key}]::text[], ${encoded}::jsonb, true)`;
      }
    }

    await transaction.execute(sql`
      UPDATE ${sql.raw(`"${this.table}"`)}
      SET state = ${stateExpression},
          source_offset = COALESCE(${commit.sourceOffset ?? null}, source_offset),
          checkpoint_id = COALESCE(${commit.checkpointId ?? null}, checkpoint_id),
          updated_at = NOW()
      WHERE topology_id = ${commit.lease.scope.topologyId}
        AND stage_id = ${commit.lease.scope.stageId}
        AND partition = ${commit.lease.scope.partition}
    `);

    if (this.processedRetentionMs !== undefined) {
      await transaction.execute(sql`
        DELETE FROM ${sql.raw(`"${this.processedTable}"`)}
        WHERE topology_id = ${commit.lease.scope.topologyId}
          AND stage_id = ${commit.lease.scope.stageId}
          AND partition = ${commit.lease.scope.partition}
          AND processed_at < NOW() - (${this.processedRetentionMs} * INTERVAL '1 millisecond')
      `);
    }
    return "committed";
  }

  async release(lease: StatePartitionLease): Promise<boolean> {
    const rows = await execRaw(
      this.db,
      sql`
        UPDATE ${sql.raw(`"${this.table}"`)}
        SET owner_id = NULL, lease_expires_at = to_timestamp(0), updated_at = NOW()
        WHERE topology_id = ${lease.scope.topologyId}
          AND stage_id = ${lease.scope.stageId}
          AND partition = ${lease.scope.partition}
          AND owner_id = ${lease.ownerId}
          AND epoch = ${lease.epoch}
        RETURNING 1
      `,
    );
    return Boolean(rows[0]);
  }
}

function identifier(value: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
    throw new TypeError(`Invalid PostgreSQL identifier: ${value}`);
  }
  return value;
}

function leaseFromRow(scope: StatePartitionScope, row: any): StatePartitionLease {
  return {
    scope,
    ownerId: TopologyInstanceId(String(row.owner_id)),
    epoch: LeaseEpoch(Number(row.epoch)),
    expiresAt: new Date(row.lease_expires_at).getTime(),
  };
}

function parseJsonObject(value: unknown): Record<string, unknown> {
  if (typeof value === "string") return JSON.parse(value) as Record<string, unknown>;
  if (value && typeof value === "object") return value as Record<string, unknown>;
  return {};
}

function validateLeaseMs(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1) {
    throw new RangeError(`leaseMs must be a positive safe integer, got ${leaseMs}`);
  }
}

function validateRetention(retentionMs: number | undefined): void {
  if (retentionMs !== undefined && (!Number.isSafeInteger(retentionMs) || retentionMs < 1)) {
    throw new RangeError(
      `processedRetentionMs must be a positive safe integer, got ${retentionMs}`,
    );
  }
}
