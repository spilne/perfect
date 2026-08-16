// ---------------------------------------------------------------------------
// PgStateBackend — StateBackend backed by a Postgres JSONB table
//
// Implements @perfect/core/connect's StateBackend<string, unknown>
// (Promise-based driver boundary — direct port from promin). Each
// key-value pair is a row; checkpoints copy all live rows to a named
// checkpoint partition in the same table.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { StateBackend, CheckpointName } from "@perfect/core/connect";
import type { DrizzleDb } from "./drizzle-db";
import { execRaw } from "./drizzle-db";
import { createTopologyStateTable, topologyState } from "./pg-state-schema";
import { ensureTable as ensureTableFromSchema } from "./schema-utils";

export interface PgStateBackendConfig {
  db: DrizzleDb;
  /** Table name. Default: "topology_state". */
  table?: string;
}

export class PgStateBackend implements StateBackend<string, unknown> {
  private readonly db: DrizzleDb;
  private readonly table: string;

  constructor(config: PgStateBackendConfig) {
    this.db = config.db;
    this.table = config.table ?? "topology_state";
  }

  /**
   * Get the Drizzle schema for the default topology state table.
   * Use this to include in your migration pipeline.
   *
   * @example
   * ```ts
   * // In your drizzle schema file:
   * export const topologyState = PgStateBackend.schema;
   *
   * // For a custom table name:
   * export const myState = PgStateBackend.schemaFor("my_topology_state");
   * ```
   */
  static readonly schema = topologyState;

  /** Get the Drizzle schema for a custom-named topology state table. */
  static schemaFor(tableName: string) {
    return createTopologyStateTable(tableName);
  }

  /** Create the state table if it doesn't exist. Derived from the Drizzle schema. */
  async ensureTable(): Promise<void> {
    const schema =
      this.table === "topology_state" ? topologyState : createTopologyStateTable(this.table);
    await ensureTableFromSchema(this.db, schema);
  }

  async get(key: string): Promise<unknown | undefined> {
    const rows = await execRaw(
      this.db,
      sql.raw(
        `SELECT value FROM ${this.table} WHERE key = '${this.esc(key)}' AND checkpoint = 'live'`,
      ),
    );
    if (rows.length === 0) return undefined;
    return rows[0].value;
  }

  async put(key: string, value: unknown): Promise<void> {
    const jsonValue = JSON.stringify(value);
    await this.db.execute(
      sql.raw(`
      INSERT INTO ${this.table} (key, value, checkpoint)
      VALUES ('${this.esc(key)}', '${this.esc(jsonValue)}'::jsonb, 'live')
      ON CONFLICT (key, checkpoint) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
    `),
    );
  }

  async delete(key: string): Promise<void> {
    await this.db.execute(
      sql.raw(`DELETE FROM ${this.table} WHERE key = '${this.esc(key)}' AND checkpoint = 'live'`),
    );
  }

  async keys(): Promise<string[]> {
    const rows = await execRaw(
      this.db,
      sql.raw(`SELECT key FROM ${this.table} WHERE checkpoint = 'live' ORDER BY key`),
    );
    return rows.map((r: { key: string }) => r.key);
  }

  async entries(): Promise<[string, unknown][]> {
    const rows = await execRaw(
      this.db,
      sql.raw(`SELECT key, value FROM ${this.table} WHERE checkpoint = 'live' ORDER BY key`),
    );
    return rows.map((r: { key: string; value: unknown }) => [r.key, r.value]);
  }

  async checkpoint(params: { name: CheckpointName }): Promise<void> {
    const name = this.esc(params.name);

    // Delete old checkpoint, then copy live state into checkpoint
    await this.db.execute(
      sql.raw(`
      DELETE FROM ${this.table} WHERE checkpoint = '${name}'
    `),
    );
    await this.db.execute(
      sql.raw(`
      INSERT INTO ${this.table} (key, value, checkpoint, updated_at)
      SELECT key, value, '${name}', now()
      FROM ${this.table}
      WHERE checkpoint = 'live'
    `),
    );
  }

  async restore(params: { name: CheckpointName }): Promise<void> {
    const name = this.esc(params.name);

    // Check if checkpoint exists
    const rows = await execRaw(
      this.db,
      sql.raw(`SELECT 1 FROM ${this.table} WHERE checkpoint = '${name}' LIMIT 1`),
    );
    if (rows.length === 0) return;

    // Replace live state with checkpoint
    await this.db.execute(sql.raw(`DELETE FROM ${this.table} WHERE checkpoint = 'live'`));
    await this.db.execute(
      sql.raw(`
      INSERT INTO ${this.table} (key, value, checkpoint, updated_at)
      SELECT key, value, 'live', now()
      FROM ${this.table}
      WHERE checkpoint = '${name}'
    `),
    );
  }

  async clear(): Promise<void> {
    await this.db.execute(sql.raw(`DELETE FROM ${this.table} WHERE checkpoint = 'live'`));
  }

  /** Escape single quotes to prevent SQL injection. */
  private esc(s: string): string {
    return s.replace(/'/g, "''");
  }
}
