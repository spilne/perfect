// ---------------------------------------------------------------------------
// DrizzleDb — shared type alias for Drizzle database instances
//
// Uses the base PgDatabase type so any Postgres driver works:
// postgres-js, bun:sql, node-postgres, neon, etc. `createPostgresDb` is the
// only postgres-js-bound entry point.
// ---------------------------------------------------------------------------

import { type SQL } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";
import postgres from "postgres";

/** Drizzle database instance — driver-agnostic (postgres-js, bun:sql, etc). */
export type DrizzleDb = PgDatabase<PgQueryResultHKT, Record<string, unknown>>;

function isRow(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function stringColumn(row: Record<string, unknown>, column: string): string {
  const value = row[column];
  if (typeof value !== "string") throw new TypeError(`Expected a string in column ${column}`);
  return value;
}

/** Normalize postgres-js arrays and node-postgres/Neon result objects. */
export async function execRaw(db: DrizzleDb, query: SQL): Promise<Record<string, unknown>[]> {
  const result: unknown = await db.execute(query);
  const rows: unknown = Array.isArray(result) ? result : isRow(result) ? result.rows : undefined;
  if (!Array.isArray(rows) || !rows.every(isRow)) {
    throw new TypeError("Postgres query did not return an array of row objects");
  }
  return rows;
}

/**
 * Open a `DrizzleDb` from a Postgres connection string (postgres-js
 * driver). The host owns the connection lifetime — for a long-lived
 * process just hold the returned db; the underlying pool closes with
 * the process.
 */
export function createPostgresDb(connectionString: string): DrizzleDb {
  return drizzle(postgres(connectionString));
}
