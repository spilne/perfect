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

/** Execute raw SQL and return rows as any[] (for extension/dynamic queries). */
export async function execRaw(db: DrizzleDb, query: SQL): Promise<any[]> {
  return (await db.execute(query)) as any[];
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
