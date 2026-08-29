// ---------------------------------------------------------------------------
// PgLeaderElection — Postgres advisory lock based leader election
//
// Implements @spilne/perfect-core/connect's LeaderElection contract. Advisory
// locks are session-scoped: acquire and release must run on the same
// connection, so use a dedicated (or single-connection) client rather
// than a large pool when leadership matters.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import type { LeaderElection } from "@spilne/perfect-core/connect";
import { type DrizzleDb, execRaw } from "./drizzle-db";

export interface PgLeaderElectionConfig {
  db: DrizzleDb;
  /** Advisory lock ID. Default: hash of "perfect-coordinator". */
  lockId?: number;
}

export class PgLeaderElection implements LeaderElection {
  private readonly db: DrizzleDb;
  private readonly lockId: number;

  constructor(config: PgLeaderElectionConfig) {
    this.db = config.db;
    this.lockId = config.lockId ?? hashToInt32("perfect-coordinator");
  }

  async tryAcquire(): Promise<boolean> {
    const [result] = await execRaw(
      this.db,
      sql`SELECT pg_try_advisory_lock(${this.lockId}) as acquired`,
    );
    return result?.acquired === true;
  }

  async release(): Promise<void> {
    await execRaw(this.db, sql`SELECT pg_advisory_unlock(${this.lockId})`);
  }
}

/** Stable string → int32 hash for advisory lock IDs. Exported for tests. */
export function hashToInt32(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0;
  }
  return hash;
}
