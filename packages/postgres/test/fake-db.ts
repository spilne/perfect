// FakeDb — in-memory DrizzleDb stand-in for unit tests.
//
// Renders every incoming drizzle SQL object to text+params via PgDialect
// and hands it to a scripted responder. Transactions run the callback
// against the same fake (no isolation — these are unit tests of SQL shape
// and driver-adjacent logic, not of Postgres semantics).

import { PgDialect } from "drizzle-orm/pg-core";
import type { DrizzleDb } from "../src/lib/drizzle-db";

const dialect = new PgDialect();

export type ExecutedQuery = { sql: string; params: unknown[] };
export type Responder = (sql: string, params: unknown[]) => unknown[] | undefined;

export class FakeDb {
  readonly queries: ExecutedQuery[] = [];

  constructor(private readonly responder: Responder = () => []) {}

  async execute(query: unknown): Promise<unknown[]> {
    const { sql, params } = dialect.sqlToQuery(query as any);
    this.queries.push({ sql, params });
    return this.responder(sql, params) ?? [];
  }

  async transaction<T>(fn: (tx: unknown) => Promise<T>): Promise<T> {
    return fn(this);
  }

  /** All executed SQL joined — convenient for substring assertions. */
  get allSql(): string {
    return this.queries.map((q) => q.sql).join("\n");
  }
}

export function fakeDb(responder?: Responder): { db: DrizzleDb; fake: FakeDb } {
  const fake = new FakeDb(responder);
  return { db: fake as unknown as DrizzleDb, fake };
}
