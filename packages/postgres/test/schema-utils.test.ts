import { describe, it, expect } from "bun:test";
import { pgTable, text, integer } from "drizzle-orm/pg-core";
import { ensureTable } from "../src/lib/schema-utils";
import { createQueueTable } from "../src/lib/pg-queue-schema";
import { topologyState } from "../src/lib/pg-state-schema";
import { fakeDb } from "./fake-db";

// ---------------------------------------------------------------------------
// ensureTable — DDL derived from Drizzle pgTable definitions (captured, not
// executed; Postgres-side behavior is covered in integration.test.ts)
// ---------------------------------------------------------------------------

describe("ensureTable — derives DDL from Drizzle pgTable", () => {
  it("creates a simple table with defaults and PRIMARY KEY", async () => {
    const table = pgTable("simple_test", {
      id: text("id").primaryKey(),
      name: text("name").notNull(),
      count: integer("count").notNull().default(0),
    });
    const { db, fake } = fakeDb();

    await ensureTable(db, table);

    const ddl = fake.queries[0]!.sql;
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS "simple_test"');
    expect(ddl).toContain('"id" text NOT NULL PRIMARY KEY');
    expect(ddl).toContain('"name" text NOT NULL');
    expect(ddl).toContain('"count" integer NOT NULL DEFAULT 0');
  });

  it("emits the queue table with SKIP LOCKED dequeue index", async () => {
    const { db, fake } = fakeDb();

    await ensureTable(db, createQueueTable("orders"));

    const ddl = fake.queries[0]!.sql;
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS "pgq_orders"');
    expect(ddl).toContain('"id" bigserial NOT NULL PRIMARY KEY');
    expect(ddl).toContain('"payload" jsonb NOT NULL');
    expect(ddl).toContain(`"status" text NOT NULL DEFAULT 'pending'`);
    expect(ddl).toContain('"visible_at" timestamp with time zone NOT NULL DEFAULT now()');

    const indexSql = fake.queries[1]!.sql;
    expect(indexSql).toContain('CREATE INDEX IF NOT EXISTS "pgq_orders_dequeue_idx"');
    expect(indexSql).toContain('("status", "visible_at")');
  });

  it("emits the topology state table with a composite primary key", async () => {
    const { db, fake } = fakeDb();

    await ensureTable(db, topologyState);

    const ddl = fake.queries[0]!.sql;
    expect(ddl).toContain('CREATE TABLE IF NOT EXISTS "topology_state"');
    expect(ddl).toContain('PRIMARY KEY ("key", "checkpoint")');
    expect(ddl).toContain(`"checkpoint" text NOT NULL DEFAULT 'live'`);

    const indexSql = fake.queries[1]!.sql;
    expect(indexSql).toContain('"idx_topology_state_checkpoint"');
  });
});
