import { expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { run } from "@spilne/perfect-core";
import { execRaw, type DrizzleDb } from "../src/lib/drizzle-db";
import { PgRef } from "../src/lib/pg-ref";

function database(result: unknown): DrizzleDb {
  return { execute: async () => result } as unknown as DrizzleDb;
}

for (const wrap of [
  (rows: Record<string, unknown>[]) => rows,
  (rows: Record<string, unknown>[]) => ({ rows, rowCount: rows.length }),
]) {
  test("normalizes driver results for raw queries and PgRef", async () => {
    const row = { value: "42" };
    const db = database(wrap([row]));
    expect(await execRaw(db, sql`SELECT value`)).toEqual([row]);
    const ref = await PgRef.make<number>({ db, name: "answer", initial: 0 });
    expect(await run(ref.get.orDie())).toBe(42);
    expect(await execRaw(database(wrap([])), sql`DELETE FROM test`)).toEqual([]);
  });
}

test("rejects malformed driver results instead of claiming they are row arrays", async () => {
  for (const result of [null, {}, { rows: null }, { rows: [null] }, [42], [[1, 2]]]) {
    await expect(execRaw(database(result), sql`SELECT value`)).rejects.toThrow(TypeError);
  }
});
