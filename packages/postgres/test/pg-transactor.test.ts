import { describe, expect, it } from "bun:test";
import type { DrizzleDb } from "../src/lib/drizzle-db";
import { PgTransactor } from "../src/lib/pg-transactor";
import { FakeDb } from "./fake-db";

describe("PgTransactor", () => {
  const db = new FakeDb() as unknown as DrizzleDb;

  it("uses the kind pg by default", () => {
    expect(PgTransactor.make({ db }).kind).toBe("pg");
  });

  it("takes a custom kind for a second database", () => {
    expect(PgTransactor.make({ db, kind: "billing" }).kind).toBe("billing");
  });
});
