import { describe, it, expect } from "bun:test";
import { CheckpointName } from "@spilne/perfect-core/connect";
import { PgStateBackend } from "../src/lib/pg-state-backend";
import { fakeDb } from "./fake-db";

describe("PgStateBackend (fake db)", () => {
  it("get returns undefined when the key is absent", async () => {
    const { db } = fakeDb(() => []);
    const backend = new PgStateBackend({ db });
    expect(await backend.get("missing")).toBeUndefined();
  });

  it("put upserts into the live partition", async () => {
    const { db, fake } = fakeDb();
    const backend = new PgStateBackend({ db });

    await backend.put("counter", { n: 1 });

    expect(fake.allSql).toContain("INSERT INTO topology_state");
    expect(fake.allSql).toContain("'live'");
    expect(fake.allSql).toContain("ON CONFLICT (key, checkpoint) DO UPDATE");
  });

  it("escapes single quotes in keys and checkpoint names", async () => {
    const { db, fake } = fakeDb();
    const backend = new PgStateBackend({ db });

    await backend.put("o'brien", "x");
    await backend.checkpoint({ name: CheckpointName("cp'1") });

    expect(fake.allSql).toContain("o''brien");
    expect(fake.allSql).toContain("cp''1");
    expect(fake.allSql).not.toContain("o'brien'");
  });

  it("restore is a no-op when the checkpoint does not exist", async () => {
    const { db, fake } = fakeDb(() => []);
    const backend = new PgStateBackend({ db });

    await backend.restore({ name: CheckpointName("nope") });

    // Only the existence probe ran — live state was not deleted
    expect(fake.allSql).not.toContain("DELETE FROM topology_state WHERE checkpoint = 'live'");
  });
});
