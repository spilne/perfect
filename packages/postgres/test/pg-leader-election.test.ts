import { describe, it, expect } from "bun:test";
import { PgLeaderElection, hashToInt32 } from "../src/lib/pg-leader-election";
import { fakeDb } from "./fake-db";

describe("hashToInt32", () => {
  it("is deterministic and int32-ranged", () => {
    const h1 = hashToInt32("perfect-coordinator");
    const h2 = hashToInt32("perfect-coordinator");
    expect(h1).toBe(h2);
    expect(Number.isInteger(h1)).toBe(true);
    expect(h1).toBeGreaterThanOrEqual(-2147483648);
    expect(h1).toBeLessThanOrEqual(2147483647);
  });

  it("differs for different inputs", () => {
    expect(hashToInt32("a")).not.toBe(hashToInt32("b"));
  });
});

describe("PgLeaderElection (fake db)", () => {
  it("tryAcquire issues pg_try_advisory_lock with the configured lock id", async () => {
    const { db, fake } = fakeDb((sql) => {
      if (sql.includes("pg_try_advisory_lock")) return [{ acquired: true }];
      return [];
    });
    const le = new PgLeaderElection({ db, lockId: 42 });

    expect(await le.tryAcquire()).toBe(true);
    expect(fake.queries[0]!.sql).toContain("pg_try_advisory_lock");
    expect(fake.queries[0]!.params).toEqual([42]);
  });

  it("tryAcquire returns false when the lock is held elsewhere", async () => {
    const { db } = fakeDb(() => [{ acquired: false }]);
    const le = new PgLeaderElection({ db, lockId: 42 });
    expect(await le.tryAcquire()).toBe(false);
  });

  it("release issues pg_advisory_unlock with the same lock id", async () => {
    const { db, fake } = fakeDb(() => [{ acquired: true }]);
    const le = new PgLeaderElection({ db, lockId: 42 });
    await le.tryAcquire();
    await le.release();
    expect(fake.queries[1]!.sql).toContain("pg_advisory_unlock");
    expect(fake.queries[1]!.params).toEqual([42]);
  });

  it("defaults the lock id to hashToInt32('perfect-coordinator')", async () => {
    const { db, fake } = fakeDb(() => [{ acquired: true }]);
    const le = new PgLeaderElection({ db });
    await le.tryAcquire();
    expect(fake.queries[0]!.params).toEqual([hashToInt32("perfect-coordinator")]);
  });
});
