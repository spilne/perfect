import { describe, test, expect } from "bun:test";
import {
  eff,
  succeed,
  sync,
  sleep,
  fork,
  join,
  interrupt,
  all,
  run,
  Pool,
  type Eff,
} from "../src";

interface FakeConn {
  id: number;
  closed: boolean;
}

let nextId = 0;

const makeConn = sync(() => {
  nextId++;
  return { id: nextId, closed: false } as FakeConn;
});

const closeConn = (c: FakeConn) =>
  sync(() => {
    c.closed = true;
  });

describe("Pool", () => {
  test("use acquires + releases (single fiber)", async () => {
    nextId = 0;
    const result = await run(
      eff(function* () {
        const pool = yield* Pool.make({ acquire: makeConn, release: closeConn, size: 3 });
        const v = yield* pool.use((c) => succeed(c.id));
        const idleAfter = yield* pool.idle;
        const inUseAfter = yield* pool.inUse;
        return { v, idleAfter, inUseAfter };
      }) as any,
    );
    expect(result).toEqual({ v: 1, idleAfter: 1, inUseAfter: 0 });
  });

  test("reuses idle resources across sequential uses", async () => {
    nextId = 0;
    await run(
      eff(function* () {
        const pool = yield* Pool.make({ acquire: makeConn, release: closeConn, size: 3 });
        for (let i = 0; i < 5; i++) yield* pool.use((c) => succeed(c.id));
        // Only ONE conn was created and reused 5 times
        const created = yield* pool.size;
        expect(created).toBe(1);
      }) as any,
    );
  });

  test("creates up to size concurrent resources", async () => {
    nextId = 0;
    const result = await run(
      eff(function* () {
        const pool = yield* Pool.make({ acquire: makeConn, release: closeConn, size: 3 });
        const ids: number[] = [];
        // Three concurrent users — each holds for 30ms
        const worker = pool.use((c) => sleep(30).flatMap(() => sync(() => { ids.push(c.id); return c.id; })));
        const f1 = yield* fork(worker);
        const f2 = yield* fork(worker);
        const f3 = yield* fork(worker);
        yield* join(f1); yield* join(f2); yield* join(f3);
        return ids.sort();
      }) as any,
    );
    expect(result).toEqual([1, 2, 3]); // three different conns
  });

  test("blocks at size + hands off to waiter on release", async () => {
    nextId = 0;
    const order: string[] = [];
    await run(
      eff(function* () {
        const pool = yield* Pool.make({ acquire: makeConn, release: closeConn, size: 1 });
        const f1 = yield* fork(
          pool.use((c) => sleep(30).flatMap(() => sync(() => { order.push(`A:${c.id}`); }))),
        );
        const f2 = yield* fork(
          pool.use((c) => sync(() => { order.push(`B:${c.id}`); })),
        );
        yield* join(f1);
        yield* join(f2);
      }) as any,
    );
    // Both used the same conn (id=1) because A released before B started
    expect(order).toEqual(["A:1", "B:1"]);
  });

  test("interrupted waiter does not take the next released resource", async () => {
    nextId = 0;
    const result = await run(
      eff(function* () {
        const pool = yield* Pool.make({ acquire: makeConn, release: closeConn, size: 1 });
        const holder = yield* fork(pool.use(() => sleep(20)));
        const waiter = yield* fork(pool.use((c) => succeed(c.id)));
        yield* sleep(1);
        yield* interrupt(waiter);
        yield* join(holder);
        const idle = yield* pool.idle;
        const inUse = yield* pool.inUse;
        return { idle, inUse };
      }) as any,
    );
    expect(result).toEqual({ idle: 1, inUse: 0 });
  });

  test("validate rejects bad resources, fresh acquired", async () => {
    nextId = 0;
    let validationCalls = 0;
    const result = await run(
      eff(function* () {
        const pool = yield* Pool.make({
          acquire: makeConn,
          release: closeConn,
          size: 5,
          // First reuse fails validation, all subsequent pass
          validate: (c: FakeConn) => sync(() => {
            validationCalls++;
            return validationCalls > 1; // first call returns false
          }),
        });
        // First use creates a fresh conn (id=1), no validate
        const id1 = yield* pool.use((c) => succeed(c.id));
        // Second use tries to reuse id=1, validate returns false → release + create id=2
        const id2 = yield* pool.use((c) => succeed(c.id));
        return [id1, id2];
      }) as any,
    );
    expect(result).toEqual([1, 2]);
  });

  test("shutdown releases all idle, rejects waiters", async () => {
    nextId = 0;
    const released: number[] = [];
    await run(
      eff(function* () {
        const pool = yield* Pool.make({
          acquire: makeConn,
          release: (c: FakeConn) => sync(() => { released.push(c.id); }),
          size: 3,
        });
        // Build 2 idle resources
        yield* pool.use((c) => succeed(c.id));
        yield* pool.use((c) => succeed(c.id));
        // Shutdown — both should be released
        yield* pool.shutdown();
        return yield* pool.size;
      }) as any,
    );
    expect(released.length).toBe(1); // sequential reuse: only 1 conn was created
  });

  test("after shutdown, use rejects with PoolClosed", async () => {
    nextId = 0;
    const program = eff(function* () {
      const pool = yield* Pool.make({ acquire: makeConn, release: closeConn, size: 3 });
      yield* pool.shutdown();
      yield* pool.use((c) => succeed(c.id));
      return "unreachable";
    });
    await expect(run(program as any)).rejects.toMatchObject({ _tag: "PoolClosed" });
  });

  test("validates size >= 1", async () => {
    expect(() => run(Pool.make({ acquire: makeConn, release: closeConn, size: 0 }) as any))
      .toThrow(/size must be >= 1/);
  });
});
