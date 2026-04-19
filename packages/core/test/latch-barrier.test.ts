import { describe, test, expect } from "bun:test";
import { eff, fork, join, sleep, run, runSync, Latch, Barrier } from "../src";

describe("Latch", () => {
  test("starts at given count", async () => {
    const latch = await run(Latch.make({ count: 3 }));
    expect(await run(latch.remaining)).toBe(3);
  });

  test("countDown decrements; await unblocks at 0", async () => {
    const program = eff(function* () {
      const latch = yield* Latch.make({ count: 2 });
      // fork a waiter
      const waiterFiber = yield* fork(latch.await.map(() => "released"));
      yield* sleep(5);
      yield* latch.countDown;
      // not yet released
      yield* sleep(5);
      yield* latch.countDown;
      // now released
      return yield* join(waiterFiber);
    });
    expect(await run(program as any)).toBe("released");
  });

  test("countDownBy(n) clamps at 0", async () => {
    const latch = await run(Latch.make({ count: 5 }));
    await run(latch.countDownBy(10));
    expect(await run(latch.remaining)).toBe(0);
  });

  test("count = 0 starts already-released", async () => {
    const latch = await run(Latch.make({ count: 0 }));
    // await returns immediately
    await run(latch.await);
    expect(await run(latch.remaining)).toBe(0);
  });

  test("multiple awaiters all release", async () => {
    const program = eff(function* () {
      const latch = yield* Latch.make({ count: 1 });
      const a = yield* fork(latch.await.map(() => "a"));
      const b = yield* fork(latch.await.map(() => "b"));
      const c = yield* fork(latch.await.map(() => "c"));
      yield* sleep(5);
      yield* latch.countDown;
      const ra = yield* join(a);
      const rb = yield* join(b);
      const rc = yield* join(c);
      return [ra, rb, rc];
    });
    expect(await run(program as any)).toEqual(["a", "b", "c"]);
  });

  test("validates count >= 0", () => {
    expect(() => runSync(Latch.make({ count: -1 }) as any)).toThrow(/count must be >= 0/);
  });
});

describe("Barrier", () => {
  test("await blocks until all parties arrive", async () => {
    const program = eff(function* () {
      const barrier = yield* Barrier.make({ parties: 3 });
      const order: string[] = [];

      const party = (name: string) =>
        eff(function* () {
          yield* sleep(Math.random() * 10);
          yield* barrier.await;
          order.push(name);
        });

      const a = yield* fork(party("a"));
      const b = yield* fork(party("b"));
      const c = yield* fork(party("c"));
      yield* join(a);
      yield* join(b);
      yield* join(c);
      return order;
    });
    const result = await run(program as any);
    expect(result.length).toBe(3);
    expect(result).toContain("a");
    expect(result).toContain("b");
    expect(result).toContain("c");
  });

  test("arrived counter tracks party arrivals", async () => {
    const program = eff(function* () {
      const barrier = yield* Barrier.make({ parties: 3 });
      // Fork two parties; third never arrives — arrived count = 2
      yield* fork(barrier.await);
      yield* fork(barrier.await);
      yield* sleep(20);
      return yield* barrier.arrived;
    });
    expect(await run(program as any)).toBe(2);
  });

  test("validates parties >= 1", () => {
    expect(() => runSync(Barrier.make({ parties: 0 }) as any)).toThrow(/parties must be >= 1/);
  });
});
