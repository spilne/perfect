import { describe, expect, test } from "bun:test";
import {
  Pool,
  Queue,
  Semaphore,
  async,
  run,
  runFiber,
  sleep,
  succeed,
  sync,
  timeoutOption,
} from "../src";
import { Deque } from "../src/internal/deque";
import { Waiter, WaiterList } from "../src/internal/waiter-list";

describe("Deque", () => {
  test("keeps order while it grows and wraps around", () => {
    const d = new Deque<number>(2);
    const expected: number[] = [];
    for (let i = 0; i < 100; i++) {
      d.push(i);
      expected.push(i);
      if (i % 3 === 0) expect(d.shift()).toBe(expected.shift());
    }
    expect(d.length).toBe(expected.length);
    expect(d.drain()).toEqual(expected);
    expect(d.length).toBe(0);
    expect(d.shift()).toBeUndefined();
  });

  test("insert puts a value at the given position", () => {
    const d = new Deque<string>();
    for (const v of ["a", "c", "d"]) d.push(v);
    d.shift();
    d.push("e");
    d.insert(0, "b");
    d.insert(4, "f");
    expect(d.drain()).toEqual(["b", "c", "d", "e", "f"]);
  });
});

describe("WaiterList", () => {
  class Item extends Waiter {
    constructor(readonly n: number) {
      super();
    }
  }

  test("removes from the front, the middle and the back", () => {
    const list = new WaiterList<Item>();
    const items = [1, 2, 3, 4].map((n) => list.push(new Item(n)));
    list.remove(items[1]!);
    list.remove(items[3]!);
    list.remove(items[3]!);
    expect(list.length).toBe(2);
    expect(list.shift()?.n).toBe(1);
    list.remove(items[0]!);
    expect(list.drain().map((item) => item.n)).toEqual([3]);
    expect(list.length).toBe(0);
  });
});

// Each of these used to leave one dead waiter behind per timed-out wait.
describe("a wait that times out leaves nothing behind", () => {
  test("Queue.take", async () => {
    const q = await run(Queue.bounded<number>(4));
    for (let i = 0; i < 1_000; i++) await run(timeoutOption(q.take(), 0).orDie());
    expect((q as any).takers.length).toBe(0);
    await run(q.offer(7).orDie());
    expect(await run(q.take().orDie())).toBe(7);
  });

  test("Queue.offer on a full queue", async () => {
    const q = await run(Queue.bounded<number>(1));
    await run(q.offer(0).orDie());
    for (let i = 0; i < 1_000; i++) await run(timeoutOption(q.offer(i), 0).orDie());
    expect((q as any).offerers.length).toBe(0);
    expect(await run(q.takeAll())).toEqual([0]);
  });

  test("Queue.awaitClose", async () => {
    const q = await run(Queue.unbounded<number>());
    for (let i = 0; i < 1_000; i++) await run(timeoutOption(q.awaitClose, 0));
    expect((q as any).closeWaiters.length).toBe(0);
  });

  test("Semaphore.acquire behind a waiter that stays", async () => {
    const sem = await run(Semaphore.make(2));
    await run(sem.acquire());
    // A big request waits at the front of the line, so the timed-out
    // waiters behind it are never at the head, where the old code cleaned up.
    const big = runFiber(sem.withPermits(5, succeed(1)));
    await run(sleep(1));
    for (let i = 0; i < 1_000; i++) await run(timeoutOption(sem.acquire(), 0));
    expect((sem as any).waiters.length).toBe(1);
    big.interrupt();
    await run(sleep(1));
    await run(sem.release());
    expect(await run(sem.available)).toBe(2);
  });

  test("Pool.use", async () => {
    const pool = await run(
      Pool.make({ size: 1, acquire: succeed({}), release: () => succeed(undefined) }),
    );
    // Hold the only resource until the end of the test. If it were given
    // back earlier, the pool would skip over the dead waiters and hide the
    // leak.
    const holder = runFiber(pool.use(() => async<void>(() => {})).orDie());
    await run(sleep(1));
    for (let i = 0; i < 200; i++)
      await run(
        timeoutOption(
          pool.use(() => succeed(1)),
          0,
        ).orDie(),
      );
    expect((pool as any).waiters.length).toBe(0);
    holder.interrupt();
  });
});

describe("Semaphore", () => {
  test("a big request that gives up lets the small ones behind it in", async () => {
    const sem = await run(Semaphore.make(2));
    await run(sem.acquire());
    const order: string[] = [];
    const big = run(
      timeoutOption(
        sem.withPermits(
          2,
          sync(() => order.push("big")),
        ),
        20,
      ),
    );
    const small = run(sem.withPermit(sync(() => order.push("small"))));
    await big;
    await small;
    expect(order).toEqual(["small"]);
  });
});
