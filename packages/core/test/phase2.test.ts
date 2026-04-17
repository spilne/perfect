import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  suspend,
  sleep,
  fork,
  join,
  all,
  ensuring,
  run,
  runSync,
  Deferred,
  Queue,
  Semaphore,
  Ref,
  Schedule,
  retryWith,
  repeat,
} from "../src";

// ── suspend ────────────────────────────────────────────────────────

describe("suspend", () => {
  test("lazy effect creation", () => {
    let created = false;
    const eff = suspend(() => {
      created = true;
      return succeed(42);
    });
    expect(created).toBe(false);
    expect(runSync(eff)).toBe(42);
    expect(created).toBe(true);
  });

  test("recursive effects don't stack overflow", () => {
    function countdown(n: number): ReturnType<typeof succeed<number>> {
      if (n <= 0) return succeed(0);
      return suspend(() => countdown(n - 1)) as any;
    }
    expect(runSync(countdown(10_000))).toBe(0);
  });
});

// ── Deferred ───────────────────────────────────────────────────────

describe("Deferred", () => {
  test("succeed then await", async () => {
    const program = Deferred.make<number>().flatMap((d) => d.succeed(42).flatMap(() => d.await));
    expect(await run(program)).toBe(42);
  });

  test("await then succeed", async () => {
    const program = Deferred.make<number>().flatMap((d) =>
      fork(sleep(10).flatMap(() => d.succeed(99))).flatMap(() => d.await),
    );
    expect(await run(program)).toBe(99);
  });

  test("multiple awaiters", async () => {
    const program = Deferred.make<string>().flatMap((d) =>
      all([d.await, d.await, d.await])
        .parZip(
          sleep(10)
            .flatMap(() => d.succeed("hello"))
            .as(undefined),
        )
        .map(([results]) => results),
    );
    expect(await run(program)).toEqual(["hello", "hello", "hello"]);
  });

  test("fail then await", async () => {
    const program = Deferred.make<number, string>().flatMap((d) =>
      d.fail("boom").flatMap(() => d.await),
    );
    await expect(run(program)).rejects.toBe("boom");
  });

  test("succeed twice returns false", async () => {
    const program = Deferred.make<number>().flatMap((d) =>
      d.succeed(1).flatMap((first) => d.succeed(2).map((second) => [first, second])),
    );
    expect(await run(program)).toEqual([true, false]);
  });

  test("isDone", async () => {
    const program = Deferred.make<number>().flatMap((d) =>
      d.isDone.flatMap((before) =>
        d.succeed(1).flatMap(() => d.isDone.map((after) => [before, after])),
      ),
    );
    expect(await run(program)).toEqual([false, true]);
  });
});

// ── Queue ──────────────────────────────────────────────────────────

describe("Queue", () => {
  test("unbounded offer + take", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q
        .offer(1)
        .flatMap(() =>
          q.offer(2).flatMap(() => q.take().flatMap((a) => q.take().map((b) => [a, b]))),
        ),
    );
    expect(await run(program)).toEqual([1, 2]);
  });

  test("take blocks until offer", async () => {
    const program = Queue.unbounded<string>().flatMap((q) =>
      fork(sleep(20).flatMap(() => q.offer("delayed"))).flatMap(() => q.take()),
    );
    expect(await run(program)).toBe("delayed");
  });

  test("bounded queue blocks offer when full", async () => {
    const log: string[] = [];
    const program = Queue.bounded<number>(2).flatMap((q) =>
      q.offer(1).flatMap(() =>
        q.offer(2).flatMap(() =>
          // third offer should block (capacity=2)
          fork(q.offer(3).flatMap(() => sync(() => log.push("offered:3")))).flatMap(() =>
            sleep(20).flatMap(() =>
              q.take().flatMap((a) =>
                sleep(20).flatMap(() =>
                  q.take().flatMap((b) =>
                    q.take().map((c) => {
                      log.push(`taken:${a},${b},${c}`);
                      return [a, b, c];
                    }),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
    expect(await run(program)).toEqual([1, 2, 3]);
  });

  test("takeAll", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q.offer(1).flatMap(() => q.offer(2).flatMap(() => q.offer(3).flatMap(() => q.takeAll()))),
    );
    expect(await run(program)).toEqual([1, 2, 3]);
  });

  test("size", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q.offer(1).flatMap(() => q.offer(2).flatMap(() => q.size)),
    );
    expect(await run(program)).toBe(2);
  });
});

// ── Semaphore ──────────────────────────────────────────────────────

describe("Semaphore", () => {
  test("withPermit limits concurrency", async () => {
    const program = Semaphore.make(1).flatMap((sem) =>
      Ref.make(0).flatMap((maxConcurrent) =>
        Ref.make(0).flatMap((current) => {
          const task = sem.withPermit(
            current
              .update((n) => n + 1)
              .flatMap(() => current.get)
              .flatMap((n) => maxConcurrent.update((m) => Math.max(m, n)))
              .flatMap(() => sleep(10))
              .flatMap(() => current.update((n) => n - 1)),
          );
          return all(Array.from({ length: 5 }, () => task)).flatMap(() => maxConcurrent.get);
        }),
      ),
    );
    expect(await run(program)).toBe(1);
  });

  test("semaphore(3) allows 3 concurrent", async () => {
    const program = Semaphore.make(3).flatMap((sem) =>
      Ref.make(0).flatMap((maxConcurrent) =>
        Ref.make(0).flatMap((current) => {
          const task = sem.withPermit(
            current
              .update((n) => n + 1)
              .flatMap(() => current.get)
              .flatMap((n) => maxConcurrent.update((m) => Math.max(m, n)))
              .flatMap(() => sleep(20))
              .flatMap(() => current.update((n) => n - 1)),
          );
          return all(Array.from({ length: 10 }, () => task)).flatMap(() => maxConcurrent.get);
        }),
      ),
    );
    expect(await run(program)).toBeLessThanOrEqual(3);
  });

  test("withPermit releases on failure", async () => {
    const program = Semaphore.make(1).flatMap((sem) =>
      sem.withPermit(fail("oops")).catch(() => sem.available),
    );
    expect(await run(program)).toBe(1);
  });
});

// ── Schedule ───────────────────────────────────────────────────────

describe("Schedule", () => {
  test("recurs(3) retries 3 times", async () => {
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 4) throw new Error("nope");
      return "ok";
    });

    expect(await run(retryWith(flaky, Schedule.recurs(5)))).toBe("ok");
    expect(attempts).toBe(4);
  });

  test("recurs(2) gives up after 2 retries", async () => {
    let attempts = 0;
    const always_fail = sync(() => {
      attempts++;
      throw new Error("always");
    });

    await expect(run(retryWith(always_fail, Schedule.recurs(2)))).rejects.toBeInstanceOf(Error);
    expect(attempts).toBe(3); // initial + 2 retries
  });

  test("spaced adds delay between retries", async () => {
    let attempts = 0;
    const flaky = sync(() => {
      attempts++;
      if (attempts < 2) throw new Error("nope");
      return "ok";
    });

    const start = Date.now();
    await run(retryWith(flaky, Schedule.intersect(Schedule.recurs(3), Schedule.spaced(30))));
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(25);
  });

  test("exponential backoff", () => {
    const sched = Schedule.exponential(100, 2);
    const d1 = sched.step(null, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag === "Continue") {
      expect(d1.delay).toBe(100);
      const d2 = sched.step(null, d1.state);
      if (d2._tag === "Continue") {
        expect(d2.delay).toBe(200);
        const d3 = sched.step(null, d2.state);
        if (d3._tag === "Continue") expect(d3.delay).toBe(400);
      }
    }
  });

  test("intersect combines schedules", () => {
    const sched = Schedule.intersect(Schedule.recurs(2), Schedule.spaced(100));
    const d1 = sched.step(null, sched.initial);
    expect(d1._tag).toBe("Continue");
    if (d1._tag === "Continue") {
      expect(d1.delay).toBe(100);
      const d2 = sched.step(null, d1.state);
      if (d2._tag === "Continue") {
        const d3 = sched.step(null, d2.state);
        expect(d3._tag).toBe("Done"); // recurs(2) = 2 retries then stop
      }
    }
  });

  test("maxDelay caps delay", () => {
    const sched = Schedule.maxDelay(Schedule.exponential(100, 2), 300);
    let state = sched.initial;
    for (let i = 0; i < 5; i++) {
      const d = sched.step(null, state);
      if (d._tag === "Continue") {
        expect(d.delay).toBeLessThanOrEqual(300);
        state = d.state;
      }
    }
  });

  test("repeat runs effect multiple times", async () => {
    const program = Ref.make(0).flatMap((counter) =>
      repeat(
        counter.update((n) => n + 1),
        Schedule.recurs(4),
      ).flatMap(() => counter.get),
    );
    expect(await run(program)).toBe(5); // initial + 4 repeats
  });
});

// ── Combined ───────────────────────────────────────────────────────

describe("phase 2 combined", () => {
  test("producer/consumer with Queue + Semaphore", async () => {
    const program = Queue.bounded<number>(5).flatMap((q) =>
      Semaphore.make(2).flatMap((sem) =>
        Ref.make<number[]>([]).flatMap((results) => {
          const producer = all(Array.from({ length: 10 }, (_, i) => q.offer(i))).flatMap(() =>
            q.shutdown(),
          );

          const consumer = (function consume(): any {
            return q
              .take()
              .flatMap((n) => sem.withPermit(results.update((r) => [...r, n])))
              .flatMap(() => suspend(() => consume()))
              .catch(() => succeed(undefined)); // shutdown
          })();

          return fork(producer)
            .flatMap(() => fork(consumer))
            .flatMap(() => fork(consumer))
            .flatMap(() => sleep(200))
            .flatMap(() => results.get)
            .map((r) => r.sort((a, b) => a - b));
        }),
      ),
    );

    expect(await run(program)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  test("Deferred as coordination", async () => {
    const program = Deferred.make<void>().flatMap((gate) =>
      Ref.make("waiting").flatMap((state) => {
        const worker = gate.await.flatMap(() => state.set("started"));
        return fork(worker)
          .flatMap(() => sleep(20))
          .flatMap(() => state.get)
          .flatMap((before) =>
            gate
              .succeed(undefined)
              .flatMap(() => sleep(20).flatMap(() => state.get.map((after) => [before, after]))),
          );
      }),
    );
    expect(await run(program)).toEqual(["waiting", "started"]);
  });
});
