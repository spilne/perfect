import { describe, expect, test } from "bun:test";
import {
  async,
  die,
  ensuring,
  fail,
  Pool,
  runFiber,
  runSync,
  Semaphore,
  succeed,
  suspend,
  sync,
  SyncScheduler,
} from "../src";

describe("async registration failures", () => {
  test("registration defects settle the fiber and run finalizers", () => {
    const scheduler = new SyncScheduler();
    const error = new Error("registration failed");
    let finalized = 0;
    let lateResume = () => {};
    const fiber = runFiber(
      ensuring(
        async<number>((resume) => {
          lateResume = () => resume(succeed(42));
          throw error;
        }),
        sync(() => {
          finalized++;
        }),
      ),
      scheduler,
    );

    expect(() => scheduler.flush()).not.toThrow();
    expect(fiber.result).toEqual({ ok: false, cause: { _tag: "Die", defect: error } });
    expect(finalized).toBe(1);
    lateResume();
    scheduler.flush();
    expect(fiber.result).toEqual({ ok: false, cause: { _tag: "Die", defect: error } });
    expect(finalized).toBe(1);
  });

  test("synchronous completion wins over later resumes and registration throws", () => {
    const scheduler = new SyncScheduler();
    let finalized = 0;
    const fiber = runFiber(
      ensuring(
        async<number>((resume) => {
          resume(succeed(1));
          resume(succeed(2));
          throw new Error("after completion");
        }),
        sync(() => {
          finalized++;
        }),
      ),
      scheduler,
    );

    expect(() => scheduler.flush()).not.toThrow();
    expect(fiber.result).toEqual({ ok: true, value: 1 });
    expect(finalized).toBe(1);
  });
});

describe("pool acquisition cleanup", () => {
  for (const outcome of ["failure", "defect", "interrupt"] as const) {
    test(`${outcome} restores capacity and wakes an existing waiter`, () => {
      const scheduler = new SyncScheduler();
      let attempts = 0;
      let rejectAcquire = () => {};
      const pool = runSync(
        Pool.make({
          size: 1,
          acquire: suspend(() => {
            attempts++;
            if (attempts > 1) return succeed(42);
            return async<number, string>((resume) => {
              rejectAcquire = () =>
                resume(outcome === "defect" ? die("broken") : fail("unavailable"));
            });
          }),
          release: () => succeed(undefined),
        }),
      );
      const first = runFiber(pool.use(succeed) as any, scheduler);
      const second = runFiber(pool.use(succeed) as any, scheduler);
      scheduler.flush();
      expect(first.status).toBe("suspended");
      expect(second.status).toBe("suspended");

      if (outcome === "interrupt") first.interrupt();
      else rejectAcquire();
      scheduler.flush();

      expect(first.result?.ok).toBe(false);
      expect(second.result).toEqual({ ok: true, value: 42 });
      expect(attempts).toBe(2);
      expect(runSync(pool.inUse as any)).toBe(0);
      expect(runSync(pool.idle as any)).toBe(1);
    });
  }

  test("a throwing use callback returns the resource for reuse and shutdown", () => {
    const scheduler = new SyncScheduler();
    const error = new Error("body construction failed");
    const released: number[] = [];
    let acquisitions = 0;
    const pool = runSync(
      Pool.make({
        size: 1,
        acquire: sync(() => ++acquisitions),
        release: (resource) =>
          sync(() => {
            released.push(resource);
          }),
      }),
    );
    const first = runFiber(
      pool.use(() => {
        throw error;
      }) as any,
      scheduler,
    );
    scheduler.flush();

    expect(first.result).toEqual({ ok: false, cause: { _tag: "Die", defect: error } });
    expect(runSync(pool.inUse)).toBe(0);
    expect(runSync(pool.idle)).toBe(1);
    expect(runSync(pool.use(succeed) as any)).toBe(1);
    expect(acquisitions).toBe(1);
    runSync(pool.shutdown());
    expect(released).toEqual([1]);
  });
});

describe("semaphore waiter cancellation", () => {
  test("canceling the head grants available permits to the next waiter", () => {
    const scheduler = new SyncScheduler();
    const semaphore = runSync(Semaphore.make(2));
    runSync(semaphore.acquire());
    const head = runFiber(semaphore.withPermits(2, succeed("head")), scheduler);
    const tail = runFiber(semaphore.withPermit(succeed("tail")), scheduler);
    scheduler.flush();
    expect(head.status).toBe("suspended");
    expect(tail.status).toBe("suspended");

    head.interrupt();
    scheduler.flush();

    expect(head.result?.ok).toBe(false);
    expect(tail.result).toEqual({ ok: true, value: "tail" });
    expect(runSync(semaphore.available)).toBe(1);
    runSync(semaphore.release());
    expect(runSync(semaphore.available)).toBe(2);
  });
});
