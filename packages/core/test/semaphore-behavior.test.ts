import { describe, test, expect } from "bun:test";
import { fail, sleep, forkDaemon, interrupt, all, run, Semaphore, Ref } from "../src";

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

  test("interrupted acquire does not consume a later release", async () => {
    const program = Semaphore.make(0).flatMap((sem) =>
      forkDaemon(sem.acquire()).flatMap((waiter) =>
        sleep(1).flatMap(() =>
          interrupt(waiter).flatMap(() => sem.release().flatMap(() => sem.available)),
        ),
      ),
    );
    expect(await run(program)).toBe(1);
  });
});
