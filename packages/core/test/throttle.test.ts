// Throttle — thin blocking wrapper over a sliding-window RateLimiter.
// All timing routes through the Clock service, so a TestClock drives it
// deterministically (same pattern as clock-routing.test.ts).

import { describe, test, expect } from "bun:test";
import { run, provide, sync, Clock, TestClock, Throttle } from "../src";

const tick = () => new Promise((r) => setTimeout(r, 0));

async function makeThrottle(c: TestClock, permits: number, windowMs: number): Promise<any> {
  return run(provide(Throttle.make({ permits, windowMs }) as any, Clock, c) as any);
}

describe("Throttle", () => {
  test("tryAcquire grants up to `permits` per window, then refuses", async () => {
    const c = new TestClock();
    const t = await makeThrottle(c, 2, 1000);
    const tryAcquire = () => run(provide(t.tryAcquire, Clock, c) as any);

    expect(await tryAcquire()).toBe(true);
    expect(await tryAcquire()).toBe(true);
    expect(await tryAcquire()).toBe(false);

    c.advance(1001);
    expect(await tryAcquire()).toBe(true);
  });

  test("remaining reports permits left in the current window", async () => {
    const c = new TestClock();
    const t = await makeThrottle(c, 3, 1000);
    const remaining = () => run(provide(t.remaining, Clock, c) as any);
    const tryAcquire = () => run(provide(t.tryAcquire, Clock, c) as any);

    expect(await remaining()).toBe(3);
    await tryAcquire();
    expect(await remaining()).toBe(2);
    await tryAcquire();
    await tryAcquire();
    expect(await remaining()).toBe(0);

    c.advance(1001);
    expect(await remaining()).toBe(3);
  });

  test("nextSlotIn is 0 when free, else ms until the oldest permit expires", async () => {
    const c = new TestClock();
    const t = await makeThrottle(c, 1, 1000);
    const nextSlotIn = () => run(provide(t.nextSlotIn, Clock, c) as any);
    const tryAcquire = () => run(provide(t.tryAcquire, Clock, c) as any);

    expect(await nextSlotIn()).toBe(0);
    await tryAcquire(); // taken at t=0
    expect(await nextSlotIn()).toBe(1000);

    c.advance(400);
    expect(await nextSlotIn()).toBe(600);

    c.advance(601);
    expect(await nextSlotIn()).toBe(0);
  });

  test("acquire blocks on virtual time until a permit opens", async () => {
    const c = new TestClock();
    const t = await makeThrottle(c, 1, 1000);
    const tryAcquire = () => run(provide(t.tryAcquire, Clock, c) as any);

    expect(await tryAcquire()).toBe(true); // exhaust the window at t=0

    let acquired = false;
    const done = run(
      provide(
        (t.acquire as any).flatMap(() =>
          sync(() => {
            acquired = true;
          }),
        ),
        Clock,
        c,
      ) as any,
    );

    await tick();
    expect(acquired).toBe(false); // still blocked inside the window

    c.advance(1000); // oldest permit expires exactly at t=1000
    await done;
    expect(acquired).toBe(true);
  });

  test("withPermit runs the effect and consumes a permit", async () => {
    const c = new TestClock();
    const t = await makeThrottle(c, 2, 1000);
    const remaining = () => run(provide(t.remaining, Clock, c) as any);

    const result = await run(provide(t.withPermit(sync(() => "done")), Clock, c) as any);
    expect(result).toBe("done");
    expect(await remaining()).toBe(1);
  });

  test("withPermit blocks until a slot frees, then runs", async () => {
    const c = new TestClock();
    const t = await makeThrottle(c, 1, 500);

    expect(await run(provide(t.withPermit(sync(() => 1)), Clock, c) as any)).toBe(1);

    let ran = false;
    const done = run(
      provide(
        t.withPermit(
          sync(() => {
            ran = true;
            return 2;
          }),
        ),
        Clock,
        c,
      ) as any,
    );

    await tick();
    expect(ran).toBe(false);

    c.advance(500);
    expect(await done).toBe(2);
    expect(ran).toBe(true);
  });
});
