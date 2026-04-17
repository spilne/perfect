import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  sleep,
  delay,
  timeoutFail,
  forkDaemon,
  awaitFiber,
  retry,
  provide,
  run,
  Cause,
  Clock,
  TestClock,
  Exit,
} from "../src";

// Helper: yield control so the fiber can advance to its next suspension point.
const tick = () => new Promise((r) => setTimeout(r, 0));

describe("TestClock — basics", () => {
  test("now() reflects the virtual clock, not wall time", () => {
    const c = new TestClock();
    expect(c.now()).toBe(0);
    c.advance(123);
    expect(c.now()).toBe(123);
    c.advance(7);
    expect(c.now()).toBe(130);
  });

  test("setTime jumps absolutely; advance is relative", () => {
    const c = new TestClock(1000);
    expect(c.now()).toBe(1000);
    c.advance(50);
    expect(c.now()).toBe(1050);
    c.setTime(2000);
    expect(c.now()).toBe(2000);
    expect(() => c.setTime(1000)).toThrow(/cannot move time backwards/);
    expect(() => c.advance(-1)).toThrow(/non-negative/);
  });

  test("real clock is the default — sleep without provide still works", async () => {
    const start = Date.now();
    await run(sleep(20));
    expect(Date.now() - start).toBeGreaterThanOrEqual(15);
  });
});

describe("TestClock — sleep", () => {
  test("sleep suspends until advance crosses its deadline", async () => {
    const c = new TestClock();
    let landed = false;

    const program = provide(
      sleep(100).flatMap(() =>
        sync(() => {
          landed = true;
          return "done";
        }),
      ),
      Clock,
      c,
    );

    // run() returns a Promise; the fiber is alive but suspended on the test clock.
    const promise = run(program as any);
    await tick();
    expect(landed).toBe(false);
    expect(c.pendingCount).toBe(1);

    c.advance(99);
    await tick();
    expect(landed).toBe(false);

    c.advance(1);
    expect(await promise).toBe("done");
    expect(landed).toBe(true);
    expect(c.pendingCount).toBe(0);
  });

  test("multiple concurrent sleeps fire in deadline order on a single advance", async () => {
    const c = new TestClock();
    const order: number[] = [];

    // Daemon fibers — survive after the parent finishes registering them.
    const program = provide(
      forkDaemon(
        sleep(300).flatMap(() =>
          sync(() => {
            order.push(3);
          }),
        ),
      )
        .flatMap(() =>
          forkDaemon(
            sleep(100).flatMap(() =>
              sync(() => {
                order.push(1);
              }),
            ),
          ),
        )
        .flatMap(() =>
          forkDaemon(
            sleep(200).flatMap(() =>
              sync(() => {
                order.push(2);
              }),
            ),
          ),
        ),
      Clock,
      c,
    );

    await run(program as any);
    await tick();
    expect(c.pendingCount).toBe(3);
    expect(c.pendingDeadlines()).toEqual([100, 200, 300]);

    c.advance(500);
    await tick();
    expect(order).toEqual([1, 2, 3]);
  });
});

describe("TestClock — timeouts", () => {
  test("timeoutFail fires deterministically when the inner work never completes in time", async () => {
    const c = new TestClock();
    const program = provide(
      timeoutFail(
        sleep(1000).flatMap(() => succeed("done")),
        50,
        () => "TIMEOUT" as const,
      ),
      Clock,
      c,
    );

    const promise = run(program as any);
    await tick();
    // Two sleeps registered: the inner work (1000) and the timeout race (50)
    expect(c.pendingCount).toBe(2);

    c.advance(50);
    await expect(promise).rejects.toBe("TIMEOUT");
  });

  test("timeoutFail does NOT fire when work finishes first", async () => {
    const c = new TestClock();
    const program = provide(
      timeoutFail(
        sleep(10).flatMap(() => succeed("done")),
        1000,
        () => "TIMEOUT" as const,
      ),
      Clock,
      c,
    );

    const promise = run(program as any);
    await tick();
    c.advance(10); // crosses inner deadline first
    expect(await promise).toBe("done");
  });
});

describe("TestClock — delay", () => {
  test("delay routes through the test clock", async () => {
    const c = new TestClock();
    const program = provide(delay(succeed(42), 200), Clock, c);

    const promise = run(program as any);
    await tick();
    expect(c.pendingCount).toBe(1);

    c.advance(199);
    await tick();
    expect(c.pendingCount).toBe(1); // still pending — we haven't crossed 200

    c.advance(1);
    expect(await promise).toBe(42);
  });
});

describe("TestClock — retry", () => {
  // Drain the event loop enough for setImmediate + microtask chains to flush.
  const drain = async () => {
    for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
  };

  test("retry with delay does not block real time", async () => {
    const c = new TestClock();
    let attempts = 0;

    const flaky: any = sync(() => ++attempts).flatMap((n: number) =>
      n < 3 ? fail("not-yet") : succeed("ok"),
    );

    const program = provide(retry(flaky, { times: 5, delay: 1000, backoff: "fixed" }), Clock, c);

    const promise = run(program as any);
    await drain();
    expect(attempts).toBe(1);

    c.advance(1000);
    await drain();
    expect(attempts).toBe(2);

    c.advance(1000);
    expect(await promise).toBe("ok");
    expect(attempts).toBe(3);
  });

  test("retry exhausts all attempts and surfaces the original failure", async () => {
    const c = new TestClock();
    let attempts = 0;
    const alwaysFails: any = sync(() => ++attempts).flatMap(() => fail("nope"));

    const program = provide(
      retry(alwaysFails, { times: 2, delay: 100, backoff: "fixed" }),
      Clock,
      c,
    );

    const promise = run(program as any);
    await drain();
    c.advance(100);
    await drain();
    c.advance(100);
    await expect(promise).rejects.toBe("nope");
    expect(attempts).toBe(3);
  });
});
