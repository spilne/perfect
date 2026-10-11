import { describe, expect, test } from "bun:test";
import {
  Cause,
  SyncScheduler,
  acquireRelease,
  fork,
  interrupt,
  run,
  runExit,
  scoped,
  sleep,
  succeed,
  sync,
  tryPromise,
} from "../src";

/** A promise that only ends when its signal aborts, like a slow fetch. */
const untilAborted = (signal: AbortSignal) =>
  new Promise<string>((_, reject) => {
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });

describe("tryPromise with an AbortSignal", () => {
  test("interrupting the effect aborts the signal", async () => {
    let seen: AbortSignal | undefined;
    const slow = tryPromise(
      (signal) => {
        seen = signal;
        return untilAborted(signal);
      },
      (error) => error,
    );
    const exit = await fork(slow)
      .flatMap((fiber) => sleep(5).flatMap(() => interrupt(fiber)))
      .runExit();
    expect(exit._tag).toBe("Success");
    expect(seen?.aborted).toBe(true);
  });

  test("a promise that finishes isn't aborted afterwards", async () => {
    let seen: AbortSignal | undefined;
    const value = await tryPromise(
      async (signal) => {
        seen = signal;
        return 42;
      },
      (error) => error,
    )
      .orDie()
      .run();
    expect(value).toBe(42);
    expect(seen?.aborted).toBe(false);
  });

  test("a promise that takes no signal works as before", async () => {
    expect(
      await tryPromise(
        async () => "plain",
        (e) => e,
      )
        .orDie()
        .run(),
    ).toBe("plain");
  });
});

describe("run and runExit with { signal }", () => {
  test("aborting interrupts the effect, and its finalizers run", async () => {
    const controller = new AbortController();
    const events: string[] = [];
    const program = scoped(
      acquireRelease(
        sync(() => events.push("open")),
        () => sync(() => void events.push("closed")),
      ).flatMap(() => sleep(10_000)),
    );
    const running = run(program, { signal: controller.signal });
    await new Promise((resolve) => setTimeout(resolve, 5));
    controller.abort();
    await expect(running).rejects.toThrow("Interrupted");
    expect(events).toEqual(["open", "closed"]);
  });

  test("runExit reports the interruption as an exit", async () => {
    const controller = new AbortController();
    const exit = runExit(sleep(10_000), { signal: controller.signal });
    controller.abort();
    const result = await exit;
    expect(result._tag === "Failure" && Cause.pretty(result.cause)).toBe("Interrupt");
  });

  test("an already aborted signal stops the effect before it does anything", async () => {
    let ran = false;
    const exit = await runExit(
      sleep(0).flatMap(() => sync(() => (ran = true))),
      { signal: AbortSignal.abort() },
    );
    expect(exit._tag).toBe("Failure");
    expect(ran).toBe(false);
  });

  test("an effect that finishes first is unaffected, and stops listening", async () => {
    const controller = new AbortController();
    expect(
      await run(
        sleep(1).map(() => "done"),
        { signal: controller.signal },
      ),
    ).toBe("done");
    controller.abort(); // nothing left to interrupt
    expect(await succeed(1).run({ signal: controller.signal })).toBe(1);
  });

  test("a scheduler can still be passed directly or as an option", async () => {
    expect(
      await run(
        succeed(1).map((n) => n + 1),
        new SyncScheduler(),
      ),
    ).toBe(2);
    expect(
      await run(
        succeed(1).map((n) => n + 2),
        { scheduler: new SyncScheduler() },
      ),
    ).toBe(3);
  });
});
