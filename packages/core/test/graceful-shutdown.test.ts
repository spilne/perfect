import { describe, test, expect } from "bun:test";
import { createGracefulShutdown } from "../src/graceful-shutdown";

describe("GracefulShutdown", () => {
  test("run aborts the signal and awaits registered teardowns", async () => {
    const shutdown = createGracefulShutdown();
    const closed: string[] = [];

    shutdown.onShutdown(async () => {
      closed.push("producer");
    });
    shutdown.onShutdown(async () => {
      await new Promise((r) => setTimeout(r, 10));
      closed.push("client");
    });

    expect(shutdown.signal.aborted).toBe(false);
    expect(shutdown.isShuttingDown).toBe(false);

    await shutdown.run();

    expect(shutdown.signal.aborted).toBe(true);
    expect(shutdown.isShuttingDown).toBe(true);
    expect(closed.sort()).toEqual(["client", "producer"]);
  });

  test("run is idempotent — teardowns fire once", async () => {
    const shutdown = createGracefulShutdown();
    let calls = 0;
    shutdown.onShutdown(async () => {
      calls++;
    });

    const first = shutdown.run();
    const second = shutdown.run();
    expect(second).toBe(first);
    await Promise.all([first, second]);
    expect(calls).toBe(1);
  });

  test("a throwing closer does not skip the others or break idempotency", async () => {
    const shutdown = createGracefulShutdown();
    const closed: string[] = [];

    shutdown.onShutdown(() => {
      throw new Error("sync boom"); // synchronous throw
    });
    shutdown.onShutdown(async () => {
      throw new Error("async boom");
    });
    shutdown.onShutdown(async () => {
      closed.push("survivor");
    });

    await shutdown.run();
    expect(closed).toEqual(["survivor"]);
    await shutdown.run(); // still idempotent after failures
  });
});

describe("GracefulShutdown + Stream.interruptOn pairing", () => {
  test("shutdown.run() ends an in-flight stream gracefully and awaits teardowns", async () => {
    const { run, sync, Stream } = await import("../src");
    const shutdown = createGracefulShutdown();
    const torn: string[] = [];
    shutdown.onShutdown(async () => {
      torn.push("producer");
    });

    let produced = 0;
    const infinite = Stream.unfoldEffect(0, (n: number) =>
      sync(() => {
        produced++;
        return [n, n + 1] as [number, number];
      }),
    );

    const consumed = run(
      (infinite as any).rechunk(1).buffer(2).interruptOn(shutdown.signal).toArray(),
    );

    await new Promise((r) => setTimeout(r, 10));
    await shutdown.run();

    const result = (await consumed) as number[];
    // graceful DONE, not a failure — whatever was consumed is returned
    expect(Array.isArray(result)).toBe(true);
    expect(torn).toEqual(["producer"]);

    const after = produced;
    await new Promise((r) => setTimeout(r, 20));
    expect(produced).toBe(after); // source stopped
  });
});
