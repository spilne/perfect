import { describe, expect, test } from "bun:test";
import {
  Cause,
  cached,
  cachedBy,
  fail,
  forkDaemon,
  hedged,
  join,
  raceSuccess,
  retryWith,
  run,
  runExit,
  Schedule,
  sleep,
  succeed,
  sync,
  timeoutOption,
} from "../src";

describe("raceSuccess", () => {
  test("a fast failure does not win", async () => {
    const result = await run(
      raceSuccess([fail("fast failure"), sleep(5).flatMap(() => succeed("slow success"))]).orDie(),
    );
    expect(result).toBe("slow success");
  });

  test("fails with every failure when all fail", async () => {
    const exit = await runExit(raceSuccess([fail("a"), sleep(1).flatMap(() => fail("b"))]));
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.failures(exit.cause).sort()).toEqual(["a", "b"]);
  });
});

describe("hedged", () => {
  test("a replica that fails fast does not cancel the backup", async () => {
    let calls = 0;
    const flaky = sync(() => ++calls).flatMap((n) =>
      n === 1 ? fail("first try failed") : succeed(n),
    );
    expect(await run(hedged(flaky, { replicas: 2, staggerMs: 1 }).orDie())).toBe(2);
  });
});

describe("retryWith", () => {
  test("`while` decides which errors are retried", async () => {
    let attempts = 0;
    const eff = sync(() => ++attempts).flatMap((n) => fail(n < 3 ? "retry me" : "stop"));
    const exit = await runExit(
      retryWith(eff, Schedule.recurs(10), { while: (e) => e === "retry me" }),
    );
    expect(attempts).toBe(3);
    expect(exit._tag === "Failure" && Cause.firstFail(exit.cause)?.value).toBe("stop");
  });

  test("the fluent form passes `while` through", async () => {
    let attempts = 0;
    const eff = sync(() => ++attempts).flatMap(() => fail("never retry"));
    await runExit(eff.retryWith(Schedule.recurs(5), { while: () => false }));
    expect(attempts).toBe(1);
  });
});

describe("join under a timeout", () => {
  test("a join that gives up stops listening", async () => {
    // A daemon, so it keeps running after the run() that started it ends.
    const fiber = await run(forkDaemon(sleep(1_000)));
    for (let i = 0; i < 100; i++) await run(timeoutOption(join(fiber), 0));
    expect(((fiber as any).listeners ?? []).length).toBe(0);
    fiber.interrupt();
  });
});

describe("cached", () => {
  test("callers that arrive together share one run", async () => {
    let runs = 0;
    const value = cached(sleep(5).flatMap(() => sync(() => ++runs)));
    const results = await Promise.all([run(value), run(value), run(value)]);
    expect(results).toEqual([1, 1, 1]);
    expect(runs).toBe(1);
  });

  test("a leader that is interrupted does not fail the others", async () => {
    let runs = 0;
    const value = cached(sleep(10).flatMap(() => sync(() => ++runs)));
    const interrupted = runExit(timeoutOption(value, 1));
    const patient = run(value);
    await interrupted;
    expect(await patient).toBe(1);
  });
});

describe("cachedBy", () => {
  test("callers that arrive together share one run per key", async () => {
    let runs = 0;
    const cache = cachedBy((k: string) => sleep(5).flatMap(() => sync(() => `${k}${++runs}`)));
    const results = await Promise.all([
      run(cache.get("a")),
      run(cache.get("a")),
      run(cache.get("b")),
    ]);
    expect(results[0]).toBe(results[1]);
    expect(runs).toBe(2);
  });

  test("expired entries are swept as new ones are added", async () => {
    const cache = cachedBy((k: number) => succeed(k), { ttlMs: 1 });
    for (let i = 0; i < 64; i++) await run(cache.get(i));
    await run(sleep(5));
    for (let i = 64; i < 200; i++) await run(cache.get(i));
    expect(await run(cache.size)).toBeLessThan(200);
  });

  test("object keys without keyFn fail instead of colliding", async () => {
    const cache = cachedBy((k: { id: number }) => succeed(k.id));
    const exit = await runExit(cache.get({ id: 1 }));
    expect(exit._tag).toBe("Failure");
    const keyed = cachedBy((k: { id: number }) => succeed(k.id), { keyFn: (k) => String(k.id) });
    expect(await run(keyed.get({ id: 1 }))).toBe(1);
    expect(await run(keyed.get({ id: 2 }))).toBe(2);
  });
});

describe("effects read state when they run, not when they are built", () => {
  test("TestFileSystem.readFile sees a file written after the effect was built", async () => {
    const { TestFileSystem } = await import("../src/filesystem");
    const fs = new TestFileSystem();
    const read = fs.readFile("/later.txt");
    await run(fs.writeFile("/later.txt", "hello").orDie());
    expect(await run(read.orDie())).toBe("hello");
  });
});
