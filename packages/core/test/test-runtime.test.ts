import { describe, expect, test } from "bun:test";
import {
  Clock,
  ConfigProvider,
  Console,
  Deferred,
  FileSystem,
  Log,
  Random,
  RetryPolicy,
  TestRuntime,
  eff,
  fail,
  fork,
  join,
  sleep,
  sync,
  timeoutOption,
  type Eff,
  type Throws,
} from "../src";

describe("TestRuntime", () => {
  test("provides every test service, and keeps them for checking afterwards", async () => {
    const t = TestRuntime.make({
      seed: 7,
      startTime: 1_000,
      files: { "/greeting.txt": "hello" },
      config: { NAME: "Ann" },
    });
    const result = await t.run(
      eff(function* () {
        const clock = yield* Clock.get;
        const random = yield* Random.get;
        const console = yield* Console.get;
        const files = yield* FileSystem.get;
        const config = yield* ConfigProvider.get;
        const greeting = yield* files.readFile("/greeting.txt").orDie();
        const name = config.read("NAME");
        yield* console.log(`${greeting}, ${name}`);
        yield* Log.warn("careful");
        return { now: clock.now(), roll: yield* random.nextInt(100) };
      }),
    );
    expect(result.now).toBe(1_000);
    expect(result.roll).toBe(
      await TestRuntime.make({ seed: 7 }).run(Random.get.flatMap((r) => r.nextInt(100))),
    );
    expect(t.console.logs()).toEqual(["hello, Ann"]);
    expect(t.logger.atLevel("warn").map((e) => e.message)).toEqual(["careful"]);
  });

  test("runAdvancing jumps the clock from sleep to sleep, so slow code finishes at once", async () => {
    const t = TestRuntime.make();
    const started = Date.now();
    const result = await t.runAdvancing(
      eff(function* () {
        yield* sleep(60_000);
        const a = yield* fork(sleep(3_600_000).map(() => "an hour"));
        const b = yield* fork(sleep(1_000).map(() => "a second"));
        return [yield* join(b), yield* join(a)];
      }),
    );
    expect(result).toEqual(["a second", "an hour"]);
    expect(t.clock.now()).toBe(60_000 + 3_600_000);
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  test("retries with backoff run through without real waiting", async () => {
    const t = TestRuntime.make();
    let attempts = 0;
    const flaky: Eff<string, Throws<string>> = sync(() => ++attempts).flatMap((n) =>
      n < 4 ? fail("not yet") : sync(() => "ok"),
    );
    const policy = RetryPolicy.exponential({ initial: 1_000 }).withMaxRetries(5);
    const result = await t.runAdvancing(flaky.retry(policy).orDie());
    expect(result).toBe("ok");
    expect(t.clock.now()).toBe(1_000 + 2_000 + 4_000);
  });

  test("timeouts fire in test time", async () => {
    const t = TestRuntime.make();
    const result = await t.runAdvancing(
      timeoutOption(
        sleep(10_000).map(() => "slow"),
        5_000,
      ),
    );
    expect(result).toBeUndefined();
    expect(t.clock.now()).toBe(5_000);
  });

  test("runAdvancing gives up on an effect that would never finish", async () => {
    const t = TestRuntime.make();
    const forever: Eff<never, never> = sleep(1_000).flatMap(() => forever);
    await expect(t.runAdvancing(forever, { maxMs: 10_000 })).rejects.toThrow("not done after");
  });

  test("runAdvancing gives up on an effect waiting for something that never comes", async () => {
    const t = TestRuntime.make();
    const never = Deferred.make<void>().flatMap((d) => d.await);
    await expect(t.runAdvancing(never, { maxIdleMs: 50 })).rejects.toThrow(
      "something other than the clock",
    );
  });

  test("run leaves the clock alone; advance moves it", async () => {
    const t = TestRuntime.make();
    const done: number[] = [];
    const running = t.run(sleep(500).flatMap(() => sync(() => void done.push(t.clock.now()))));
    await t.advance(499);
    expect(done).toEqual([]);
    await t.advance(1);
    await running;
    expect(done).toEqual([500]);
  });

  test("a failure comes out as the rejection, like run", async () => {
    const t = TestRuntime.make();
    await expect(t.run(fail(new Error("nope")).orDie())).rejects.toThrow("nope");
    const exit = await t.runExit(fail("typed"));
    expect(exit._tag).toBe("Failure");
  });
});
