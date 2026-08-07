// Audit-driven coverage for the fluent methods added in the API consistency
// pass: .provide / .retry / .timeout / .scoped / .acquireRelease /
// .raceFirst / .raceEither / .repeat / .retryWith.

import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  sync,
  sleep,
  service,
  fromPromise,
  raceEither,
  RetryPolicy,
  run,
  runSync,
  type Eff,
  type Throws,
} from "../src";

describe("fluent API additions", () => {
  test(".provide(tag, impl) installs a service", () => {
    interface Greeter {
      greet(n: string): Eff<string, never>;
    }
    const Greeter = service<Greeter>("Greeter");
    const program = Greeter.get.flatMap((g: Greeter) => g.greet("world"));
    const wired = program.provide(Greeter, { greet: (n) => succeed(`hello, ${n}`) });
    expect(runSync(wired)).toBe("hello, world");
  });

  test(".retry(policy) retries typed failures", async () => {
    let calls = 0;
    // Use fail() to make it a typed failure that retry will catch
    const flakyTyped: Eff<string, Throws<string>> = sync(() => {
      calls++;
      return calls;
    }).flatMap((n: number) =>
      n < 3 ? (fail("transient") as Eff<never, Throws<string>>) : succeed("ok"),
    );
    calls = 0;
    const result = await run(flakyTyped.retry(RetryPolicy.recurs(5)));
    expect(result).toBe("ok");
    expect(calls).toBe(3);
  });

  test(".timeout(ms, onTimeout) fluent variant", async () => {
    const slow = sleep(100).flatMap(() => succeed("late"));
    const result = run(slow.timeout(10, () => "timeout"));
    await expect(result).rejects.toBe("timeout");
  });

  test(".scoped() releases finalizers on exit", async () => {
    const events: string[] = [];
    const program = succeed(undefined)
      .acquireRelease(() =>
        sync(() => {
          events.push("released");
        }),
      )
      .flatMap(() =>
        sync(() => {
          events.push("body");
          return "done";
        }),
      )
      .scoped();
    const result = await run(program);
    expect(result).toBe("done");
    expect(events).toEqual(["body", "released"]);
  });

  test(".raceFirst(other) — first to finish wins (success or failure)", async () => {
    const winner = sleep(10).flatMap(() => succeed("done"));
    const slow = sleep(50).flatMap(() => succeed("late"));
    expect(await run(winner.raceFirst(slow))).toBe("done");
  });

  test(".raceEither(other) — wraps winner in tagged Either", async () => {
    const fast = sleep(10).flatMap(() => succeed(1));
    const slow = sleep(50).flatMap(() => succeed("hi"));
    const result = await run(fast.raceEither(slow));
    expect(result).toEqual({ _tag: "Left", left: 1 });
  });

  test("raceEither also accepts array form", async () => {
    const fast = sleep(10).flatMap(() => succeed(1));
    const slow = sleep(50).flatMap(() => succeed("hi"));
    const result = await run(raceEither([fast, slow]));
    expect(result).toEqual({ _tag: "Left", left: 1 });
  });

  test("fromPromise alias works the same as tryPromise", async () => {
    const p = fromPromise(
      () => Promise.resolve(42),
      (e) => `err: ${e}`,
    );
    expect(await run(p)).toBe(42);
  });
});
