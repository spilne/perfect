import { describe, test, expect } from "bun:test";
import {
  eff,
  succeed,
  fail,
  sync,
  sleep,
  service,
  provide,
  run,
  runSync,
  type Eff,
  type Throws,
} from "../src";

describe("eff(function*)", () => {
  test("pure sequence — runs synchronously", () => {
    const program = eff(function* () {
      const a = yield* succeed(1);
      const b = yield* succeed(2);
      const c = yield* succeed(3);
      return a + b + c;
    });
    expect(runSync(program)).toBe(6);
  });

  test("sync effects thread values through", () => {
    const program = eff(function* () {
      const x = yield* sync(() => 10);
      const y = yield* sync(() => x * 2);
      return y + 1;
    });
    expect(runSync(program)).toBe(21);
  });

  test("async effect suspends and resumes with value", async () => {
    const program = eff(function* () {
      yield* sleep(0);
      const x = yield* succeed(42);
      return x;
    });
    expect(await run(program)).toBe(42);
  });

  test("try/catch catches typed failures", async () => {
    const program = eff(function* () {
      try {
        yield* fail("boom") as Eff<never, Throws<string>>;
        return "unreachable";
      } catch (e) {
        return `caught:${e}`;
      }
    });
    expect(await run(program as any)).toBe("caught:boom");
  });

  test("uncaught failure propagates through generator", async () => {
    const program = eff(function* () {
      yield* fail("nope") as Eff<never, Throws<string>>;
      return "unreachable";
    });
    await expect(run(program as any)).rejects.toBe("nope");
  });

  test("return of another effect is flattened", async () => {
    const inner = eff(function* () {
      const x = yield* succeed(5);
      return x * 2;
    });
    const outer = eff(function* () {
      return yield* inner;
    });
    expect(runSync(outer)).toBe(10);
  });

  test("service lookup via $-like yield", async () => {
    interface Counter {
      add(n: number): Eff<number, never>;
    }
    const Counter = service<Counter>()("Counter");
    const program = eff(function* () {
      const c = yield* Counter.get;
      const a = yield* c.add(1);
      const b = yield* c.add(a);
      return b;
    });
    const wired = provide(program, Counter, { add: (n: number) => succeed(n + 1) });
    expect(await run(wired as any)).toBe(3);
  });

  test("generator body runs fresh each execution", async () => {
    let calls = 0;
    const program = eff(function* () {
      calls++;
      return yield* succeed(calls);
    });
    expect(runSync(program)).toBe(1);
    expect(runSync(program)).toBe(2);
  });
});
