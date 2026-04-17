import { describe, test, expect } from "bun:test";
import { provide, run, Console, TestConsole } from "../src";

describe("TestConsole — capture", () => {
  test("captures log/warn/error per channel in order", async () => {
    const c = new TestConsole();
    const program = provide(
      Console.get.flatMap((con: any) =>
        con
          .log("hello")
          .flatMap(() =>
            con.warn("careful").flatMap(() => con.log("world").flatMap(() => con.error("oops"))),
          ),
      ),
      Console,
      c,
    );

    await run(program);
    expect(c.logs()).toEqual(["hello", "world"]);
    expect(c.warns()).toEqual(["careful"]);
    expect(c.errors()).toEqual(["oops"]);
  });

  test("clear() resets all channels", async () => {
    const c = new TestConsole();
    const program = provide(
      Console.get.flatMap((con: any) => con.log("a").flatMap(() => con.error("b"))),
      Console,
      c,
    );
    await run(program);
    expect(c.logs().length + c.errors().length).toBe(2);
    c.clear();
    expect(c.logs().length).toBe(0);
    expect(c.errors().length).toBe(0);
  });

  test("real Console is the default — no provide() needed", async () => {
    // Just verify it doesn't throw. The actual stdout output is silent in
    // bun test by default; we only care that the service resolves and runs.
    await run(Console.get.flatMap((con: any) => con.log("")));
  });
});
