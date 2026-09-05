import { describe, test, expect } from "bun:test";
import { sleep, fork, all, run, Deferred } from "../src";

describe("Deferred", () => {
  test("succeed then await", async () => {
    const program = Deferred.make<number>().flatMap((d) => d.succeed(42).flatMap(() => d.await));
    expect(await run(program)).toBe(42);
  });

  test("await then succeed", async () => {
    const program = Deferred.make<number>().flatMap((d) =>
      fork(sleep(10).flatMap(() => d.succeed(99))).flatMap(() => d.await),
    );
    expect(await run(program)).toBe(99);
  });

  test("multiple awaiters", async () => {
    const program = Deferred.make<string>().flatMap((d) =>
      all([d.await, d.await, d.await])
        .parZip(
          sleep(10)
            .flatMap(() => d.succeed("hello"))
            .as(undefined),
        )
        .map(([results]) => results),
    );
    expect(await run(program)).toEqual(["hello", "hello", "hello"]);
  });

  test("fail then await", async () => {
    const program = Deferred.make<number, string>().flatMap((d) =>
      d.fail("boom").flatMap(() => d.await),
    );
    await expect(run(program)).rejects.toBe("boom");
  });

  test("succeed twice returns false", async () => {
    const program = Deferred.make<number>().flatMap((d) =>
      d.succeed(1).flatMap((first) => d.succeed(2).map((second) => [first, second])),
    );
    expect(await run(program)).toEqual([true, false]);
  });

  test("isDone", async () => {
    const program = Deferred.make<number>().flatMap((d) =>
      d.isDone.flatMap((before) =>
        d.succeed(1).flatMap(() => d.isDone.map((after) => [before, after])),
      ),
    );
    expect(await run(program)).toEqual([false, true]);
  });
});
