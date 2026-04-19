import { describe, test, expect } from "bun:test";
import { all, succeed, fail, sleep, run, runSync, type Eff, type Throws } from "../src";

describe("all() — object form", () => {
  test("returns a record keyed by input keys", () => {
    const result = runSync(
      all({
        a: succeed(1),
        b: succeed("hi"),
        c: succeed(true),
      }),
    );
    expect(result).toEqual({ a: 1, b: "hi", c: true });
  });

  test("destructure pattern works", () => {
    const { user, posts } = runSync(
      all({
        user: succeed({ id: 7, name: "alice" }),
        posts: succeed([{ id: 1 }, { id: 2 }]),
      }),
    );
    expect(user).toEqual({ id: 7, name: "alice" });
    expect(posts).toEqual([{ id: 1 }, { id: 2 }]);
  });

  test("runs in parallel — all sleeps overlap", async () => {
    const start = Date.now();
    await run(
      all({
        a: sleep(30).flatMap(() => succeed(1)),
        b: sleep(30).flatMap(() => succeed(2)),
        c: sleep(30).flatMap(() => succeed(3)),
      }),
    );
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(100); // not 90ms — must run in parallel
  });

  test("any failure interrupts the rest", async () => {
    const program = all({
      a: succeed(1),
      b: fail("nope") as Eff<never, Throws<string>>,
      c: succeed(3),
    });
    await expect(run(program as any)).rejects.toBe("nope");
  });

  test("array form still works", () => {
    const [a, b, c] = runSync(all([succeed(1), succeed(2), succeed(3)]));
    expect([a, b, c]).toEqual([1, 2, 3]);
  });
});
