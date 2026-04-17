import { describe, test, expect } from "bun:test";

// ── Selective syntax imports ───────────────────────────────────────
// Instead of importing from "../src" (which loads ALL syntax),
// you can import only what you need:
import { succeed, fail, sync } from "../src/constructors";
import { runSync } from "../src/runtime";
import "../src/syntax/functor"; // adds .map, .as, .asVoid
import "../src/syntax/monad"; // adds .flatMap, .tap, .flatten
import "../src/syntax/error"; // adds .catch, .catchTag, .orElse, .either
import "../src/syntax/applicative"; // adds .zip, .zipWith, .zipLeft, .zipRight

describe("syntax/functor", () => {
  test("map", () => {
    expect(runSync(succeed(10).map((x) => x * 2))).toBe(20);
  });

  test("as", () => {
    expect(runSync(succeed(1).as("hello"))).toBe("hello");
  });

  test("asVoid", () => {
    expect(runSync(succeed(42).asVoid())).toBeUndefined();
  });
});

describe("syntax/monad", () => {
  test("flatMap", () => {
    expect(runSync(succeed(1).flatMap((x) => succeed(x + 1)))).toBe(2);
  });

  test("tap", () => {
    let seen = 0;
    const result = runSync(
      succeed(42).tap((x) =>
        sync(() => {
          seen = x;
        }),
      ),
    );
    expect(result).toBe(42);
    expect(seen).toBe(42);
  });

  test("flatten", () => {
    const nested = succeed(succeed(42));
    expect(runSync(nested.flatten())).toBe(42);
  });
});

describe("syntax/error", () => {
  test("orElse", () => {
    const eff = fail("nope").orElse(() => succeed("fallback"));
    expect(runSync(eff)).toBe("fallback");
  });

  test("either — success", () => {
    const result = runSync(succeed(42).either());
    expect(result).toEqual({ _tag: "Right", right: 42 });
  });

  test("either — failure", () => {
    const result = runSync(fail("boom").either());
    expect(result).toEqual({ _tag: "Left", left: "boom" });
  });
});

describe("syntax/applicative", () => {
  test("zip", () => {
    expect(runSync(succeed(1).zip(succeed("a")))).toEqual([1, "a"]);
  });

  test("zipWith", () => {
    expect(runSync(succeed(3).zipWith(succeed(4), (a, b) => a + b))).toBe(7);
  });

  test("zipLeft", () => {
    expect(runSync(succeed("keep").zipLeft(succeed("drop")))).toBe("keep");
  });

  test("zipRight", () => {
    expect(runSync(succeed("drop").zipRight(succeed("keep")))).toBe("keep");
  });
});

describe("chaining across syntax modules", () => {
  test("functor + monad + error + applicative", () => {
    const program = succeed(10)
      .map((x) => x * 2) // functor
      .flatMap((x) => succeed(x + 1)) // monad
      .zip(succeed("hello")) // applicative
      .map(([n, s]) => `${s}:${n}`) // functor
      .catch(() => succeed("fallback")); // error

    expect(runSync(program)).toBe("hello:21");
  });
});
