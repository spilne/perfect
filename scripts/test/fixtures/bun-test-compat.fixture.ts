// Runs under both `bun test` and the Node shim; node-bun-test-shim.test.ts
// checks that both report the same test titles.
import { describe, expect, it, test } from "bun:test";

test.each([1, 2])("scalar %p", (n) => {
  expect(typeof n).toBe("number");
});

test.each([
  [1, "a"],
  [2, "b"],
])("tuple %d and %s", (n, s) => {
  expect([typeof n, typeof s]).toEqual(["number", "string"]);
});

test.each([0, -1, 1.5, Number.NaN, Infinity])("pretty %p", (n) => {
  expect(typeof n).toBe("number");
});

test.each(["str"])("quoted %p", (s) => {
  expect(s).toBe("str");
});

test.each([{ name: "alpha", n: 1 }])("$name has $n", ({ name, n }) => {
  expect([name, n]).toEqual(["alpha", 1]);
});

test.each([{ a: { b: "deep" } }])("nested $a.b, missing $zzz", ({ a }) => {
  expect(a.b).toBe("deep");
});

test.each([["x", "y"]])("row %# is %s-%s", (x, y) => {
  expect([x, y]).toEqual(["x", "y"]);
});

test.each([["only one"]])("too few %s %s", (value) => {
  expect(value).toBe("only one");
});

test.each([[1, 2]])("done callback %d", (a, b, done: () => void) => {
  expect(a + b).toBe(3);
  setTimeout(done, 1);
});

test.skip.each([1])("skipped %p", () => {
  throw new Error("a skipped row must not run");
});

describe.each(["d1", "d2"])("suite %s", (name) => {
  it.each([true])(`inner %p in ${name}`, (flag) => {
    expect(flag).toBe(true);
  });
});

describe.each([{ label: "wrapped" }])("$label suite", ({ label }) => {
  test(`sees ${label}`, () => {
    expect(label).toBe("wrapped");
  });
});

test("expect takes a custom failure message", () => {
  expect(() => expect(1, "custom label").toBe(2)).toThrow("custom label");
  expect(2, "passing label").toBe(2);
});
