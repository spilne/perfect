import { describe, test, expect } from "bun:test";
import { succeed, suspend, runSync } from "../src";

describe("suspend", () => {
  test("lazy effect creation", () => {
    let created = false;
    const eff = suspend(() => {
      created = true;
      return succeed(42);
    });
    expect(created).toBe(false);
    expect(runSync(eff)).toBe(42);
    expect(created).toBe(true);
  });

  test("recursive effects don't stack overflow", () => {
    function countdown(n: number): ReturnType<typeof succeed<number>> {
      if (n <= 0) return succeed(0);
      return suspend(() => countdown(n - 1));
    }
    expect(runSync(countdown(10_000))).toBe(0);
  });
});
