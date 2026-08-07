// Tiny assertion helper used by every example file.
//
// Examples are self-verifying: each `assertEq(actual, expected)` proves the
// expected behavior at runtime. The smoke test (test/examples.test.ts)
// imports every example, so any drift from the documented contract fails
// the suite — no comments, no stdout capture, no subprocess spawning.

export function assertEq<T>(actual: T, expected: T, msg?: string): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) {
    const where = msg ? `: ${msg}` : "";
    throw new Error(`assertion failed${where}\n  expected: ${e}\n  actual:   ${a}`);
  }
  console.log("✓", e);
}

/** Assert a string contains a substring (for non-deterministic outputs). */
export function assertContains(actual: string, needle: string, msg?: string): void {
  if (!actual.includes(needle)) {
    const where = msg ? `: ${msg}` : "";
    throw new Error(
      `assertion failed${where}\n  expected to contain: ${needle}\n  actual:              ${actual}`,
    );
  }
  console.log("✓ contains", JSON.stringify(needle));
}
