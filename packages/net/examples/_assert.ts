// Tiny assertion helper — same pattern as packages/core/examples/_assert.ts.

export function assertEq<T>(actual: T, expected: T, msg?: string): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) {
    const where = msg ? `: ${msg}` : "";
    throw new Error(`assertion failed${where}\n  expected: ${e}\n  actual:   ${a}`);
  }
  console.log("✓", e);
}

export function assertContains(actual: string, needle: string, msg?: string): void {
  if (!actual.includes(needle)) {
    const where = msg ? `: ${msg}` : "";
    throw new Error(
      `assertion failed${where}\n  expected to contain: ${needle}\n  actual:              ${actual}`,
    );
  }
  console.log("✓ contains", JSON.stringify(needle));
}
