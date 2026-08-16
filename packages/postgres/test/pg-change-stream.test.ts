import { describe, it, expect } from "bun:test";
import { offsetToDate } from "../src/lib/pg-change-stream";

describe("offsetToDate — Offset → poll cursor mapping", () => {
  it("earliest maps to the epoch", () => {
    expect(offsetToDate({ type: "earliest" }).getTime()).toBe(0);
  });

  it("latest maps to (approximately) now", () => {
    const before = Date.now();
    const d = offsetToDate({ type: "latest" }).getTime();
    expect(d).toBeGreaterThanOrEqual(before);
    expect(d).toBeLessThanOrEqual(Date.now());
  });

  it("timestamp maps to that millisecond", () => {
    expect(offsetToDate({ type: "timestamp", value: 1735689600000 }).getTime()).toBe(1735689600000);
  });

  it("specific is interpreted as an ISO timestamp string", () => {
    const iso = "2026-01-01T00:00:00.000Z";
    expect(offsetToDate({ type: "specific", value: iso }).toISOString()).toBe(iso);
  });
});
