import { describe, test, expect } from "bun:test";
import { Duration, resolveMs } from "../src";

describe("Duration", () => {
  test("factory functions produce correct ms", () => {
    expect(Duration.millis(100).toMillis()).toBe(100);
    expect(Duration.seconds(5).toMillis()).toBe(5000);
    expect(Duration.minutes(2).toMillis()).toBe(120_000);
    expect(Duration.hours(1).toMillis()).toBe(3_600_000);
    expect(Duration.days(1).toMillis()).toBe(86_400_000);
    expect(Duration.weeks(1).toMillis()).toBe(604_800_000);
  });

  test("conversions roundtrip", () => {
    expect(Duration.minutes(2).toSeconds()).toBe(120);
    expect(Duration.hours(1).toMinutes()).toBe(60);
    expect(Duration.days(2).toHours()).toBe(48);
  });

  test("arithmetic", () => {
    const total = Duration.hours(1).plus(Duration.minutes(30));
    expect(total.toMillis()).toBe(5_400_000);

    const diff = Duration.minutes(10).minus(Duration.minutes(3));
    expect(diff.toMillis()).toBe(420_000);

    const tripled = Duration.seconds(5).times(3);
    expect(tripled.toSeconds()).toBe(15);
  });

  test("comparison", () => {
    const a = Duration.seconds(5);
    const b = Duration.seconds(10);
    expect(a.lt(b)).toBe(true);
    expect(b.gt(a)).toBe(true);
    expect(a.eq(Duration.seconds(5))).toBe(true);
    expect(a.lte(b)).toBe(true);
    expect(b.gte(a)).toBe(true);
  });

  test("parse handles common units", () => {
    expect(Duration.parse("100ms").toMillis()).toBe(100);
    expect(Duration.parse("5s").toMillis()).toBe(5000);
    expect(Duration.parse("30m").toMillis()).toBe(1_800_000);
    expect(Duration.parse("2h").toMillis()).toBe(7_200_000);
    expect(Duration.parse("1d").toMillis()).toBe(86_400_000);
    expect(Duration.parse("1w").toMillis()).toBe(604_800_000);
    expect(Duration.parse("1.5s").toMillis()).toBe(1500);
  });

  test("parse rejects bad input", () => {
    expect(() => Duration.parse("garbage")).toThrow(/invalid duration/);
    expect(() => Duration.parse("5y")).toThrow(/invalid duration/);
  });

  test("from accepts number, string, or Duration", () => {
    expect(Duration.from(500).toMillis()).toBe(500);
    expect(Duration.from("10s").toMillis()).toBe(10_000);
    expect(Duration.from(Duration.minutes(1)).toMillis()).toBe(60_000);
  });

  test("resolveMs convenience", () => {
    expect(resolveMs(100)).toBe(100);
    expect(resolveMs("5s")).toBe(5000);
    expect(resolveMs(Duration.hours(1))).toBe(3_600_000);
  });

  test("toString picks the largest natural unit", () => {
    expect(Duration.millis(500).toString()).toBe("500ms");
    expect(Duration.seconds(10).toString()).toBe("10s");
    expect(Duration.minutes(5).toString()).toBe("5m");
    expect(Duration.hours(2).toString()).toBe("2h");
    expect(Duration.days(3).toString()).toBe("3d");
  });
});
