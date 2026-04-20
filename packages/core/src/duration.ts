// Duration — type-safe time arithmetic.
//
// Eliminates magic millisecond numbers in APIs. Functions that take time
// values can accept `DurationInput` (a number, "5m" string, or Duration
// object) and resolve via `resolveMs`.
//
// Duration is a value type (not a runtime primitive needing a distributed
// backend), so it's a plain class — no interface+namespace split.
//
//   Duration.seconds(5).plus(Duration.minutes(1)).toMillis() // 65000
//   Duration.parse("2h")                                     // 7200000ms
//   sleep(Duration.seconds(5).toMillis())

export class Duration {
  private constructor(readonly ms: number) {}

  // ── Factories ─────────────────────────────────────────────────────

  static millis(n: number): Duration { return new Duration(n); }
  static seconds(n: number): Duration { return new Duration(n * 1000); }
  static minutes(n: number): Duration { return new Duration(n * 60_000); }
  static hours(n: number): Duration { return new Duration(n * 3_600_000); }
  static days(n: number): Duration { return new Duration(n * 86_400_000); }
  static weeks(n: number): Duration { return new Duration(n * 604_800_000); }

  /** Parse a string like `"5s"`, `"30m"`, `"2h"`, `"1d"`, `"1w"`. */
  static parse(s: string): Duration {
    const match = s.match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$/);
    if (!match) throw new Error(`Duration.parse: invalid duration "${s}"`);
    const n = parseFloat(match[1]!);
    switch (match[2]) {
      case "ms": return Duration.millis(n);
      case "s":  return Duration.seconds(n);
      case "m":  return Duration.minutes(n);
      case "h":  return Duration.hours(n);
      case "d":  return Duration.days(n);
      case "w":  return Duration.weeks(n);
      default: throw new Error(`Duration.parse: unknown unit "${match[2]}"`);
    }
  }

  /**
   * Coerce a flexible input to Duration. Accepts:
   *   - `number`   — interpreted as milliseconds
   *   - `string`   — parsed via `Duration.parse`
   *   - `Duration` — passthrough
   */
  static from(value: DurationInput): Duration {
    if (value instanceof Duration) return value;
    if (typeof value === "number") return Duration.millis(value);
    return Duration.parse(value);
  }

  // ── Conversions ───────────────────────────────────────────────────

  toMillis(): number { return this.ms; }
  toSeconds(): number { return this.ms / 1000; }
  toMinutes(): number { return this.ms / 60_000; }
  toHours(): number { return this.ms / 3_600_000; }
  toDays(): number { return this.ms / 86_400_000; }

  // ── Arithmetic ────────────────────────────────────────────────────

  plus(other: Duration): Duration { return new Duration(this.ms + other.ms); }
  minus(other: Duration): Duration { return new Duration(this.ms - other.ms); }
  times(factor: number): Duration { return new Duration(this.ms * factor); }

  // ── Comparison ────────────────────────────────────────────────────

  gt(other: Duration): boolean { return this.ms > other.ms; }
  gte(other: Duration): boolean { return this.ms >= other.ms; }
  lt(other: Duration): boolean { return this.ms < other.ms; }
  lte(other: Duration): boolean { return this.ms <= other.ms; }
  eq(other: Duration): boolean { return this.ms === other.ms; }

  // ── Display ───────────────────────────────────────────────────────

  toString(): string {
    if (this.ms < 1000) return `${this.ms}ms`;
    if (this.ms < 60_000) return `${this.ms / 1000}s`;
    if (this.ms < 3_600_000) return `${this.ms / 60_000}m`;
    if (this.ms < 86_400_000) return `${this.ms / 3_600_000}h`;
    return `${this.ms / 86_400_000}d`;
  }
}

/** Flexible duration input: ms number, parseable string, or Duration. */
export type DurationInput = number | string | Duration;

/** Resolve a `DurationInput` to a millisecond number. */
export function resolveMs(input: DurationInput): number {
  return Duration.from(input).ms;
}
