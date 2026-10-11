// Cron expressions: when a periodic job should run.
//
//   const nightly = Cron.parse("30 2 * * *");          // 02:30 every day
//   nightly.next(Date.now())                            // the next 02:30, in ms
//   repeat(job, Schedule.cron("*/5 * * * *"))           // now, then every 5 min
//   forever(nightly.untilNext.flatMap(() => job))       // only at 02:30
//
// Five fields: minute hour day-of-month month day-of-week. A sixth field in
// front adds seconds. Each field takes `*`, a number, a range `1-5`, a step
// `*/15` or `10-50/10`, or a list of those `1,15,30`. Months and days can be
// named (JAN, MON). Day-of-week 0 and 7 are both Sunday. As in classic cron,
// when both day-of-month and day-of-week are restricted, a day matching
// either one counts. `@yearly`, `@monthly`, `@weekly`, `@daily` and
// `@hourly` are shorthands.
//
// Times are local unless `{ utc: true }`. Around daylight-saving changes, as
// in classic cron: a local time that doesn't exist is skipped, a job at a
// fixed hour runs once when the clocks go back and its time comes twice, and
// jobs with a wildcard hour (every 15 minutes) keep going by real time.

import { type Eff } from "./eff.js";
import { currentClock } from "./clock.js";

export interface CronOptions {
  /** Read the expression in UTC instead of local time. Default: false. */
  readonly utc?: boolean;
}

const MACROS: Record<string, string> = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@hourly": "0 * * * *",
};

const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
const DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

interface FieldSpec {
  readonly name: string;
  readonly min: number;
  readonly max: number;
  /** Names for values, starting at `min`. */
  readonly names?: readonly string[];
}

const SECOND: FieldSpec = { name: "second", min: 0, max: 59 };
const MINUTE: FieldSpec = { name: "minute", min: 0, max: 59 };
const HOUR: FieldSpec = { name: "hour", min: 0, max: 23 };
const DAY_OF_MONTH: FieldSpec = { name: "day of month", min: 1, max: 31 };
const MONTH: FieldSpec = { name: "month", min: 1, max: 12, names: MONTHS };
const DAY_OF_WEEK: FieldSpec = { name: "day of week", min: 0, max: 7, names: DAYS };

/** The values a field allows, and whether it was `*` (no restriction). */
interface Field {
  readonly values: ReadonlySet<number>;
  readonly any: boolean;
}

// Looking further than this for a match means there is none (31 February).
const SEARCH_LIMIT_MS = 5 * 366 * 24 * 60 * 60 * 1000;

export class Cron {
  private constructor(
    /** The expression as given. */
    readonly expression: string,
    private readonly utc: boolean,
    private readonly seconds: Field,
    private readonly minutes: Field,
    private readonly hours: Field,
    private readonly daysOfMonth: Field,
    private readonly months: Field,
    private readonly daysOfWeek: Field,
  ) {}

  /** Parse a cron expression. Throws a SyntaxError saying what is wrong. */
  static parse(expression: string, options: CronOptions = {}): Cron {
    const trimmed = expression.trim();
    const expanded = MACROS[trimmed.toLowerCase()] ?? trimmed;
    const parts = expanded.split(/\s+/);
    if (parts.length !== 5 && parts.length !== 6) {
      throw new SyntaxError(
        `cron expression "${expression}" has ${parts.length} fields; expected 5 (minute hour day month weekday) or 6 (with seconds first)`,
      );
    }
    const fields = parts.length === 6 ? parts : ["0", ...parts];
    const [second, minute, hour, dayOfMonth, month, dayOfWeek] = fields as [
      string,
      string,
      string,
      string,
      string,
      string,
    ];
    const weekdays = parseField(dayOfWeek, DAY_OF_WEEK, expression);
    // 7 is Sunday too.
    const sundayAsSeven = weekdays.values.has(7)
      ? { values: new Set([...weekdays.values].map((d) => d % 7)), any: weekdays.any }
      : weekdays;
    return new Cron(
      expression,
      options.utc ?? false,
      parseField(second, SECOND, expression),
      parseField(minute, MINUTE, expression),
      parseField(hour, HOUR, expression),
      parseField(dayOfMonth, DAY_OF_MONTH, expression),
      parseField(month, MONTH, expression),
      sundayAsSeven,
    );
  }

  /**
   * The first time strictly after `after` (ms since the epoch) that matches,
   * in ms. Throws a RangeError for an expression that never matches, like
   * the 31st of February.
   */
  next(after: number): number {
    const date = new Date(Math.floor(after / 1000) * 1000 + 1000);
    const limit = after + SEARCH_LIMIT_MS;
    const get = this.utc ? utcParts : localParts;
    // When the clocks go back, the same local times come again. A job at a
    // fixed hour must not run at them a second time.
    const notBefore =
      !this.utc && !this.hours.any ? wallTime(localParts(new Date(after))) : -Infinity;

    while (date.getTime() <= limit) {
      const t = get(date);
      if (!this.months.values.has(t.month)) {
        this.startOfNextMonth(date);
      } else if (!this.dayMatches(t.day, t.weekday)) {
        this.startOfNextDay(date);
      } else if (!this.hours.values.has(t.hour)) {
        date.setTime(date.getTime() + (60 - t.minute) * 60_000 - t.second * 1000);
      } else if (!this.minutes.values.has(t.minute)) {
        date.setTime(date.getTime() + 60_000 - t.second * 1000);
      } else if (!this.seconds.values.has(t.second)) {
        date.setTime(date.getTime() + 1000);
      } else if (wallTime(t) <= notBefore) {
        date.setTime(date.getTime() + 1000);
      } else {
        return date.getTime();
      }
    }
    throw new RangeError(`cron expression "${this.expression}" never matches`);
  }

  /** Sleep until the next matching time, by the Clock in use. */
  get untilNext(): Eff<void, never> {
    return currentClock.flatMap((clock) => {
      const now = clock.now();
      return clock.sleep(this.next(now) - now);
    });
  }

  private dayMatches(day: number, weekday: number): boolean {
    const byDate = this.daysOfMonth.values.has(day);
    const byWeekday = this.daysOfWeek.values.has(weekday);
    // Classic cron: with both restricted, either one is enough.
    if (!this.daysOfMonth.any && !this.daysOfWeek.any) return byDate || byWeekday;
    return byDate && byWeekday;
  }

  private startOfNextDay(date: Date): void {
    if (this.utc) {
      date.setUTCDate(date.getUTCDate() + 1);
      date.setUTCHours(0, 0, 0, 0);
    } else {
      date.setDate(date.getDate() + 1);
      date.setHours(0, 0, 0, 0);
    }
  }

  private startOfNextMonth(date: Date): void {
    if (this.utc) {
      date.setUTCMonth(date.getUTCMonth() + 1, 1);
      date.setUTCHours(0, 0, 0, 0);
    } else {
      date.setMonth(date.getMonth() + 1, 1);
      date.setHours(0, 0, 0, 0);
    }
  }
}

interface Parts {
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly weekday: number;
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
}

const utcParts = (d: Date): Parts => ({
  year: d.getUTCFullYear(),
  month: d.getUTCMonth() + 1,
  day: d.getUTCDate(),
  weekday: d.getUTCDay(),
  hour: d.getUTCHours(),
  minute: d.getUTCMinutes(),
  second: d.getUTCSeconds(),
});

const localParts = (d: Date): Parts => ({
  year: d.getFullYear(),
  month: d.getMonth() + 1,
  day: d.getDate(),
  weekday: d.getDay(),
  hour: d.getHours(),
  minute: d.getMinutes(),
  second: d.getSeconds(),
});

/** The time shown on the clock, as a number that orders like it. */
const wallTime = (t: Parts): number =>
  Date.UTC(t.year, t.month - 1, t.day, t.hour, t.minute, t.second);

function parseField(text: string, spec: FieldSpec, expression: string): Field {
  const bad = (why: string) =>
    new SyntaxError(`cron expression "${expression}": ${spec.name} "${text}" ${why}`);
  const value = (token: string): number => {
    const named = spec.names?.indexOf(token.toUpperCase()) ?? -1;
    if (named >= 0) return named + spec.min;
    if (!/^\d+$/.test(token)) throw bad(`has "${token}", which is not a number`);
    const n = Number(token);
    if (n < spec.min || n > spec.max) {
      throw bad(`has ${n}, outside ${spec.min}-${spec.max}`);
    }
    return n;
  };

  const values = new Set<number>();
  for (const part of text.split(",")) {
    const [range, stepText, extra] = part.split("/");
    if (range === undefined || range === "" || extra !== undefined) throw bad("is malformed");
    let step = 1;
    if (stepText !== undefined) {
      if (!/^\d+$/.test(stepText) || Number(stepText) === 0) {
        throw bad(`has step "${stepText}"; a step is a whole number from 1`);
      }
      step = Number(stepText);
    }
    let from: number;
    let to: number;
    if (range === "*") {
      from = spec.min;
      to = spec.max;
    } else if (range.includes("-")) {
      const [a, b] = range.split("-");
      if (a === undefined || b === undefined) throw bad("has a malformed range");
      from = value(a);
      to = value(b);
      if (from > to) throw bad(`has the range ${a}-${b}, which runs backwards`);
    } else {
      from = value(range);
      // `5/15` means from 5 to the end, every 15.
      to = stepText === undefined ? from : spec.max;
    }
    for (let n = from; n <= to; n += step) values.add(n);
  }
  return { values, any: text === "*" };
}
