import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Clock, Cron, Schedule, TestClock, provide, repeat, runFiber, sync } from "../src";

const at = (iso: string) => Date.parse(iso);
const iso = (ms: number) => new Date(ms).toISOString().replace(".000", "");

/** The next `count` times after `from`, as ISO strings. */
function nextTimes(expression: string, from: string, count = 3): string[] {
  const cron = Cron.parse(expression, { utc: true });
  const times: string[] = [];
  let t = at(from);
  for (let i = 0; i < count; i++) {
    t = cron.next(t);
    times.push(iso(t));
  }
  return times;
}

describe("Cron.next", () => {
  test("every minute, and strictly after the given time", () => {
    expect(nextTimes("* * * * *", "2026-01-01T10:00:00Z")).toEqual([
      "2026-01-01T10:01:00Z",
      "2026-01-01T10:02:00Z",
      "2026-01-01T10:03:00Z",
    ]);
    expect(nextTimes("* * * * *", "2026-01-01T10:00:30Z", 1)).toEqual(["2026-01-01T10:01:00Z"]);
  });

  test("steps, ranges and lists", () => {
    expect(nextTimes("*/15 * * * *", "2026-01-01T10:07:00Z")).toEqual([
      "2026-01-01T10:15:00Z",
      "2026-01-01T10:30:00Z",
      "2026-01-01T10:45:00Z",
    ]);
    expect(nextTimes("0 9-17/4 * * *", "2026-01-01T00:00:00Z")).toEqual([
      "2026-01-01T09:00:00Z",
      "2026-01-01T13:00:00Z",
      "2026-01-01T17:00:00Z",
    ]);
    expect(nextTimes("5,35 * * * *", "2026-01-01T10:10:00Z", 2)).toEqual([
      "2026-01-01T10:35:00Z",
      "2026-01-01T11:05:00Z",
    ]);
    // A start with a step runs from there to the end of the range.
    expect(nextTimes("50/5 * * * *", "2026-01-01T10:00:00Z")).toEqual([
      "2026-01-01T10:50:00Z",
      "2026-01-01T10:55:00Z",
      "2026-01-01T11:50:00Z",
    ]);
  });

  test("days of the week, by number or name; 7 is Sunday too", () => {
    // 2026-01-01 is a Thursday.
    expect(nextTimes("0 8 * * MON-FRI", "2026-01-02T09:00:00Z", 2)).toEqual([
      "2026-01-05T08:00:00Z",
      "2026-01-06T08:00:00Z",
    ]);
    expect(nextTimes("0 0 * * 7", "2026-01-01T00:00:00Z", 1)).toEqual(["2026-01-04T00:00:00Z"]);
    expect(nextTimes("0 0 * * sun", "2026-01-01T00:00:00Z", 1)).toEqual(["2026-01-04T00:00:00Z"]);
  });

  test("months and month ends", () => {
    expect(nextTimes("0 0 31 * *", "2026-01-31T00:00:00Z", 3)).toEqual([
      "2026-03-31T00:00:00Z",
      "2026-05-31T00:00:00Z",
      "2026-07-31T00:00:00Z",
    ]);
    expect(nextTimes("0 12 1 JAN,JUL *", "2026-02-01T00:00:00Z", 2)).toEqual([
      "2026-07-01T12:00:00Z",
      "2027-01-01T12:00:00Z",
    ]);
    expect(nextTimes("0 0 29 2 *", "2026-01-01T00:00:00Z", 1)).toEqual(["2028-02-29T00:00:00Z"]);
  });

  test("with both day fields restricted, either one matching is enough", () => {
    // The 15th, or any Monday.
    expect(nextTimes("0 0 15 * MON", "2026-01-10T00:00:00Z", 3)).toEqual([
      "2026-01-12T00:00:00Z",
      "2026-01-15T00:00:00Z",
      "2026-01-19T00:00:00Z",
    ]);
  });

  test("six fields put seconds first", () => {
    expect(nextTimes("*/20 * * * * *", "2026-01-01T10:00:05Z")).toEqual([
      "2026-01-01T10:00:20Z",
      "2026-01-01T10:00:40Z",
      "2026-01-01T10:01:00Z",
    ]);
  });

  test("shorthands", () => {
    expect(nextTimes("@hourly", "2026-01-01T10:30:00Z", 1)).toEqual(["2026-01-01T11:00:00Z"]);
    expect(nextTimes("@daily", "2026-01-01T10:30:00Z", 1)).toEqual(["2026-01-02T00:00:00Z"]);
    expect(nextTimes("@weekly", "2026-01-01T10:30:00Z", 1)).toEqual(["2026-01-04T00:00:00Z"]);
    expect(nextTimes("@monthly", "2026-01-01T10:30:00Z", 1)).toEqual(["2026-02-01T00:00:00Z"]);
    expect(nextTimes("@yearly", "2026-01-01T10:30:00Z", 1)).toEqual(["2027-01-01T00:00:00Z"]);
  });

  test("local time by default", () => {
    const cron = Cron.parse("30 14 * * *");
    const next = new Date(cron.next(new Date(2026, 0, 1, 15, 0).getTime()));
    expect([next.getDate(), next.getHours(), next.getMinutes()]).toEqual([2, 14, 30]);
  });

  test("an expression that never matches says so", () => {
    expect(() => Cron.parse("0 0 31 2 *").next(at("2026-01-01T00:00:00Z"))).toThrow(
      "never matches",
    );
  });
});

describe("Cron.next around daylight-saving changes (New York, local time)", () => {
  const original = process.env.TZ;
  beforeAll(() => {
    process.env.TZ = "America/New_York";
  });
  afterAll(() => {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  });
  const runs = (expression: string, from: number, count: number) => {
    const cron = Cron.parse(expression);
    const times: string[] = [];
    let t = from;
    for (let i = 0; i < count; i++) {
      t = cron.next(t);
      times.push(iso(t));
    }
    return times;
  };

  test("a time that doesn't exist (02:30 on 8 March 2026) is skipped", () => {
    expect(runs("30 2 * * *", at("2026-03-07T08:00:00Z"), 2)).toEqual([
      "2026-03-09T06:30:00Z", // 02:30 EDT
      "2026-03-10T06:30:00Z",
    ]);
  });

  test("a time that comes twice (01:30 on 1 November 2026) runs once", () => {
    expect(runs("30 1 * * *", at("2026-10-31T08:00:00Z"), 2)).toEqual([
      "2026-11-01T05:30:00Z", // the first 01:30, EDT
      "2026-11-02T06:30:00Z", // the next day, EST
    ]);
  });

  test("a wildcard hour keeps going by real time through the repeated hour", () => {
    expect(runs("*/30 * * * *", at("2026-11-01T04:50:00Z"), 4)).toEqual([
      "2026-11-01T05:00:00Z",
      "2026-11-01T05:30:00Z",
      "2026-11-01T06:00:00Z",
      "2026-11-01T06:30:00Z",
    ]);
  });
});

describe("Cron.parse refuses bad expressions, saying why", () => {
  test.each([
    ["* * * *", "4 fields"],
    ["60 * * * *", 'minute "60" has 60, outside 0-59'],
    ["* * * 13 *", "outside 1-12"],
    ["*/0 * * * *", "a step is a whole number"],
    ["5-1 * * * *", "runs backwards"],
    ["x * * * *", "not a number"],
    ["* * * FOO *", "not a number"],
  ])("%p", (expression, message) => {
    expect(() => Cron.parse(expression)).toThrow(message);
  });
});

describe("Schedule.cron", () => {
  test("repeat waits for each matching time, by the Clock", async () => {
    const clock = new TestClock(at("2026-01-01T10:00:10Z"));
    const runs: string[] = [];
    const job = sync(() => void runs.push(iso(clock.now())));
    const fiber = runFiber(
      provide(
        // Three more runs after the first.
        repeat(
          job,
          Schedule.intersect(Schedule.cron("*/10 * * * * *", { utc: true }), Schedule.recurs(3)),
        ),
        Clock,
        clock,
      ),
    );
    for (let i = 0; i < 5; i++) {
      await new Promise((resolve) => setTimeout(resolve, 1));
      clock.advance(10_000);
    }
    await fiber.await();
    expect(runs).toEqual([
      "2026-01-01T10:00:10Z",
      "2026-01-01T10:00:20Z",
      "2026-01-01T10:00:30Z",
      "2026-01-01T10:00:40Z",
    ]);
  });

  test("untilNext waits for the first matching time", async () => {
    const clock = new TestClock(at("2026-01-01T10:00:05Z"));
    const cron = Cron.parse("0 * * * * *", { utc: true });
    const fiber = runFiber(
      provide(
        cron.untilNext.map(() => iso(clock.now())),
        Clock,
        clock,
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 1));
    clock.advance(55_000);
    const exit = await fiber.await();
    expect(exit._tag === "Success" && exit.value).toBe("2026-01-01T10:01:00Z");
  });
});
