import { describe, expect, test } from "bun:test";
import { acquireRelease, eff, fail, sync, use, type Eff, type Throws } from "../src";

const resource = (log: string[], name: string) =>
  acquireRelease(
    sync(() => {
      log.push(`open ${name}`);
      return name;
    }),
    () => sync(() => void log.push(`close ${name}`)),
  );

describe("use", () => {
  test("releases the resource as soon as the body is done, not at the end", async () => {
    const log: string[] = [];
    await eff(function* () {
      const length = yield* resource(log, "file").use((name) => sync(() => name.length));
      log.push(`after use: ${length}`);
    }).run();
    expect(log).toEqual(["open file", "close file", "after use: 4"]);
  });

  test("releases when the body fails, and keeps the failure typed", async () => {
    const log: string[] = [];
    const failing: Eff<never, Throws<"broken">> = use(resource(log, "conn"), () =>
      fail("broken" as const),
    );
    const result = await failing.either().run();
    expect(result).toEqual({ _tag: "Left", left: "broken" });
    expect(log).toEqual(["open conn", "close conn"]);
  });

  test("nested uses release inner first", async () => {
    const log: string[] = [];
    await resource(log, "outer")
      .use(() => resource(log, "inner").use(() => sync(() => void log.push("work"))))
      .run();
    expect(log).toEqual(["open outer", "open inner", "work", "close inner", "close outer"]);
  });
});
