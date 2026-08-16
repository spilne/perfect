import { describe, test, expect } from "bun:test";
import {
  provide,
  run,
  Clock,
  TestClock,
  Log,
  Logger,
  TestLogger,
  levelEnabled,
} from "../src";

function withLogger(eff: any, logger: TestLogger, clock?: TestClock) {
  const withLog = provide(eff, Logger, logger);
  return clock ? provide(withLog, Clock, clock) : withLog;
}

describe("Log levels", () => {
  test("levelEnabled ordering", () => {
    expect(levelEnabled("info", "debug")).toBe(false);
    expect(levelEnabled("info", "info")).toBe(true);
    expect(levelEnabled("info", "error")).toBe(true);
    expect(levelEnabled("trace", "trace")).toBe(true);
  });

  test("entries below minLevel are dropped before the sink", async () => {
    const logger = new TestLogger("warn");
    await run(
      withLogger(
        Log.debug("noise")
          .flatMap(() => Log.warn("kept"))
          .flatMap(() => Log.error("also kept")),
        logger,
      ) as any,
    );
    expect(logger.messages).toEqual(["kept", "also kept"]);
    expect(logger.atLevel("error").length).toBe(1);
  });
});

describe("Log annotations + timestamps", () => {
  test("timestamp reads the Clock service", async () => {
    const logger = new TestLogger();
    const clock = new TestClock(123_456);
    await run(withLogger(Log.info("hello"), logger, clock) as any);
    expect(logger.entries[0]!.timestamp).toBe(123_456);
  });

  test("annotated regions merge and nest", async () => {
    const logger = new TestLogger();
    const program = Log.annotated(
      Log.info("outer")
        .flatMap(() => Log.annotated(Log.info("inner", { extra: 1 }), { span: "child" }))
        .flatMap(() => Log.info("after")),
      { requestId: "r-1" },
    );

    await run(withLogger(program, logger) as any);

    expect(logger.entries[0]!.annotations).toEqual({ requestId: "r-1" });
    expect(logger.entries[1]!.annotations).toEqual({ requestId: "r-1", span: "child", extra: 1 });
    // after the inner region unwinds, only the outer annotations remain
    expect(logger.entries[2]!.annotations).toEqual({ requestId: "r-1" });
  });

  test("per-call extras override region annotations", async () => {
    const logger = new TestLogger();
    await run(
      withLogger(
        Log.annotated(Log.info("m", { requestId: "call" }), { requestId: "region" }),
        logger,
      ) as any,
    );
    expect(logger.entries[0]!.annotations).toEqual({ requestId: "call" });
  });

  test("default logger context works without provide", async () => {
    // just proves the seeded default doesn't blow up (writes to console)
    await run(Log.debug("dropped by default info level") as any);
    expect(true).toBe(true);
  });
});
