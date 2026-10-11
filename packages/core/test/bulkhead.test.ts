import { describe, expect, test } from "bun:test";
import {
  Bulkhead,
  Clock,
  Deferred,
  TestClock,
  all,
  awaitFiber,
  eff,
  fail,
  fork,
  interrupt,
  join,
  provide,
  runFiber,
  sleep,
  sync,
  timeoutOption,
} from "../src";

/** A call that runs until `open` completes, writing down that it ran. */
const callUntil = (gate: Deferred<void>, log: string[], name: string) =>
  sync(() => void log.push(`start ${name}`)).flatMap(() =>
    gate.await.map(() => {
      log.push(`end ${name}`);
      return name;
    }),
  );

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

describe("Bulkhead", () => {
  test("runs at most maxConcurrent calls at once; the rest wait their turn in order", async () => {
    const log: string[] = [];
    const result = await eff(function* () {
      const bulkhead = Bulkhead.make({ maxConcurrent: 2, maxQueued: 10 });
      const gate = yield* Deferred.make<void>();
      const calls = yield* fork(
        all(["a", "b", "c", "d"].map((name) => bulkhead.protect(callUntil(gate, log, name)))),
      );
      yield* sleep(5);
      const during = { running: yield* bulkhead.running, queued: yield* bulkhead.queued };
      const startedBefore = [...log];
      yield* gate.succeed(undefined);
      const names = yield* join(calls);
      const after = { running: yield* bulkhead.running, queued: yield* bulkhead.queued };
      return { during, startedBefore, names, after };
    })
      .orDie()
      .run();
    expect(result.during).toEqual({ running: 2, queued: 2 });
    expect(result.startedBefore).toEqual(["start a", "start b"]);
    expect(result.names).toEqual(["a", "b", "c", "d"]);
    expect(log.filter((l) => l.startsWith("start"))).toEqual([
      "start a",
      "start b",
      "start c",
      "start d",
    ]);
    expect(result.after).toEqual({ running: 0, queued: 0 });
  });

  test("when the line is full, a call fails at once with BulkheadFull", async () => {
    const result = await eff(function* () {
      const bulkhead = Bulkhead.make({ maxConcurrent: 1, maxQueued: 1 });
      const gate = yield* Deferred.make<void>();
      const log: string[] = [];
      yield* fork(bulkhead.protect(callUntil(gate, log, "running")));
      yield* fork(bulkhead.protect(callUntil(gate, log, "waiting")));
      yield* sleep(5);
      const turnedAway = yield* bulkhead.protect(sync(() => "never")).either();
      yield* gate.succeed(undefined);
      return turnedAway;
    }).run();
    expect(result).toEqual({
      _tag: "Left",
      left: { _tag: "BulkheadFull", reason: "queue full", maxConcurrent: 1, maxQueued: 1 },
    });
  });

  test("with no queue, only maxConcurrent calls are accepted", async () => {
    const result = await eff(function* () {
      const bulkhead = Bulkhead.make({ maxConcurrent: 1 });
      const gate = yield* Deferred.make<void>();
      yield* fork(bulkhead.protect(gate.await));
      yield* sleep(1);
      const second = yield* bulkhead.protect(sync(() => 1)).either();
      yield* gate.succeed(undefined);
      return second._tag;
    }).run();
    expect(result).toBe("Left");
  });

  test("maxWaitMs fails a call that waited too long, by the Clock", async () => {
    const clock = new TestClock();
    const bulkhead = Bulkhead.make({ maxConcurrent: 1, maxQueued: 5, maxWaitMs: 1_000 });
    const gate = Deferred.make<void>().runSync();
    const program = eff(function* () {
      const started = yield* Deferred.make<void>();
      const busy = yield* fork(
        bulkhead.protect(started.succeed(undefined).flatMap(() => gate.await)),
      );
      yield* started.await;
      const outcome = yield* bulkhead.protect(sync(() => "ran")).either();
      yield* gate.succeed(undefined);
      yield* join(busy);
      return outcome;
    });
    const running = runFiber(provide(program.orDie(), Clock, clock));
    await settle();

    clock.advance(999);
    await settle();
    expect(bulkhead.queued.runSync()).toBe(1);
    clock.advance(1);
    const exit = await running.await();
    expect(exit._tag === "Success" && exit.value).toMatchObject({
      _tag: "Left",
      left: { reason: "waited too long" },
    });
    expect(bulkhead.queued.runSync()).toBe(0);
    expect(bulkhead.running.runSync()).toBe(0);
  });

  test("a call interrupted while waiting leaves the line, and its turn goes to the next", async () => {
    const result = await eff(function* () {
      const bulkhead = Bulkhead.make({ maxConcurrent: 1, maxQueued: 5 });
      const gate = yield* Deferred.make<void>();
      const log: string[] = [];
      yield* fork(bulkhead.protect(callUntil(gate, log, "first")));
      yield* sleep(1);
      const impatient = yield* fork(bulkhead.protect(callUntil(gate, log, "impatient")));
      const patient = yield* fork(bulkhead.protect(callUntil(gate, log, "patient")));
      yield* sleep(1);
      yield* interrupt(impatient);
      yield* awaitFiber(impatient);
      const queuedAfterInterrupt = yield* bulkhead.queued;
      yield* gate.succeed(undefined);
      yield* join(patient);
      return { log, queuedAfterInterrupt, running: yield* bulkhead.running };
    })
      .orDie()
      .run();
    expect(result.queuedAfterInterrupt).toBe(1);
    expect(result.log).not.toContain("start impatient");
    expect(result.log).toContain("end patient");
    expect(result.running).toBe(0);
  });

  test("a call that fails gives its slot back", async () => {
    const result = await eff(function* () {
      const bulkhead = Bulkhead.make({ maxConcurrent: 1 });
      const failed = yield* bulkhead.protect(fail("boom")).either();
      const next = yield* bulkhead.protect(sync(() => "ok"));
      return { failed: failed._tag, next, running: yield* bulkhead.running };
    })
      .orDie()
      .run();
    expect(result).toEqual({ failed: "Left", next: "ok", running: 0 });
  });

  test("maxConcurrent must be at least 1", () => {
    expect(() => Bulkhead.make({ maxConcurrent: 0 })).toThrow(RangeError);
  });

  // A call that decides to wait must be in line before the slot can be
  // freed: joining only once its wait starts (inside a race, with
  // maxWaitMs) would let a slot freed in between be given up, and the call
  // would then wait for nothing.
  test.each([[undefined], [1_000]])(
    "a slot freed just as a call decides to wait still reaches it (maxWaitMs %p)",
    async (maxWaitMs) => {
      const result = await eff(function* () {
        const bulkhead = Bulkhead.make({ maxConcurrent: 1, maxQueued: 1, maxWaitMs });
        const other = yield* fork(bulkhead.protect(sync(() => "other")).either());
        yield* bulkhead.protect(sync(() => "mine"));
        return yield* timeoutOption(join(other), 200);
      })
        .orDie()
        .run();
      expect(result).toEqual({ _tag: "Right", right: "other" });
    },
  );
});
