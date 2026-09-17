import { describe, expect, test } from "bun:test";
import {
  Cause,
  Clock,
  Deferred,
  Stream,
  SyncScheduler,
  TaggedError,
  TestClock,
  die,
  provide,
  run,
  runExit,
  runFiber,
  runSync,
  sync,
  yieldNow,
  type Eff,
  type Scheduler,
  type Throws,
} from "../src";

class InnerFailure extends TaggedError("InnerFailure")<{}>() {}
class OuterFailure extends TaggedError("OuterFailure")<{}>() {}

const virtualTime = () => {
  const scheduler = new SyncScheduler();
  const clock = new TestClock();
  return {
    start: <A>(effect: Eff<A, never>) => runFiber(provide(effect, Clock, clock), scheduler),
    advance: (ms: number, step = 5) => {
      scheduler.flush();
      for (let elapsed = 0; elapsed < ms; elapsed += step) {
        clock.advance(step);
        scheduler.flush();
      }
    },
  };
};

const ticks = (params: { label: string; everyMs: number; count: number }): Stream<string> =>
  Stream.tick(params.everyMs)
    .take(params.count)
    .mapAccumulate(0, (index) => [index + 1, `${params.label}${index + 1}`] as const);

const drainScheduler = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) await new Promise((resolve) => setImmediate(resolve));
};

describe("Stream.parJoin", () => {
  test("interleaves inner stream chunks in emission order", () => {
    const { start, advance } = virtualTime();
    const fiber = start(
      Stream.of(
        ticks({ label: "a", everyMs: 10, count: 3 }),
        ticks({ label: "b", everyMs: 25, count: 2 }),
      )
        .parJoin(2)
        .toArray(),
    );

    advance(50);

    expect(fiber.result).toEqual({ ok: true, value: ["a1", "a2", "b1", "a3", "b2"] });
  });

  test("emits every element and keeps each inner stream's own order", async () => {
    const result = await run(
      Stream.of(Stream.range(0, 5), Stream.empty<number>(), Stream.range(5, 10).rechunk(2))
        .parJoin(2)
        .toArray(),
    );

    expect([...result].sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(result.filter((n) => n < 5)).toEqual([0, 1, 2, 3, 4]);
    expect(result.filter((n) => n >= 5)).toEqual([5, 6, 7, 8, 9]);
  });

  test("opens at most maxOpen inner streams and pulls the outer only for a free slot", () => {
    const scheduler = new SyncScheduler();
    const gate = runSync(Deferred.make<void>());
    let pulled = 0;
    let open = 0;
    let maxOpen = 0;
    const inner = (value: number) =>
      Stream.suspend(() => {
        open++;
        maxOpen = Math.max(maxOpen, open);
        return Stream.fromEffect(gate.await.map(() => value));
      }).onFinalize(
        sync(() => {
          open--;
        }),
      );

    const fiber = runFiber(
      Stream.repeat(sync(() => ++pulled))
        .take(10)
        .map(inner)
        .parJoin(3)
        .toArray(),
      scheduler,
    );
    scheduler.flush();

    expect({ pulled, open }).toEqual({ pulled: 3, open: 3 });

    runSync(gate.succeed(undefined));
    scheduler.flush();

    expect(fiber.result?.ok).toBe(true);
    if (fiber.result?.ok) {
      expect([...fiber.result.value].sort((a, b) => a - b)).toEqual([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      ]);
    }
    expect({ pulled, open, maxOpen }).toEqual({ pulled: 10, open: 0, maxOpen: 3 });
  });

  test("completes only after the outer stream and every open inner stream complete", () => {
    const scheduler = new SyncScheduler();
    const innerGate = runSync(Deferred.make<void>());
    const outerGate = runSync(Deferred.make<void>());
    const seen: number[] = [];

    const waitsForInner = runFiber(
      Stream.of(Stream.of(1), Stream.fromEffect(innerGate.await.map(() => 2)))
        .parJoin(2)
        .toArray(),
      scheduler,
    );
    const waitsForOuter = runFiber(
      Stream.of(Stream.of(1))
        .concat(Stream.fromEffect(outerGate.await.map(() => Stream.of(2))))
        .parJoin(2)
        .forEach((value) =>
          sync(() => {
            seen.push(value);
          }),
        ),
      scheduler,
    );
    scheduler.flush();

    expect(waitsForInner.result).toBeNull();
    expect(waitsForOuter.result).toBeNull();
    expect(seen).toEqual([1]);

    runSync(innerGate.succeed(undefined));
    runSync(outerGate.succeed(undefined));
    scheduler.flush();

    expect(waitsForInner.result).toEqual({ ok: true, value: [1, 2] });
    expect(waitsForOuter.result).toEqual({ ok: true, value: undefined });
    expect(seen).toEqual([1, 2]);
  });

  test("completes immediately for an empty outer stream or empty inner streams", async () => {
    expect(await run(Stream.empty<Stream<number>>().parJoin(4).toArray())).toEqual([]);
    expect(
      await run(Stream.of(Stream.empty<number>(), Stream.empty<number>()).parJoin(1).toArray()),
    ).toEqual([]);
    expect(
      await run(
        Stream.of(Stream.empty<number>(), Stream.empty<number>()).parJoinUnbounded().toArray(),
      ),
    ).toEqual([]);
  });

  test("an inner failure fails the result, interrupts siblings, and runs every finalizer", async () => {
    const never = runSync(Deferred.make<void>());
    const events: string[] = [];
    const inner = (value: number): Stream<number, Throws<InnerFailure>> =>
      (value === 2
        ? Stream.fromEffect(yieldNow).flatMap(() => Stream.fail(new InnerFailure({})))
        : Stream.fromEffect(never.await.map(() => value))
      ).onFinalize(sync(() => events.push(`inner${value}`)));

    const exit = await runExit(
      Stream.of(1, 2, 3)
        .onFinalize(sync(() => events.push("outer")))
        .map(inner)
        .parJoin(3)
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.failures(exit.cause)[0]).toBeInstanceOf(InnerFailure);
    }
    expect([...events].sort()).toEqual(["inner1", "inner2", "inner3", "outer"]);
    expect(events.at(-1)).toBe("outer");
  });

  test("an inner failure tears down siblings before a stalled consumer reaches it", () => {
    const scheduler = new SyncScheduler();
    const never = runSync(Deferred.make<void>());
    const consumerGate = runSync(Deferred.make<void>());
    const events: string[] = [];
    const failing: Stream<number, Throws<InnerFailure>> = Stream.of(1)
      .concat(Stream.fromEffect(yieldNow).flatMap(() => Stream.fail(new InnerFailure({}))))
      .onFinalize(sync(() => events.push("failing")));
    const sibling = Stream.fromEffect(never.await.map(() => 2)).onFinalize(
      sync(() => events.push("sibling")),
    );

    const fiber = runFiber(
      Stream.of(sibling, failing)
        .parJoinUnbounded()
        .forEach(() => consumerGate.await),
      scheduler,
    );
    scheduler.flush();

    expect(fiber.result).toBeNull();
    expect([...events].sort()).toEqual(["failing", "sibling"]);

    runSync(consumerGate.succeed(undefined));
    scheduler.flush();

    expect(fiber.result?.ok).toBe(false);
    if (fiber.result?.ok === false) {
      expect(Cause.failures(fiber.result.cause)[0]).toBeInstanceOf(InnerFailure);
    }
  });

  test("an inner defect fails the result with the defect", async () => {
    const defect = new Error("inner defect");
    const exit = await runExit(
      Stream.of(Stream.of(1), Stream.fromEffect(die(defect)))
        .parJoinUnbounded()
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.defects(exit.cause)).toEqual([defect]);
  });

  test("an outer failure fails the result and finalizes opened inner streams first", async () => {
    const never = runSync(Deferred.make<void>());
    const events: string[] = [];

    const exit = await runExit(
      Stream.of(
        Stream.fromEffect(never.await.map(() => 1)).onFinalize(sync(() => events.push("inner"))),
      )
        .concat(Stream.fail(new OuterFailure({})))
        .onFinalize(sync(() => events.push("outer")))
        .parJoin(2)
        .toArray(),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.failures(exit.cause)[0]).toBeInstanceOf(OuterFailure);
    }
    expect(events).toEqual(["inner", "outer"]);
  });

  test("downstream take stops early and finalizes the outer and every opened inner", async () => {
    let launched = 0;
    let innerFinalized = 0;
    let outerFinalized = 0;

    const result = await run(
      Stream.iterate(0, (n) => n + 1)
        .onFinalize(
          sync(() => {
            outerFinalized++;
          }),
        )
        .map((id) => {
          launched++;
          return Stream.repeat(yieldNow.map(() => id)).onFinalize(
            sync(() => {
              innerFinalized++;
            }),
          );
        })
        .parJoin(3)
        .take(5)
        .toArray(),
    );

    expect(result).toHaveLength(5);
    expect({ launched, innerFinalized, outerFinalized }).toEqual({
      launched: 3,
      innerFinalized: 3,
      outerFinalized: 1,
    });
  });

  test("finalizes inner streams that were forked but never started", () => {
    const tasks: Array<() => void> = [];
    const lastInFirstOut: Scheduler = {
      schedule: (task) => {
        tasks.push(task);
      },
      flush: () => {
        while (tasks.length > 0) tasks.pop()!();
      },
      shutdown: () => {
        tasks.length = 0;
      },
    };
    const finalized: number[] = [];

    const fiber = runFiber(
      Stream.of(1, 2, 3)
        .map((value) => Stream.of(value).onFinalize(sync(() => finalized.push(value))))
        .parJoinUnbounded()
        .take(1)
        .toArray(),
      lastInFirstOut,
    );
    lastInFirstOut.flush();

    expect(fiber.result?.ok).toBe(true);
    expect([...finalized].sort()).toEqual([1, 2, 3]);
  });

  test("interrupting the consumer tears down the outer and every open inner", () => {
    const { start, advance } = virtualTime();
    const events: string[] = [];
    const fiber = start(
      Stream.of("a", "b")
        .onFinalize(sync(() => events.push("outer")))
        .map((label) =>
          Stream.tick(10)
            .map(() => label)
            .onFinalize(sync(() => events.push(label))),
        )
        .parJoinUnbounded()
        .drain(),
    );

    advance(30);
    expect(fiber.result).toBeNull();

    fiber.interrupt();
    advance(0);

    expect(fiber.result?.ok).toBe(false);
    expect([...events].sort()).toEqual(["a", "b", "outer"]);
    expect(events.at(-1)).toBe("outer");
  });

  test("a slow consumer backpressures inner streams instead of buffering without bound", async () => {
    const gate = await run(Deferred.make<void>());
    let produced = 0;
    const inner = () => Stream.repeat(sync(() => ++produced));

    const fiber = runFiber(
      Stream.of(inner(), inner(), inner())
        .parJoinUnbounded()
        .forEach(() => gate.await),
    );
    await drainScheduler();
    const bufferedAfterStall = produced;
    await drainScheduler();

    expect(bufferedAfterStall).toBeGreaterThan(0);
    expect(bufferedAfterStall).toBeLessThan(64);
    expect(produced).toBe(bufferedAfterStall);

    fiber.interrupt();
    await fiber.await();
  });

  test("keeps working under operators that race each pull on a separate fiber", () => {
    const { start, advance } = virtualTime();
    const fiber = start(
      Stream.of(
        ticks({ label: "a", everyMs: 10, count: 2 }),
        ticks({ label: "b", everyMs: 15, count: 2 }),
      )
        .parJoin(2)
        .timeout(1_000)
        .toArray(),
    );

    advance(30);

    expect(fiber.result).toEqual({ ok: true, value: ["a1", "b1", "a2", "b2"] });
  });
});

describe("Stream.parJoinUnbounded", () => {
  test("opens every inner stream without waiting for earlier ones to finish", () => {
    const scheduler = new SyncScheduler();
    const gate = runSync(Deferred.make<void>());
    let open = 0;

    const fiber = runFiber(
      Stream.range(0, 100)
        .map((value) =>
          Stream.suspend(() => {
            open++;
            return Stream.fromEffect(gate.await.map(() => value));
          }),
        )
        .parJoinUnbounded()
        .count(),
      scheduler,
    );
    scheduler.flush();

    expect(open).toBe(100);

    runSync(gate.succeed(undefined));
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: true, value: 100 });
  });

  test("runs inner streams that arrive over time alongside the ones still open", () => {
    const { start, advance } = virtualTime();
    let next = 0;
    const fiber = start(
      Stream.tick(100)
        .take(3)
        .map(() => {
          const id = next++;
          return Stream.tick(40)
            .take(5)
            .map(() => id);
        })
        .parJoinUnbounded()
        .toArray(),
    );

    advance(490, 10);
    expect(fiber.result).toBeNull();

    advance(10, 10);
    expect(fiber.result).toEqual({
      ok: true,
      value: [0, 0, 0, 1, 0, 1, 0, 1, 2, 1, 2, 1, 2, 2, 2],
    });
  });
});
