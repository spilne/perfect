// Stream operator tail: mapAccumulate/statefulMap, dedupe/distinctBy,
// sliding, zipWithPrevious, Stream.repeatWith, orElse, timeout, interruptOn,
// interruptAfter. Time-based ops are Clock-routed — a TestClock drives them
// deterministically (tick + advance loops, no real waiting).

import { describe, test, expect } from "bun:test";
import {
  run,
  runExit,
  provide,
  sync,
  Stream,
  Chunk,
  Queue,
  Clock,
  TestClock,
  Cause,
  StreamTimeoutError,
} from "../src";

const tick = () => new Promise((r) => setTimeout(r, 0));

describe("mapAccumulate / statefulMap", () => {
  test("threads state and emits mapped values (running sum)", async () => {
    const result = await run(
      (Stream.of(1, 2, 3) as any)
        .mapAccumulate(0, (acc: number, a: number) => [acc + a, acc + a])
        .toArray(),
    );
    expect(result).toEqual([1, 3, 6]);
  });

  test("state carries across chunk boundaries", async () => {
    const result = await run(
      (Stream.of(1, 2) as any)
        .concat(Stream.of(3, 4))
        .mapAccumulate(0, (acc: number, a: number) => [acc + a, acc + a])
        .toArray(),
    );
    expect(result).toEqual([1, 3, 6, 10]);
  });

  test("statefulMap is an alias with identical semantics", async () => {
    const result = await run(
      (Stream.of("a", "b", "c") as any)
        .statefulMap(0, (i: number, s: string) => [i + 1, `${i}:${s}`])
        .toArray(),
    );
    expect(result).toEqual(["0:a", "1:b", "2:c"]);
  });

  test("empty stream emits nothing", async () => {
    const result = await run(
      (Stream.empty<number>() as any).mapAccumulate(0, (s: number, a: number) => [s, a]).toArray(),
    );
    expect(result).toEqual([]);
  });

  test("failure propagates", async () => {
    const exit = await runExit(
      (Stream.of(1) as any)
        .concat(Stream.fail("boom"))
        .mapAccumulate(0, (s: number, a: number) => [s, a])
        .toArray(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
  });
});

describe("dedupe / distinctBy", () => {
  test("global dedup — non-consecutive duplicates are dropped", async () => {
    const result = await run((Stream.of(1, 2, 1, 3, 2, 1) as any).dedupe().toArray());
    expect(result).toEqual([1, 2, 3]);
  });

  test("dedupe with a key function keeps the first occurrence per key", async () => {
    const result = await run(
      (Stream.of({ id: 1, v: "a" }, { id: 2, v: "b" }, { id: 1, v: "c" }) as any)
        .dedupe((x: { id: number }) => x.id)
        .toArray(),
    );
    expect(result).toEqual([
      { id: 1, v: "a" },
      { id: 2, v: "b" },
    ]);
  });

  test("distinctBy is dedupe with a required key function", async () => {
    const result = await run(
      (Stream.of("aa", "ab", "ba", "bb") as any).distinctBy((s: string) => s[0]).toArray(),
    );
    expect(result).toEqual(["aa", "ba"]);
  });

  test("differs from changes: changes only drops consecutive duplicates", async () => {
    const source = [1, 1, 2, 1];
    expect(await run((Stream.fromArray(source) as any).changes().toArray())).toEqual([1, 2, 1]);
    expect(await run((Stream.fromArray(source) as any).dedupe().toArray())).toEqual([1, 2]);
  });

  test("empty stream emits nothing", async () => {
    expect(await run((Stream.empty<number>() as any).dedupe().toArray())).toEqual([]);
  });

  test("works across chunk boundaries", async () => {
    const result = await run(
      (Stream.of(1, 2) as any)
        .concat(Stream.of(2, 1, 3))
        .dedupe()
        .toArray(),
    );
    expect(result).toEqual([1, 2, 3]);
  });
});

describe("sliding", () => {
  const arrays = (chunks: Chunk<number>[]) => chunks.map((c) => c.toArray());

  test("step 1 emits overlapping full windows", async () => {
    const result = await run((Stream.of(1, 2, 3, 4, 5) as any).sliding(3).toArray());
    expect(arrays(result)).toEqual([
      [1, 2, 3],
      [2, 3, 4],
      [3, 4, 5],
    ]);
  });

  test("step == size behaves like grouped without the partial tail", async () => {
    const result = await run((Stream.of(1, 2, 3, 4, 5, 6) as any).sliding(2, 2).toArray());
    expect(arrays(result)).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
    ]);
  });

  test("step > size skips elements between windows", async () => {
    const result = await run((Stream.of(1, 2, 3, 4, 5, 6) as any).sliding(2, 3).toArray());
    expect(arrays(result)).toEqual([
      [1, 2],
      [4, 5],
    ]);
  });

  test("windows span chunk boundaries", async () => {
    const result = await run((Stream.of(1, 2) as any).concat(Stream.of(3, 4)).sliding(3).toArray());
    expect(arrays(result)).toEqual([
      [1, 2, 3],
      [2, 3, 4],
    ]);
  });

  test("stream shorter than the window emits nothing", async () => {
    expect(await run((Stream.of(1, 2) as any).sliding(3).toArray())).toEqual([]);
  });

  test("empty stream emits nothing", async () => {
    expect(await run((Stream.empty<number>() as any).sliding(2).toArray())).toEqual([]);
  });

  test("failure propagates", async () => {
    const exit = await runExit((Stream.fail("boom") as any).sliding(2).toArray());
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
  });
});

describe("zipWithPrevious", () => {
  test("pairs each element with its predecessor, undefined for the head", async () => {
    const result = await run((Stream.of(1, 2, 3) as any).zipWithPrevious().toArray());
    expect(result).toEqual([
      [undefined, 1],
      [1, 2],
      [2, 3],
    ]);
  });

  test("previous carries across chunk boundaries", async () => {
    const result = await run(
      (Stream.of("a") as any).concat(Stream.of("b")).zipWithPrevious().toArray(),
    );
    expect(result).toEqual([
      [undefined, "a"],
      ["a", "b"],
    ]);
  });

  test("empty stream emits nothing", async () => {
    expect(await run((Stream.empty<number>() as any).zipWithPrevious().toArray())).toEqual([]);
  });
});

describe("Stream.repeatWith", () => {
  test("re-runs the factory-built stream n times", async () => {
    let builds = 0;
    const result = await run(
      Stream.repeatWith(() => {
        builds++;
        return Stream.of(1, 2);
      }, 3).toArray() as any,
    );
    expect(result).toEqual([1, 2, 1, 2, 1, 2]);
    expect(builds).toBe(3);
  });

  test("n = 1 runs the stream once", async () => {
    expect(await run(Stream.repeatWith(() => Stream.of(7), 1).toArray() as any)).toEqual([7]);
  });

  test("n <= 0 is empty and never builds the stream", async () => {
    let builds = 0;
    const result = await run(
      Stream.repeatWith(() => {
        builds++;
        return Stream.of(1);
      }, 0).toArray() as any,
    );
    expect(result).toEqual([]);
    expect(builds).toBe(0);
  });

  test("each repetition gets a fresh single-shot source", async () => {
    const result = await run(
      Stream.repeatWith(
        () => Stream.unfold(0, (n) => (n < 2 ? [n, n + 1] : null)),
        2,
      ).toArray() as any,
    );
    expect(result).toEqual([0, 1, 0, 1]);
  });

  test("failure in a repetition propagates", async () => {
    const exit = await runExit(
      Stream.repeatWith(() => Stream.fail("boom") as any, 2).toArray() as any,
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
  });
});

describe("orElse", () => {
  test("switches to the fallback when the source fails, keeping prior elements", async () => {
    const result = await run(
      (Stream.of(1, 2) as any)
        .concat(Stream.fail("boom"))
        .orElse(() => Stream.of(9))
        .toArray(),
    );
    expect(result).toEqual([1, 2, 9]);
  });

  test("successful source never builds the fallback", async () => {
    let built = false;
    const result = await run(
      (Stream.of(1, 2) as any)
        .orElse(() => {
          built = true;
          return Stream.of(9);
        })
        .toArray(),
    );
    expect(result).toEqual([1, 2]);
    expect(built).toBe(false);
  });

  test("empty source stays empty", async () => {
    expect(await run((Stream.empty<number>() as any).orElse(() => Stream.of(9)).toArray())).toEqual(
      [],
    );
  });

  test("a failing fallback propagates its own failure", async () => {
    const exit = await runExit((Stream.fail("a") as any).orElse(() => Stream.fail("b")).toArray());
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.pretty(exit.cause)).toBe("Fail(b)");
  });
});

describe("timeout under TestClock", () => {
  test("elements arriving within the limit pass through", async () => {
    const c = new TestClock();
    const result = await run(
      provide((Stream.of(1, 2, 3) as any).timeout(100).toArray(), Clock, c) as any,
    );
    expect(result).toEqual([1, 2, 3]);
  });

  test("a pull gap beyond the limit fails with StreamTimeoutError", async () => {
    const c = new TestClock();
    let finalized = false;
    const q: any = await run(Queue.unbounded<number>() as any);
    const source = (Stream.fromQueue(q) as any).onFinalize(
      sync(() => {
        finalized = true;
      }),
    );
    const done = runExit(provide(source.timeout(100).toArray(), Clock, c) as any);

    await run(q.offer(1));
    // element 1 arrives immediately; the next take then waits past the limit
    for (let i = 0; i < 5; i++) {
      await tick();
      c.advance(50);
    }
    const exit = await done;
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const err = Cause.firstFail(exit.cause)?.value as any;
      expect(err).toBeInstanceOf(StreamTimeoutError);
      expect(err._tag).toBe("StreamTimeoutError");
      expect(err.ms).toBe(100);
    }
    expect(finalized).toBe(true);
  });

  test("elements arriving before each deadline keep the stream alive", async () => {
    const c = new TestClock();
    const q: any = await run(Queue.unbounded<number>() as any);
    const done = run(
      provide((Stream.fromQueue(q) as any).timeout(100).take(2).toArray(), Clock, c) as any,
    );
    await run(q.offer(1));
    await tick();
    c.advance(50); // inside the window
    await run(q.offer(2));
    await tick();
    expect(await done).toEqual([1, 2]);
  });

  test("empty stream completes without failing", async () => {
    const c = new TestClock();
    const result = await run(
      provide((Stream.empty<number>() as any).timeout(100).toArray(), Clock, c) as any,
    );
    expect(result).toEqual([]);
  });
});

describe("interruptOn", () => {
  test("already-aborted signal ends the stream immediately (graceful Done)", async () => {
    const ctrl = new AbortController();
    ctrl.abort();
    let pulls = 0;
    const src = Stream.repeat(
      sync(() => {
        pulls++;
        return 1;
      }) as any,
    );
    const result = await run((src as any).interruptOn(ctrl.signal).toArray());
    expect(result).toEqual([]);
    expect(pulls).toBe(0);
  });

  test("abort mid-pull-wait ends gracefully with elements seen so far", async () => {
    const ctrl = new AbortController();
    let seen = 0;
    let finalized = false;
    const q: any = await run(Queue.unbounded<number>() as any);
    const done = run(
      (Stream.fromQueue(q) as any)
        .tap(() => seen++)
        .onFinalize(
          sync(() => {
            finalized = true;
          }),
        )
        .interruptOn(ctrl.signal)
        .toArray(),
    );

    await run(q.offer(1));
    await tick();
    ctrl.abort(); // consumer is parked on the queue take
    expect(await done).toEqual([1]);
    expect(finalized).toBe(true);

    // no further consumption after abort — the blocked pull was interrupted
    await run(q.offer(2));
    await tick();
    await tick();
    expect(seen).toBe(1);
  });

  test("removes its abort listener once each pull settles", async () => {
    let adds = 0;
    let removes = 0;
    const fake = {
      aborted: false,
      addEventListener: () => {
        adds++;
      },
      removeEventListener: () => {
        removes++;
      },
    } as unknown as AbortSignal;

    // A parked pull is what registers the listener — sync pulls settle the
    // race before the abort side ever runs its register.
    const q: any = await run(Queue.unbounded<number>() as any);
    const done = run((Stream.fromQueue(q) as any).interruptOn(fake).toArray());
    await tick();
    await run(q.offer(1));
    await tick();
    await run(q.close());
    expect(await done).toEqual([1]);
    await tick();
    expect(adds).toBeGreaterThan(0);
    expect(removes).toBe(adds);
  });

  test("failure still propagates (not converted to Done)", async () => {
    const ctrl = new AbortController();
    const exit = await runExit((Stream.fail("boom") as any).interruptOn(ctrl.signal).toArray());
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
  });
});

describe("interruptAfter under TestClock", () => {
  test("ends an infinite tick stream at the virtual deadline", async () => {
    const c = new TestClock();
    const done = run(
      provide(
        (Stream.tick(10) as any)
          .map(() => 1)
          .interruptAfter(35)
          .toArray(),
        Clock,
        c,
      ) as any,
    );
    for (let i = 0; i < 8; i++) {
      await tick();
      c.advance(10);
    }
    const result = await done;
    expect(result).toEqual([1, 1, 1]); // ticks at t=10, 20, 30; deadline t=35
    expect(c.pendingCount).toBe(0); // no timers leaked after completion
  });

  test("a stream finishing before the deadline is unaffected", async () => {
    const c = new TestClock();
    const result = await run(
      provide((Stream.of(1, 2, 3) as any).interruptAfter(100).toArray(), Clock, c) as any,
    );
    expect(result).toEqual([1, 2, 3]);
  });

  test("ends gracefully mid-pull-wait and runs finalizers", async () => {
    const c = new TestClock();
    let finalized = false;
    const q: any = await run(Queue.unbounded<number>() as any);
    const done = run(
      provide(
        (Stream.fromQueue(q) as any)
          .onFinalize(
            sync(() => {
              finalized = true;
            }),
          )
          .interruptAfter(50)
          .toArray(),
        Clock,
        c,
      ) as any,
    );
    await tick();
    c.advance(51);
    expect(await done).toEqual([]);
    expect(finalized).toBe(true);
  });

  test("non-positive budget ends before the first pull", async () => {
    const c = new TestClock();
    let pulls = 0;
    const src = Stream.repeat(
      sync(() => {
        pulls++;
        return 1;
      }) as any,
    );
    const result = await run(provide((src as any).interruptAfter(0).toArray(), Clock, c) as any);
    expect(result).toEqual([]);
    expect(pulls).toBe(0);
  });

  test("failure before the deadline propagates", async () => {
    const c = new TestClock();
    const exit = await runExit(
      provide((Stream.fail("boom") as any).interruptAfter(100).toArray(), Clock, c) as any,
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.pretty(exit.cause)).toBe("Fail(boom)");
  });
});
