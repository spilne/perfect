import { describe, test, expect } from "bun:test";
import { eff, succeed, sleep, fork, join, run, runSync, PubSub, SubscriptionRef } from "../src";

describe("PubSub", () => {
  test("reusable publish effects use subscribers present at execution", () => {
    const pubsub = runSync(PubSub.unbounded<number>());
    const publish = pubsub.publish(42);
    expect(runSync(publish)).toBe(false);
    for (let i = 0; i < 2; i++) {
      const stream = runSync(pubsub.subscribe);
      expect(runSync(publish)).toBe(true);
      expect(runSync(stream.head().orDie())).toBe(42);
      expect(runSync(pubsub.subscriberCount)).toBe(0);
    }
    runSync(pubsub.shutdown());
    expect(runSync(publish)).toBe(false);
  });

  test("subscriber receives published messages", async () => {
    const result = await run(
      eff(function* () {
        const pubsub = yield* PubSub.unbounded<number>();
        const stream = yield* pubsub.subscribe;
        const collector = yield* fork(stream.take(3).toArray());
        yield* sleep(5);
        yield* pubsub.publish(1);
        yield* pubsub.publish(2);
        yield* pubsub.publish(3);
        return yield* join(collector);
      }) as any,
    );
    expect(result).toEqual([1, 2, 3]);
  });

  test("multiple subscribers each see every message", async () => {
    const result = await run(
      eff(function* () {
        const pubsub = yield* PubSub.unbounded<string>();
        const a = yield* pubsub.subscribe;
        const b = yield* pubsub.subscribe;
        const c = yield* pubsub.subscribe;
        const fA = yield* fork(a.take(2).toArray());
        const fB = yield* fork(b.take(2).toArray());
        const fC = yield* fork(c.take(2).toArray());
        yield* sleep(5);
        yield* pubsub.publish("x");
        yield* pubsub.publish("y");
        return [yield* join(fA), yield* join(fB), yield* join(fC)];
      }) as any,
    );
    expect(result).toEqual([
      ["x", "y"],
      ["x", "y"],
      ["x", "y"],
    ]);
  });

  test("publish with no subscribers returns false", async () => {
    const program = eff(function* () {
      const pubsub = yield* PubSub.unbounded<number>();
      return yield* pubsub.publish(42);
    });
    expect(await run(program as any)).toBe(false);
  });

  test("subscriberCount tracks active subscribers", async () => {
    const program = eff(function* () {
      const pubsub = yield* PubSub.unbounded<number>();
      yield* pubsub.subscribe;
      yield* pubsub.subscribe;
      yield* pubsub.subscribe;
      return yield* pubsub.subscriberCount;
    });
    expect(await run(program as any)).toBe(3);
  });

  test("shutdown closes all subscriber streams", async () => {
    const result = await run(
      eff(function* () {
        const pubsub = yield* PubSub.unbounded<number>();
        const stream = yield* pubsub.subscribe;
        const collector = yield* fork((stream.toArray() as any).catch(() => succeed([])));
        yield* sleep(5);
        yield* pubsub.publish(1);
        yield* pubsub.publish(2);
        yield* sleep(5);
        yield* pubsub.shutdown();
        return yield* join(collector);
      }) as any,
    );
    expect(result).toEqual([1, 2]);
  });

  test("validates capacity >= 1 for bounded", async () => {
    expect(() => run(PubSub.bounded<number>(0) as any)).toThrow(/capacity must be >= 1/);
  });
});

describe("SubscriptionRef", () => {
  test("get returns current value", async () => {
    const program = eff(function* () {
      const ref = yield* SubscriptionRef.make(42);
      return yield* ref.get;
    });
    expect(await run(program as any)).toBe(42);
  });

  test("set updates the value", async () => {
    const program = eff(function* () {
      const ref = yield* SubscriptionRef.make("a");
      yield* ref.set("b");
      return yield* ref.get;
    });
    expect(await run(program as any)).toBe("b");
  });

  test("update applies a function", async () => {
    const program = eff(function* () {
      const ref = yield* SubscriptionRef.make(10);
      yield* ref.update((n) => n + 1);
      yield* ref.update((n) => n * 2);
      return yield* ref.get;
    });
    expect(await run(program as any)).toBe(22);
  });

  test("changes emits current value first, then updates", async () => {
    const result = await run(
      eff(function* () {
        const ref = yield* SubscriptionRef.make(0);
        const stream = yield* ref.changes;
        const collector = yield* fork(stream.take(4).toArray());
        yield* sleep(5);
        yield* ref.set(1);
        yield* ref.set(2);
        yield* ref.update((n) => n + 10); // 12
        return yield* join(collector);
      }) as any,
    );
    expect(result).toEqual([0, 1, 2, 12]);
  });

  test("multiple subscribers each see initial value + all updates", async () => {
    const result = await run(
      eff(function* () {
        const ref = yield* SubscriptionRef.make("initial");
        const a = yield* ref.changes;
        const b = yield* ref.changes;
        const fA = yield* fork(a.take(3).toArray());
        const fB = yield* fork(b.take(3).toArray());
        yield* sleep(5);
        yield* ref.set("first");
        yield* ref.set("second");
        return [yield* join(fA), yield* join(fB)];
      }) as any,
    );
    expect(result).toEqual([
      ["initial", "first", "second"],
      ["initial", "first", "second"],
    ]);
  });
});

describe("PubSub subscriber cleanup", () => {
  test("finished subscriber leaves the broadcast set", async () => {
    const result = await run(
      eff(function* () {
        const pubsub = yield* PubSub.bounded<number>(4);
        const stream = yield* pubsub.subscribe;
        const collector = yield* fork(stream.take(2).toArray());
        yield* sleep(5);
        yield* pubsub.publish(1);
        yield* pubsub.publish(2);
        const vals = yield* join(collector);
        yield* sleep(5);
        const count = yield* pubsub.subscriberCount;
        return { vals, count };
      }) as any,
    );
    expect(result).toEqual({ vals: [1, 2], count: 0 });
  });

  test("publish does not block on an abandoned bounded subscriber", async () => {
    const result = await run(
      eff(function* () {
        const pubsub = yield* PubSub.bounded<number>(1);
        const stream = yield* pubsub.subscribe;
        const collector = yield* fork(stream.take(1).toArray());
        yield* sleep(5);
        yield* pubsub.publish(1);
        yield* join(collector);
        yield* sleep(5);
        // subscriber is gone — these must return immediately, not block on
        // its full 1-slot queue
        yield* pubsub.publish(2);
        yield* pubsub.publish(3);
        return "done";
      }) as any,
    );
    expect(result).toBe("done");
  }, 2000);
});
