import { describe, expect, test } from "bun:test";
import {
  SyncScheduler,
  async,
  run,
  runFiber,
  sync,
  type Eff,
  type Scheduler,
} from "@spilne/perfect-core";
import { RedisLatch } from "../src/redis-latch";
import { RedisPubSub } from "../src/redis-pubsub";
import { RedisQueue } from "../src/redis-queue";
import { RedisSingleflight } from "../src/redis-singleflight";
import { RedisCircuitBreaker } from "../src/redis-circuit-breaker";
import { RedisStream } from "../src/redis-stream";
import { redisKeyFamily } from "../src/internal";
import type { RedisClient } from "../src/redis-client";

const unsafeRun = <A>(effect: Eff<A, unknown>): Promise<A> => run(effect as any);

describe("Redis production hardening", () => {
  test("derives Redis Cluster-safe key families", () => {
    expect(`${redisKeyFamily("orders")}:data`).toBe("{orders}:data");
    expect(`${redisKeyFamily("{tenant-1}:orders")}:data`).toBe("{tenant-1}:orders:data");
  });

  test("fails a slow Pub/Sub subscriber instead of growing without bound", async () => {
    const listeners = new Set<(...args: any[]) => void>();
    const subscriber: Partial<RedisClient> = {
      async subscribe() {},
      async unsubscribe() {},
      on(event, listener) {
        if (event === "message") listeners.add(listener);
      },
      off(event, listener) {
        if (event === "message") listeners.delete(listener);
      },
      disconnect() {},
    };
    const redis: Partial<RedisClient> = {
      duplicate: () => subscriber as RedisClient,
    };
    const pubsub = RedisPubSub.make<number>({
      redis: redis as RedisClient,
      channel: "events",
      bufferCapacity: 1,
    });
    const stream = await unsafeRun(pubsub.subscribe);

    for (const listener of listeners) {
      listener("events", "1");
      listener("events", "2");
    }

    await expect(unsafeRun(stream.toArray())).rejects.toMatchObject({
      _tag: "RedisError",
      operation: "pubsub.overflow",
    });
    expect(listeners.size).toBe(0);
  });

  test("routes poison pending messages to a dead-letter stream", async () => {
    const additions: unknown[][] = [];
    const acknowledgements: string[][] = [];
    const deletions: string[][] = [];
    const redis: Partial<RedisClient> = {
      async xgroup() {
        return "OK";
      },
      async xautoclaim() {
        return ["0-0", [["7-0", ["data", JSON.stringify({ n: 7 }), "key", "account-1"]]], []];
      },
      async xpending() {
        return [["7-0", "old-consumer", 10_000, 3]];
      },
      async xadd(...args) {
        additions.push(args);
        return "8-0";
      },
      async xack(_stream, _group, ...ids) {
        acknowledgements.push(ids);
        return ids.length;
      },
      async xdel(_stream, ...ids) {
        deletions.push(ids);
        return ids.length;
      },
    };
    const stream = RedisStream.make<{ n: number }>({
      redis: redis as RedisClient,
      stream: "events",
      group: "workers",
    });

    const result = await unsafeRun(
      stream.recoverPending({
        minIdleMs: 5_000,
        count: 10,
        maxDeliveries: 3,
        deadLetterStream: "events-dlq",
        deleteAfterDeadLetter: true,
      }),
    );

    expect(result.messages).toEqual([]);
    expect(result.deadLetteredIds).toEqual(["7-0"]);
    expect(additions[0]).toEqual([
      "events-dlq",
      "*",
      "data",
      JSON.stringify({ n: 7 }),
      "key",
      "account-1",
      "source-stream",
      "events",
      "source-group",
      "workers",
      "source-id",
      "7-0",
      "deliveries",
      3,
    ]);
    expect(acknowledgements).toEqual([["7-0"]]);
    expect(deletions).toEqual([["7-0"]]);
  });

  test("an interrupted singleflight leader publishes its failure and releases the lock", async () => {
    const published: string[] = [];
    let releases = 0;
    const redis: Partial<RedisClient> = {
      set: async () => "OK",
      rpush: async (_key, ...values) => {
        published.push(...values);
        return published.length;
      },
      pexpire: async () => 1,
      eval: async () => {
        releases++;
        return 1;
      },
    };
    let started = false;
    const flights = RedisSingleflight.make({ redis: redis as RedisClient });
    const leader = runFiber(
      flights.do(
        "key",
        sync(() => void (started = true)).flatMap(() => async<void>(() => () => {})),
      ) as any,
    );
    for (let i = 0; i < 100 && !started; i++) await new Promise((r) => setTimeout(r, 0));
    expect(started).toBe(true);

    leader.interrupt();
    const exit = await leader.await();

    expect(exit).toEqual({ _tag: "Failure", cause: { _tag: "Interrupt" } });
    expect(published.map((value) => JSON.parse(value).ok)).toEqual([false]);
    expect(releases).toBe(1);
  });

  describe("a blocking pop whose waiter is gone puts back what it took", () => {
    const macrotask = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
    // Lets promises settle and runs the fibers they resumed.
    const settle = async (scheduler: SyncScheduler, until: () => boolean = () => false) => {
      for (let i = 0; i < 20 && !until(); i++) {
        await macrotask();
        scheduler.flush();
      }
    };

    const fakeRedis = () => {
      const pushed: Array<[string, string[]]> = [];
      let pop: ((value: [string, string] | null) => void) | undefined;
      const connection: Partial<RedisClient> = {
        brpop: () =>
          new Promise((resolve) => {
            pop = resolve;
          }),
        disconnect() {},
      };
      const redis: Partial<RedisClient> = {
        duplicate: () => connection as RedisClient,
        rpush: async (key, ...values) => {
          pushed.push([key, values]);
          return pushed.length;
        },
        get: async () => "1",
        set: async () => "OK",
      };
      return {
        redis: redis as RedisClient,
        pushed,
        popping: () => pop !== undefined,
        pop: (value: [string, string] | null) => pop!(value),
      };
    };

    test("RedisQueue.take interrupted as BRPOP returns an item", async () => {
      const fake = fakeRedis();
      const queue = RedisQueue.make<number>({ redis: fake.redis, key: "jobs" });
      const scheduler = new SyncScheduler();
      const taker = runFiber(queue.take() as Eff<number, never>, scheduler);
      await settle(scheduler, fake.popping);

      fake.pop(["{jobs}:data", "7"]);
      await macrotask();
      expect(taker.status).toBe("ready");
      taker.interrupt();
      scheduler.flush();

      expect(taker.result).toEqual({ ok: false, cause: { _tag: "Interrupt" } });
      expect(fake.pushed).toEqual([["{jobs}:data", ["7"]]]);
    });

    test("RedisQueue.take interrupted while BRPOP is still returning an item", async () => {
      const fake = fakeRedis();
      const queue = RedisQueue.make<number>({ redis: fake.redis, key: "jobs" });
      const scheduler = new SyncScheduler();
      const taker = runFiber(queue.take() as Eff<number, never>, scheduler);
      await settle(scheduler, fake.popping);

      taker.interrupt();
      scheduler.flush();
      fake.pop(["{jobs}:data", "8"]);
      await macrotask();

      expect(fake.pushed).toEqual([["{jobs}:data", ["8"]]]);
    });

    test("RedisLatch.await interrupted as BRPOP returns the wake-up token", async () => {
      const fake = fakeRedis();
      const latch = await unsafeRun(RedisLatch.make({ redis: fake.redis, key: "ready", count: 1 }));
      const scheduler = new SyncScheduler();
      const waiter = runFiber(latch.await as Eff<void, never>, scheduler);
      await settle(scheduler, fake.popping);

      fake.pop(["{ready}:notify", "1"]);
      await macrotask();
      expect(waiter.status).toBe("ready");
      waiter.interrupt();
      scheduler.flush();

      expect(fake.pushed).toEqual([["{ready}:notify", ["1"]]]);
    });

    test("a BRPOP that times out gives nothing back", async () => {
      const fake = fakeRedis();
      const queue = RedisQueue.make<number>({ redis: fake.redis, key: "jobs" });
      const scheduler = new SyncScheduler();
      const taker = runFiber(queue.take() as Eff<number, never>, scheduler);
      await settle(scheduler, fake.popping);

      taker.interrupt();
      scheduler.flush();
      fake.pop(null);
      await macrotask();

      expect(fake.pushed).toEqual([]);
    });
  });

  test("a singleflight leader interrupted after taking the lock still releases it", async () => {
    const published: string[] = [];
    let releases = 0;
    const redis: Partial<RedisClient> = {
      set: async () => "OK",
      rpush: async (_key, ...values) => {
        published.push(...values);
        return published.length;
      },
      pexpire: async () => 1,
      eval: async () => {
        releases++;
        return 1;
      },
    };
    const queue: Array<() => void> = [];
    const scheduler: Scheduler = {
      schedule: (task) => void queue.push(task),
      flush: () => {
        while (queue.length > 0) queue.shift()!();
      },
      shutdown: () => void (queue.length = 0),
    };
    const flights = RedisSingleflight.make({ redis: redis as RedisClient });
    const leader = runFiber(
      flights.do(
        "key",
        async<void>(() => () => {}),
      ) as any,
      scheduler,
    );
    scheduler.flush();
    // SET NX has resolved and the leader's resume is queued, but has not run.
    for (let i = 0; i < 10 && queue.length === 0; i++) await Promise.resolve();
    expect(queue.length).toBe(1);

    leader.interrupt();
    for (let i = 0; i < 20 && leader.status !== "done"; i++) {
      scheduler.flush();
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    expect(leader.result).toEqual({ ok: false, cause: { _tag: "Interrupt" } });
    expect(published.map((value) => JSON.parse(value).ok)).toEqual([false]);
    expect(releases).toBe(1);
  });

  test("an interrupted half-open circuit breaker probe releases its claim", async () => {
    const calls: string[] = [];
    const redis: Partial<RedisClient> = {
      eval: async (script: string) => {
        if (script.includes("local claim")) {
          calls.push("inspect");
          return [1, "half-open", 0, 0, Date.now(), 7];
        }
        calls.push(script.includes("and state == 'half-open'") ? "release" : "other");
        return 1;
      },
    };
    const breaker = RedisCircuitBreaker.make({
      redis: redis as RedisClient,
      key: "breaker",
      failureThreshold: 1,
      resetTimeoutMs: 10,
    });
    let started = false;
    const fiber = runFiber(
      breaker.protect(
        sync(() => {
          started = true;
        }).flatMap(() => async<void>(() => () => {})),
      ) as any,
    );
    for (let i = 0; i < 20 && !started; i++) await new Promise((resolve) => setTimeout(resolve, 0));
    expect(started).toBe(true);

    fiber.interrupt();
    await fiber.await();

    expect(calls).toEqual(["inspect", "release"]);
  });
});
