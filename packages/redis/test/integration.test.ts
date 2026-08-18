import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { fail, fromPromise, run, sleep, succeed, type Eff } from "@perfect/core";
import { CheckpointName } from "@perfect/core/connect";
import Redis from "ioredis";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import {
  RedisBarrier,
  RedisCacheStore,
  RedisCircuitBreaker,
  RedisChannel,
  RedisDeferred,
  RedisLatch,
  RedisPubSub,
  RedisQueue,
  RedisRateLimiter,
  RedisRef,
  RedisSemaphore,
  RedisStateBackend,
  RedisSingleflight,
  RedisStream,
  RedisSubscriptionRef,
  RedisThrottle,
  type RedisClient,
} from "../src";

const dockerAvailable = (() => {
  try {
    return (
      Bun.spawnSync(["docker", "info"], {
        stdout: "ignore",
        stderr: "ignore",
      }).exitCode === 0
    );
  } catch {
    return false;
  }
})();

const unsafeRun = <A>(effect: Eff<A, unknown>): Promise<A> => run(effect as any);

describe.skipIf(!dockerAvailable)("integration — redis:7-alpine", () => {
  let container: StartedTestContainer;
  let driver: Redis;
  let redis: RedisClient;

  beforeAll(async () => {
    container = await new GenericContainer("redis:7-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage("Ready to accept connections"))
      .withStartupTimeout(120_000)
      .start();
    driver = new Redis({
      host: container.getHost(),
      port: container.getMappedPort(6379),
      lazyConnect: true,
    });
    await driver.connect();
    redis = driver as unknown as RedisClient;
    await driver.flushdb();
  }, 180_000);

  afterAll(async () => {
    driver?.disconnect();
    await container?.stop();
  });

  test("ref mutations are atomic across concurrent callers", async () => {
    const ref = await unsafeRun(RedisRef.make({ redis, key: "ref", initial: 0 }));

    await Promise.all(Array.from({ length: 40 }, () => unsafeRun(ref.update((n) => n + 1))));

    expect(await unsafeRun(ref.get)).toBe(40);
    expect(await unsafeRun(ref.getAndSet(10))).toBe(40);
    expect(await unsafeRun(ref.updateAndGet((n) => n + 5))).toBe(15);
  });

  test("deferred broadcasts one success to multiple waiters", async () => {
    const deferred = RedisDeferred.make<number>({ redis, key: "deferred" });
    const first = unsafeRun(deferred.await);
    const second = unsafeRun(deferred.await);

    await Bun.sleep(25);
    expect(await unsafeRun(deferred.succeed(42))).toBe(true);
    expect(await Promise.all([first, second])).toEqual([42, 42]);
    expect(await unsafeRun(deferred.succeed(0))).toBe(false);
  });

  test("semaphore acquires weighted permits and restores capacity", async () => {
    const semaphore = await unsafeRun(
      RedisSemaphore.make({ redis, key: "semaphore", permits: 2, pollIntervalMs: 10 }),
    );
    let active = 0;
    let maximum = 0;
    const work = () =>
      semaphore.withPermit(
        fromPromise(async () => {
          active++;
          maximum = Math.max(maximum, active);
          await Bun.sleep(30);
          active--;
        }, String),
      );

    await Promise.all([unsafeRun(work()), unsafeRun(work()), unsafeRun(work())]);
    expect(maximum).toBe(2);
    expect(await unsafeRun(semaphore.available)).toBe(2);
  });

  test("latch and barrier release every waiter", async () => {
    const latch = await unsafeRun(RedisLatch.make({ redis, key: "latch", count: 2 }));
    const latchWaiters = [unsafeRun(latch.await), unsafeRun(latch.await)];
    await unsafeRun(latch.countDown);
    expect(await unsafeRun(latch.remaining)).toBe(1);
    await unsafeRun(latch.countDown);
    await Promise.all(latchWaiters);

    const barrier = await unsafeRun(RedisBarrier.make({ redis, key: "barrier", parties: 3 }));
    await Promise.all([
      unsafeRun(barrier.await),
      unsafeRun(barrier.await),
      unsafeRun(barrier.await),
    ]);
    expect(await unsafeRun(barrier.arrived)).toBe(3);
  });

  test("rate limiter and throttle share limits across instances", async () => {
    const first = RedisRateLimiter.make({ redis, key: "rate", limit: 2, windowMs: 120 });
    const second = RedisRateLimiter.make({ redis, key: "rate", limit: 2, windowMs: 120 });

    expect(await unsafeRun(first.tryAcquire)).toBe(true);
    expect(await unsafeRun(second.tryAcquire)).toBe(true);
    expect(await unsafeRun(first.tryAcquire)).toBe(false);
    expect(await unsafeRun(first.remaining)).toBe(0);

    const throttle = RedisThrottle.make({
      redis,
      key: "throttle",
      permits: 1,
      windowMs: 60,
    });
    await unsafeRun(throttle.acquire);
    const started = performance.now();
    await unsafeRun(throttle.acquire);
    expect(performance.now() - started).toBeGreaterThanOrEqual(35);
  });

  test("cache supports TTL, size, delete, and prefix-scoped clear", async () => {
    const cache = RedisCacheStore.make<string, { n: number }>({
      redis,
      prefix: "cache:",
      ttlMs: 50,
    });
    await unsafeRun(cache.set("a", { n: 1 }, 500));
    await unsafeRun(cache.set("b", { n: 2 }));
    expect(await unsafeRun(cache.get("a"))).toEqual({ n: 1 });
    expect(await unsafeRun(cache.size)).toBe(2);
    await Bun.sleep(70);
    expect(await unsafeRun(cache.has("b"))).toBe(false);
    await unsafeRun(cache.clear());
    expect(await unsafeRun(cache.size)).toBe(0);
  });

  test("bounded queue applies backpressure, preserves FIFO, and closes remotely", async () => {
    const queue = RedisQueue.make<number>({
      redis,
      key: "queue",
      capacity: 2,
      pollIntervalMs: 20,
    });
    await unsafeRun(queue.offer(1));
    await unsafeRun(queue.offer(2));
    const third = unsafeRun(queue.offer(3));
    await Bun.sleep(40);
    expect(await unsafeRun(queue.take())).toBe(1);
    await third;
    expect(await unsafeRun(queue.takeAll())).toEqual([2, 3]);
    await unsafeRun(queue.close());
    expect(await unsafeRun(queue.isClosed)).toBe(true);
    await expect(unsafeRun(queue.take())).rejects.toMatchObject({ _tag: "QueueClosed" });
  });

  test("pubsub and subscription ref stream distributed changes", async () => {
    const pubsub = RedisPubSub.make<{ n: number }>({ redis, channel: "events" });
    const stream = await unsafeRun(pubsub.subscribe);
    const received = unsafeRun(stream.take(1).toArray());
    expect(await unsafeRun(pubsub.subscriberCount)).toBe(1);
    expect(await unsafeRun(pubsub.publish({ n: 1 }))).toBe(true);
    expect(await received).toEqual([{ n: 1 }]);

    const ref = await unsafeRun(RedisSubscriptionRef.make({ redis, key: "signal", initial: 0 }));
    const changes = await unsafeRun(ref.changes);
    const values = unsafeRun(changes.take(2).toArray());
    await Bun.sleep(10);
    await unsafeRun(ref.set(1));
    expect(await values).toEqual([0, 1]);
    await unsafeRun(ref.shutdown());
    await unsafeRun(pubsub.shutdown());
  });

  test("pubsub pattern subscriptions receive every matching channel", async () => {
    const owner = RedisPubSub.make<{ n: number }>({ redis, channel: "pattern-events:one" });
    const second = RedisPubSub.make<{ n: number }>({ redis, channel: "pattern-events:two" });
    const stream = await unsafeRun(owner.subscribePattern("pattern-events:*"));
    const received = unsafeRun(stream.take(2).toArray());

    expect(await unsafeRun(owner.patternSubscriberCount)).toBeGreaterThanOrEqual(1);
    expect(await unsafeRun(owner.publish({ n: 1 }))).toBe(true);
    expect(await unsafeRun(second.publish({ n: 2 }))).toBe(true);

    expect(await received).toEqual([{ n: 1 }, { n: 2 }]);
    await unsafeRun(owner.shutdown());
    await unsafeRun(second.shutdown());
  });

  test("stream connector supports durable replay, acknowledgement, and claiming", async () => {
    const messages = RedisStream.make<{ n: number }>({
      redis,
      stream: "stream-messages",
      group: "stream-group",
      blockMs: 50,
    });
    await messages.publish({ n: 1 }, { key: "account-1" });
    await messages.publish({ n: 2 });

    expect(await run(messages.subscribe().take(2).toArray().orDie())).toEqual([{ n: 1 }, { n: 2 }]);
    expect(
      await run(
        messages
          .subscribeFrom({ offset: { type: "earliest" } })
          .take(2)
          .toArray()
          .orDie(),
      ),
    ).toEqual([{ n: 1 }, { n: 2 }]);
    expect(await messages.info()).toMatchObject({ length: 2, groups: 1 });

    const pending = RedisStream.make<{ n: number }>({
      redis,
      stream: "stream-pending",
      group: "pending-group",
      blockMs: 50,
    });
    await pending.publish({ n: 3 });
    const [envelope] = await run(pending.subscribeAck().take(1).toArray().orDie());
    const claimed = await pending.claimPending({ minIdleMs: 0, count: 10 });
    expect(claimed).toEqual([{ id: String(envelope!.metadata.id), value: { n: 3 } }]);
    expect(await pending.acknowledge(String(envelope!.metadata.id))).toBe(true);
  });

  test("state backend atomically checkpoints and restores keyed state", async () => {
    const state = new RedisStateBackend<{ count: number }>({
      redis,
      key: "topology-state",
    });
    await state.put("user-1", { count: 1 });
    await state.checkpoint({ name: CheckpointName("checkpoint-1") });
    await state.put("user-1", { count: 2 });
    await state.put("user-2", { count: 1 });

    await state.restore({ name: CheckpointName("checkpoint-1") });

    expect(await state.entries()).toEqual([["user-1", { count: 1 }]]);
    await state.clear();
  });

  test("channel connector publishes to active stream subscribers", async () => {
    const channel = RedisChannel.make<{ n: number }>({ redis, channel: "channel-events" });
    const received = run(channel.subscribe().take(1).toArray().orDie());

    for (let attempt = 0; attempt < 100 && (await channel.subscriberCount()) === 0; attempt++) {
      await Bun.sleep(5);
    }
    expect(await channel.subscriberCount()).toBe(1);
    await channel.publish({ n: 1 });

    expect(await received).toEqual([{ n: 1 }]);
  });

  test("channel connector supports pattern subscriptions", async () => {
    const first = RedisChannel.make<{ n: number }>({ redis, channel: "channel-pattern:one" });
    const second = RedisChannel.make<{ n: number }>({ redis, channel: "channel-pattern:two" });
    const received = run(first.subscribePattern("channel-pattern:*").take(2).toArray().orDie());

    for (
      let attempt = 0;
      attempt < 100 && (await first.patternSubscriberCount()) === 0;
      attempt++
    ) {
      await Bun.sleep(5);
    }
    expect(await first.patternSubscriberCount()).toBeGreaterThanOrEqual(1);
    await first.publish({ n: 1 });
    await second.publish({ n: 2 });

    expect(await received).toEqual([{ n: 1 }, { n: 2 }]);
  });

  test("singleflight executes one leader across instances", async () => {
    const first = RedisSingleflight.make({ redis, prefix: "sf:", timeoutMs: 1_000 });
    const second = RedisSingleflight.make({ redis, prefix: "sf:", timeoutMs: 1_000 });
    let calls = 0;
    const work = () =>
      fromPromise(
        async () => {
          calls++;
          await Bun.sleep(75);
          return 42;
        },
        (cause) => ({ _tag: "WorkFailed" as const, cause }),
      );

    const [a, b] = await Promise.all([
      unsafeRun(first.do("key", work())),
      unsafeRun(second.do("key", work())),
    ]);
    expect([a, b]).toEqual([42, 42]);
    expect(calls).toBe(1);
  });

  test("circuit breaker state is shared and admits one half-open probe", async () => {
    type Boom = { readonly _tag: "Boom" };
    const first = RedisCircuitBreaker.make<Boom>({
      redis,
      key: "breaker",
      failureThreshold: 2,
      resetTimeoutMs: 80,
    });
    const second = RedisCircuitBreaker.make<Boom>({
      redis,
      key: "breaker",
      failureThreshold: 2,
      resetTimeoutMs: 80,
    });

    await expect(unsafeRun(first.protect(fail<Boom>({ _tag: "Boom" })))).rejects.toEqual({
      _tag: "Boom",
    });
    await expect(unsafeRun(second.protect(fail<Boom>({ _tag: "Boom" })))).rejects.toEqual({
      _tag: "Boom",
    });
    expect(await unsafeRun(first.state)).toBe("open");
    const blocked = second
      .protect(succeed("ran"))
      .catchTag("CircuitOpen", () => succeed("blocked"));
    expect(await unsafeRun(blocked)).toBe("blocked");

    await unsafeRun(sleep(90));
    expect(await unsafeRun(first.protect(succeed("probe")))).toBe("probe");
    expect(await unsafeRun(second.state)).toBe("closed");

    type Filtered = { readonly _tag: "Counted" } | { readonly _tag: "Ignored" };
    const filtered = RedisCircuitBreaker.make<Filtered>({
      redis,
      key: "filtered-breaker",
      failureThreshold: 1,
      resetTimeoutMs: 30,
      isFailure: (error) => error._tag === "Counted",
    });
    await expect(unsafeRun(filtered.protect(fail<Filtered>({ _tag: "Counted" })))).rejects.toEqual({
      _tag: "Counted",
    });
    await unsafeRun(sleep(40));
    await expect(unsafeRun(filtered.protect(fail<Filtered>({ _tag: "Ignored" })))).rejects.toEqual({
      _tag: "Ignored",
    });
    expect(await unsafeRun(filtered.protect(succeed("next probe")))).toBe("next probe");
  });
});
