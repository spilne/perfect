import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { fail, fromPromise, run, succeed } from "@spilne/perfect-core";
import {
  CheckpointName,
  LeaseEpoch,
  Partition,
  SourceRecordId,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
} from "@spilne/perfect-core/connect";
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
  RedisPartitionedStateBackend,
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
import { runUnchecked } from "./run-unchecked";
// These tests await effects directly (await queue.publish(x)).
import "@spilne/perfect-core/thenable";

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

describe.skipIf(!dockerAvailable)("integration — redis:7-alpine", () => {
  let container: StartedTestContainer;
  let driver: Redis;
  let redis: RedisClient;

  beforeAll(async () => {
    container = await new GenericContainer("redis:7-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(
        Wait.forAll([Wait.forLogMessage("Ready to accept connections"), Wait.forListeningPorts()]),
      )
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
    const ref = await runUnchecked(RedisRef.make({ redis, key: "ref", initial: 0 }));

    await Promise.all(Array.from({ length: 40 }, () => runUnchecked(ref.update((n) => n + 1))));

    expect(await runUnchecked(ref.get)).toBe(40);
    expect(await runUnchecked(ref.getAndSet(10))).toBe(40);
    expect(await runUnchecked(ref.updateAndGet((n) => n + 5))).toBe(15);
  });

  test("deferred broadcasts one success to multiple waiters", async () => {
    const deferred = RedisDeferred.make<number>({ redis, key: "deferred" });
    const first = runUnchecked(deferred.await);
    const second = runUnchecked(deferred.await);

    await Bun.sleep(25);
    expect(await runUnchecked(deferred.succeed(42))).toBe(true);
    expect(await Promise.all([first, second])).toEqual([42, 42]);
    expect(await runUnchecked(deferred.succeed(0))).toBe(false);
  });

  test("semaphore acquires weighted permits and restores capacity", async () => {
    const semaphore = await runUnchecked(
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

    await Promise.all([runUnchecked(work()), runUnchecked(work()), runUnchecked(work())]);
    expect(maximum).toBe(2);
    expect(await runUnchecked(semaphore.available)).toBe(2);
  });

  test("latch and barrier release every waiter", async () => {
    const latch = await runUnchecked(RedisLatch.make({ redis, key: "latch", count: 2 }));
    const latchWaiters = [runUnchecked(latch.await), runUnchecked(latch.await)];
    await runUnchecked(latch.countDown);
    expect(await runUnchecked(latch.remaining)).toBe(1);
    await runUnchecked(latch.countDown);
    await Promise.all(latchWaiters);

    const barrier = await runUnchecked(RedisBarrier.make({ redis, key: "barrier", parties: 3 }));
    await Promise.all([
      runUnchecked(barrier.await),
      runUnchecked(barrier.await),
      runUnchecked(barrier.await),
    ]);
    expect(await runUnchecked(barrier.arrived)).toBe(3);
  });

  test("rate limiter and throttle share limits across instances", async () => {
    const first = RedisRateLimiter.make({ redis, key: "rate", limit: 2, windowMs: 120 });
    const second = RedisRateLimiter.make({ redis, key: "rate", limit: 2, windowMs: 120 });

    expect(await runUnchecked(first.tryAcquire)).toBe(true);
    expect(await runUnchecked(second.tryAcquire)).toBe(true);
    expect(await runUnchecked(first.tryAcquire)).toBe(false);
    expect(await runUnchecked(first.remaining)).toBe(0);

    const throttle = RedisThrottle.make({
      redis,
      key: "throttle",
      permits: 1,
      windowMs: 60,
    });
    await runUnchecked(throttle.acquire);
    const started = performance.now();
    await runUnchecked(throttle.acquire);
    expect(performance.now() - started).toBeGreaterThanOrEqual(35);
  });

  test("cache supports TTL, size, delete, and prefix-scoped clear", async () => {
    const cache = RedisCacheStore.make<string, { n: number }>({
      redis,
      prefix: "cache:",
      ttlMs: 50,
    });
    await runUnchecked(cache.set("a", { n: 1 }, 500));
    await runUnchecked(cache.set("b", { n: 2 }));
    expect(await runUnchecked(cache.get("a"))).toEqual({ n: 1 });
    expect(await runUnchecked(cache.size)).toBe(2);
    await Bun.sleep(70);
    expect(await runUnchecked(cache.has("b"))).toBe(false);
    await runUnchecked(cache.clear());
    expect(await runUnchecked(cache.size)).toBe(0);
  });

  test("bounded queue applies backpressure, preserves FIFO, and closes remotely", async () => {
    const queue = RedisQueue.make<number>({
      redis,
      key: "queue",
      capacity: 2,
      pollIntervalMs: 20,
    });
    await runUnchecked(queue.offer(1));
    await runUnchecked(queue.offer(2));
    const third = runUnchecked(queue.offer(3));
    await Bun.sleep(40);
    expect(await runUnchecked(queue.take())).toBe(1);
    await third;
    expect(await runUnchecked(queue.takeAll())).toEqual([2, 3]);
    await runUnchecked(queue.close());
    expect(await runUnchecked(queue.isClosed)).toBe(true);
    await expect(runUnchecked(queue.take())).rejects.toMatchObject({ _tag: "QueueClosed" });
  });

  test("pubsub and subscription ref stream distributed changes", async () => {
    const pubsub = RedisPubSub.make<{ n: number }>({ redis, channel: "events" });
    const stream = await runUnchecked(pubsub.subscribe);
    const received = runUnchecked(stream.take(1).toArray());
    expect(await runUnchecked(pubsub.subscriberCount)).toBe(1);
    expect(await runUnchecked(pubsub.publish({ n: 1 }))).toBe(true);
    expect(await received).toEqual([{ n: 1 }]);

    const ref = await runUnchecked(RedisSubscriptionRef.make({ redis, key: "signal", initial: 0 }));
    const changes = await runUnchecked(ref.changes);
    const values = runUnchecked(changes.take(2).toArray());
    await Bun.sleep(10);
    await runUnchecked(ref.set(1));
    expect(await values).toEqual([0, 1]);
    await runUnchecked(ref.shutdown());
    await runUnchecked(pubsub.shutdown());
  });

  test("pubsub pattern subscriptions receive every matching channel", async () => {
    const owner = RedisPubSub.make<{ n: number }>({ redis, channel: "pattern-events:one" });
    const second = RedisPubSub.make<{ n: number }>({ redis, channel: "pattern-events:two" });
    const stream = await runUnchecked(owner.subscribePattern("pattern-events:*"));
    const received = runUnchecked(stream.take(2).toArray());

    expect(await runUnchecked(owner.patternSubscriberCount)).toBeGreaterThanOrEqual(1);
    expect(await runUnchecked(owner.publish({ n: 1 }))).toBe(true);
    expect(await runUnchecked(second.publish({ n: 2 }))).toBe(true);

    expect(await received).toEqual([{ n: 1 }, { n: 2 }]);
    await runUnchecked(owner.shutdown());
    await runUnchecked(second.shutdown());
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

  test("partitioned state atomically fences owners, mutations, progress, and dedupe", async () => {
    const state = new RedisPartitionedStateBackend({ redis, key: "partitioned-state" });
    const scope = {
      topologyId: TopologyId("orders"),
      stageId: StageId("aggregate"),
      partition: Partition(3),
    };
    const first = await state.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-a"),
      leaseMs: 30_000,
    });
    expect(first).toBeDefined();
    expect(
      await state.acquire({
        scope,
        ownerId: TopologyInstanceId("worker-b"),
        leaseMs: 30_000,
      }),
    ).toBeUndefined();
    expect(
      await state.commit({
        lease: first!,
        mutations: [{ type: "put", key: "count", value: 9 }],
        sourceId: SourceRecordId("orders:3:12"),
        sourceOffset: "12",
        checkpointId: StateCheckpointId("cp-12"),
      }),
    ).toBe("committed");
    expect(
      await state.commit({
        lease: first!,
        mutations: [],
        sourceId: SourceRecordId("orders:3:12"),
      }),
    ).toBe("duplicate");
    expect(
      await state.isProcessed({ lease: first!, sourceId: SourceRecordId("orders:3:12") }),
    ).toBe(true);
    expect(await state.load(first!)).toMatchObject({ sourceOffset: "12", checkpointId: "cp-12" });
    expect((await state.load(first!))?.values.get("count")).toBe(9);

    // A batch commit marks all its source records at once...
    expect(
      await state.commit({
        lease: first!,
        mutations: [{ type: "put", key: "batch", value: 1 }],
        sourceIds: [SourceRecordId("orders:3:20"), SourceRecordId("orders:3:21")],
      }),
    ).toBe("committed");
    // ...and a batch with one record that was already processed changes nothing.
    expect(
      await state.commit({
        lease: first!,
        mutations: [{ type: "put", key: "batch", value: 2 }],
        sourceIds: [SourceRecordId("orders:3:21"), SourceRecordId("orders:3:22")],
      }),
    ).toBe("duplicate");
    expect(
      await state.isProcessed({ lease: first!, sourceId: SourceRecordId("orders:3:20") }),
    ).toBe(true);
    expect(
      await state.isProcessed({ lease: first!, sourceId: SourceRecordId("orders:3:22") }),
    ).toBe(false);
    expect((await state.load(first!))?.values.get("batch")).toBe(1);

    expect(await state.release(first!)).toBe(true);
    const second = await state.acquire({
      scope,
      ownerId: TopologyInstanceId("worker-b"),
      leaseMs: 30_000,
    });
    expect(second?.epoch).toBe(LeaseEpoch(first!.epoch + 1));
    expect(await state.commit({ lease: first!, mutations: [] })).toBe("fenced");
    await state.release(second!);
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
      runUnchecked(first.do("key", work())),
      runUnchecked(second.do("key", work())),
    ]);
    expect([a, b]).toEqual([42, 42]);
    expect(calls).toBe(1);
  });

  test("circuit breaker state is shared and admits one half-open probe", async () => {
    type Boom = { readonly _tag: "Boom" };
    // Exceed the test deadline, then age openedAt explicitly: Redis uses its
    // own clock, and round-trips on CI can outlast a short reset timeout.
    const RESET_MS = 60_000;
    const first = RedisCircuitBreaker.make<Boom>({
      redis,
      key: "breaker",
      failureThreshold: 2,
      resetTimeoutMs: RESET_MS,
    });
    const second = RedisCircuitBreaker.make<Boom>({
      redis,
      key: "breaker",
      failureThreshold: 2,
      resetTimeoutMs: RESET_MS,
    });

    await expect(runUnchecked(first.protect(fail<Boom>({ _tag: "Boom" })))).rejects.toEqual({
      _tag: "Boom",
    });
    await expect(runUnchecked(second.protect(fail<Boom>({ _tag: "Boom" })))).rejects.toEqual({
      _tag: "Boom",
    });
    expect(await runUnchecked(first.state)).toBe("open");
    const blocked = second
      .protect(succeed("ran"))
      .catchTag("CircuitOpen", () => succeed("blocked"));
    expect(await runUnchecked(blocked)).toBe("blocked");

    await driver.hincrby("breaker", "openedAt", -RESET_MS);
    expect(await runUnchecked(first.state)).toBe("half-open");
    let releaseProbe!: () => void;
    let probeStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      probeStarted = resolve;
    });
    const release = new Promise<void>((resolve) => {
      releaseProbe = resolve;
    });
    const probe = runUnchecked(
      first.protect(
        fromPromise(
          async () => {
            probeStarted();
            await release;
            return "probe";
          },
          (e) => e,
        ),
      ),
    );
    try {
      await started;
      expect(await runUnchecked(blocked)).toBe("blocked");
    } finally {
      releaseProbe();
      await probe;
    }
    expect(await probe).toBe("probe");
    expect(await runUnchecked(second.state)).toBe("closed");

    type Filtered = { readonly _tag: "Counted" } | { readonly _tag: "Ignored" };
    const filtered = RedisCircuitBreaker.make<Filtered>({
      redis,
      key: "filtered-breaker",
      failureThreshold: 1,
      resetTimeoutMs: RESET_MS,
      isFailure: (error) => error._tag === "Counted",
    });
    await expect(
      runUnchecked(filtered.protect(fail<Filtered>({ _tag: "Counted" }))),
    ).rejects.toEqual({
      _tag: "Counted",
    });
    await driver.hincrby("filtered-breaker", "openedAt", -RESET_MS);
    await expect(
      runUnchecked(filtered.protect(fail<Filtered>({ _tag: "Ignored" }))),
    ).rejects.toEqual({
      _tag: "Ignored",
    });
    expect(await runUnchecked(filtered.protect(succeed("next probe")))).toBe("next probe");
  });
});
