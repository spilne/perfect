import { expect, test } from "bun:test";
import { run, runFiber, succeed, sync, SyncScheduler } from "@spilne/perfect-core";
import { RedisSemaphore } from "../src/redis-semaphore";
import type { RedisClient } from "../src/redis-client";

test("interruption during a Redis acquire releases the granted permit", async () => {
  let permits = 1;
  let releases = 0;
  let complete = () => {};
  const redis: Partial<RedisClient> = {
    set: async () => "OK",
    eval: async (_script, _keys, _key, requested, maximum) => {
      if (maximum !== undefined) {
        releases++;
        permits += Number(requested);
        return permits;
      }
      permits -= Number(requested);
      return new Promise<number>((resolve) => {
        complete = () => resolve(permits);
      });
    },
  };
  const sem = await run(
    RedisSemaphore.make({ redis: redis as RedisClient, key: "test", permits: 1 }).orDie(),
  );
  const scheduler = new SyncScheduler();
  let bodyRan = false;
  const fiber = runFiber(
    sem
      .withPermit(
        sync(() => {
          bodyRan = true;
        }),
      )
      .orDie(),
    scheduler,
  );
  scheduler.flush();
  fiber.interrupt();
  scheduler.flush();
  expect(permits).toBe(0);
  complete();
  for (let i = 0; i < 3; i++) {
    await new Promise((resolve) => setTimeout(resolve, 0));
    scheduler.flush();
  }
  expect(fiber.interrupted).toBe(true);
  expect(fiber.status).toBe("done");
  expect(bodyRan).toBe(false);
  expect(permits).toBe(1);
  expect(releases).toBe(1);
});

test("waiting for capacity remains interruptible without releasing unowned permits", async () => {
  let releases = 0;
  const redis: Partial<RedisClient> = {
    set: async () => "OK",
    eval: async (_script, _keys, _key, _requested, maximum) => {
      if (maximum !== undefined) releases++;
      return -1;
    },
  };
  const sem = await run(
    RedisSemaphore.make({
      redis: redis as RedisClient,
      key: "test",
      permits: 1,
      pollIntervalMs: 60_000,
    }).orDie(),
  );
  const scheduler = new SyncScheduler();
  const fiber = runFiber(sem.withPermit(succeed(1)).orDie(), scheduler);
  scheduler.flush();
  await new Promise((resolve) => setTimeout(resolve, 0));
  scheduler.flush();
  fiber.interrupt();
  scheduler.flush();
  expect(fiber.status).toBe("done");
  expect(fiber.interrupted).toBe(true);
  expect(releases).toBe(0);
});
