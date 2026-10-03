import { describe, expect, test } from "bun:test";
import { run } from "@spilne/perfect-core";
import { RedisQueue } from "../src/redis-queue";
import type { RedisClient } from "../src/redis-client";

// A fake Redis whose BRPOP times out (returns null) a few times before an
// item shows up, like an idle queue being polled.
function fakeRedis(emptyPolls: number) {
  let opened = 0;
  let closed = 0;
  let polls = 0;
  let next = 0;
  const connection = (): RedisClient =>
    ({
      brpop: async () => {
        polls++;
        await new Promise((r) => setTimeout(r, 1));
        if (polls % (emptyPolls + 1) !== 0) return null;
        return ["{jobs}:data", String(next++)];
      },
      disconnect() {
        closed++;
      },
    }) as Partial<RedisClient> as RedisClient;
  const redis = {
    duplicate: () => {
      opened++;
      return connection();
    },
    hget: async () => null, // the queue is not closed
  } as Partial<RedisClient> as RedisClient;
  return { redis, opened: () => opened, closed: () => closed };
}

describe("blocking Redis commands reuse their connection", () => {
  test("a take that polls several times opens one connection", async () => {
    const fake = fakeRedis(5);
    const queue = RedisQueue.make<number>({ redis: fake.redis, key: "jobs", pollIntervalMs: 1 });
    expect(await run(queue.take())).toBe(0);
    expect(fake.opened()).toBe(1);
  });

  test("takes one after another share the connection", async () => {
    const fake = fakeRedis(0);
    const queue = RedisQueue.make<number>({ redis: fake.redis, key: "jobs", pollIntervalMs: 1 });
    for (let i = 0; i < 10; i++) await run(queue.take());
    expect(fake.opened()).toBe(1);
  });

  test("an idle connection is closed after a short while", async () => {
    const fake = fakeRedis(0);
    const queue = RedisQueue.make<number>({ redis: fake.redis, key: "jobs", pollIntervalMs: 1 });
    await run(queue.take());
    expect(fake.closed()).toBe(0);
    await new Promise((r) => setTimeout(r, 1_100));
    expect(fake.closed()).toBe(1);
  });
});
