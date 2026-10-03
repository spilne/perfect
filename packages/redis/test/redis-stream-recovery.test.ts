import { describe, expect, test } from "bun:test";
import { RedisStream } from "../src/redis-stream";
import type { RedisClient } from "../src/redis-client";

function fakeRedis(claimed: number) {
  let groupCreates = 0;
  let pendingInFlight = 0;
  let maxPendingInFlight = 0;
  const redis = {
    xgroup: async (command: string) => {
      if (command === "CREATE") {
        groupCreates++;
        if (groupCreates > 1) throw new Error("BUSYGROUP Consumer Group name already exists");
      }
      return "OK";
    },
    xautoclaim: async () => [
      "0-0",
      Array.from({ length: claimed }, (_, i) => [`${i + 1}-0`, ["data", JSON.stringify(i)]]),
    ],
    xpending: async (_s: string, _g: string, id: string) => {
      pendingInFlight++;
      maxPendingInFlight = Math.max(maxPendingInFlight, pendingInFlight);
      await new Promise((r) => setTimeout(r, 2));
      pendingInFlight--;
      return [[id, "me", 1000, 1]];
    },
  } as Partial<RedisClient> as RedisClient;
  return { redis, groupCreates: () => groupCreates, maxPendingInFlight: () => maxPendingInFlight };
}

describe("RedisStream recovery", () => {
  const recover = (stream: RedisStream<unknown>) =>
    (stream as any).recoverPendingEntries("workers", "0-0", { minIdleMs: 0 });

  test("creates the consumer group once, not on every poll", async () => {
    const fake = fakeRedis(1);
    const stream = RedisStream.make<unknown>({
      redis: fake.redis,
      stream: "jobs",
      group: "workers",
      consumer: "me",
      recovery: { minIdleMs: 0 },
    });
    for (let i = 0; i < 3; i++) await recover(stream);
    expect(fake.groupCreates()).toBe(1);
  });

  test("asks for the delivery counts of claimed entries together", async () => {
    const fake = fakeRedis(5);
    const stream = RedisStream.make<unknown>({
      redis: fake.redis,
      stream: "jobs",
      group: "workers",
      consumer: "me",
      recovery: { minIdleMs: 0 },
    });
    const result = await recover(stream);
    expect(result.entries).toHaveLength(5);
    expect(fake.maxPendingInFlight()).toBe(5);
  });
});
