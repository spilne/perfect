import { describe, expect, test } from "bun:test";
import { run, type Eff } from "@spilne/perfect-core";
import { RedisPubSub } from "../src/redis-pubsub";
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
});
