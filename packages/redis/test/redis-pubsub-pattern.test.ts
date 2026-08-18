import { expect, test } from "bun:test";
import { run, type Eff } from "@perfect/core";
import { RedisPubSub } from "../src/redis-pubsub";
import type { RedisClient } from "../src/redis-client";

const unsafeRun = <A>(effect: Eff<A, unknown>): Promise<A> => run(effect as any);

test("RedisPubSub exposes typed pattern subscriptions", async () => {
  const listeners = new Set<(...args: any[]) => void>();
  const subscriber: Partial<RedisClient> = {
    async psubscribe() {},
    async punsubscribe() {},
    on(event, listener) {
      if (event === "pmessage") listeners.add(listener);
    },
    off(event, listener) {
      if (event === "pmessage") listeners.delete(listener);
    },
    disconnect() {},
  };
  const client: Partial<RedisClient> = {
    duplicate() {
      return subscriber as RedisClient;
    },
    async publish(channel, message) {
      if (channel.startsWith("events:")) {
        for (const listener of listeners) listener("events:*", channel, message);
      }
      return listeners.size;
    },
  };
  const pubsub = RedisPubSub.make<{ n: number }>({
    redis: client as RedisClient,
    channel: "events:one",
  });

  const stream = await unsafeRun(pubsub.subscribePattern("events:*"));
  const received = unsafeRun(stream.take(1).toArray());
  expect(await unsafeRun(pubsub.publish({ n: 1 }))).toBe(true);

  expect(await received).toEqual([{ n: 1 }]);
  expect(listeners.size).toBe(0);
});

test("RedisPubSub fails a subscription on malformed messages", async () => {
  const listeners = new Map<string, Set<(...args: any[]) => void>>();
  const subscriber: Partial<RedisClient> = {
    async subscribe() {},
    async unsubscribe() {},
    on(event, listener) {
      const eventListeners = listeners.get(event) ?? new Set();
      eventListeners.add(listener);
      listeners.set(event, eventListeners);
    },
    off(event, listener) {
      listeners.get(event)?.delete(listener);
    },
    disconnect() {},
  };
  const client: Partial<RedisClient> = {
    duplicate() {
      return subscriber as RedisClient;
    },
  };
  const pubsub = RedisPubSub.make<number>({
    redis: client as RedisClient,
    channel: "events",
  });

  const stream = await unsafeRun(pubsub.subscribe);
  const received = unsafeRun(stream.take(1).toArray());
  for (const listener of listeners.get("message") ?? []) listener("events", "{");

  await expect(received).rejects.toMatchObject({
    _tag: "RedisError",
    operation: "pubsub.decode",
  });
  expect(listeners.get("message")?.size).toBe(0);
});
