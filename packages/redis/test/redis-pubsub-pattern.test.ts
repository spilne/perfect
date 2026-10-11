import { expect, test } from "bun:test";
import { RedisPubSub } from "../src/redis-pubsub";
import type { RedisClient } from "../src/redis-client";
import { fakeListeners } from "./fake-listeners";
import { runUnchecked } from "./run-unchecked";

test("RedisPubSub exposes typed pattern subscriptions", async () => {
  const listeners = fakeListeners();
  const subscriber: Partial<RedisClient> = {
    async psubscribe() {},
    async punsubscribe() {},
    on(event, listener) {
      listeners[event].add(listener);
    },
    off(event, listener) {
      listeners[event].delete(listener);
    },
    disconnect() {},
  };
  const client: Partial<RedisClient> = {
    duplicate() {
      return subscriber as RedisClient;
    },
    async publish(channel, message) {
      if (channel.startsWith("events:")) {
        for (const listener of listeners.pmessage) listener("events:*", channel, message);
      }
      return listeners.pmessage.size;
    },
  };
  const pubsub = RedisPubSub.make<{ n: number }>({
    redis: client as RedisClient,
    channel: "events:one",
  });

  const stream = await runUnchecked(pubsub.subscribePattern("events:*"));
  const received = runUnchecked(stream.take(1).toArray());
  expect(await runUnchecked(pubsub.publish({ n: 1 }))).toBe(true);

  expect(await received).toEqual([{ n: 1 }]);
  expect(listeners.pmessage.size).toBe(0);
});

test("RedisPubSub fails a subscription on malformed messages", async () => {
  const listeners = fakeListeners();
  const subscriber: Partial<RedisClient> = {
    async subscribe() {},
    async unsubscribe() {},
    on(event, listener) {
      listeners[event].add(listener);
    },
    off(event, listener) {
      listeners[event].delete(listener);
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

  const stream = await runUnchecked(pubsub.subscribe);
  const received = runUnchecked(stream.take(1).toArray());
  for (const listener of listeners.message) listener("events", "{");

  await expect(received).rejects.toMatchObject({
    _tag: "RedisError",
    operation: "pubsub.decode",
  });
  expect(listeners.message.size).toBe(0);
});
