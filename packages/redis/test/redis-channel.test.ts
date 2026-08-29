import { expect, test } from "bun:test";
import { run, succeed } from "@spilne/perfect-core";
import type { Throws } from "@spilne/perfect-core";
import type { Stream } from "@spilne/perfect-core/stream";
import { RedisChannel } from "../src/redis-channel";
import type { RedisClient } from "../src/redis-client";
import { RedisError } from "../src/redis-error";

test("RedisChannel bridges Redis Pub/Sub to the connect contracts", async () => {
  const listeners = new Set<(...args: any[]) => void>();
  let subscribed = false;
  const subscriber: Partial<RedisClient> = {
    async subscribe() {
      subscribed = true;
    },
    async unsubscribe() {},
    on(event, listener) {
      if (event === "message") listeners.add(listener);
    },
    off(event, listener) {
      if (event === "message") listeners.delete(listener);
    },
    disconnect() {},
  };
  const client: Partial<RedisClient> = {
    duplicate() {
      return subscriber as RedisClient;
    },
    async publish(channel, message) {
      for (const listener of listeners) listener(channel, message);
      return listeners.size;
    },
    async pubsub() {
      return ["events", listeners.size];
    },
  };
  const channel = RedisChannel.make<{ n: number }>({
    redis: client as RedisClient,
    channel: "events",
  });

  const received = run(channel.subscribe().take(1).toArray().orDie());
  while (!subscribed) await Bun.sleep(1);
  expect(await channel.subscriberCount()).toBe(1);
  await channel.publish({ n: 1 });

  expect(await received).toEqual([{ n: 1 }]);
  expect(listeners.size).toBe(0);
});

test("RedisChannel supports pattern subscriptions", async () => {
  const listeners = new Set<(...args: any[]) => void>();
  let subscribed = false;
  const subscriber: Partial<RedisClient> = {
    async psubscribe() {
      subscribed = true;
    },
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
  const channel = RedisChannel.make<{ n: number }>({
    redis: client as RedisClient,
    channel: "events:one",
  });

  const received = run(channel.subscribePattern("events:*").take(1).toArray().orDie());
  while (!subscribed) await Bun.sleep(1);
  await channel.publish({ n: 2 });

  expect(await received).toEqual([{ n: 2 }]);
  expect(listeners.size).toBe(0);
});

test("RedisChannel exposes subscription setup failures as RedisError", async () => {
  const driverFailure = new Error("connection refused");
  const client: Partial<RedisClient> = {
    duplicate() {
      throw driverFailure;
    },
  };
  const channel = RedisChannel.make<number>({
    redis: client as RedisClient,
    channel: "events",
  });
  const source: Stream<number, Throws<RedisError>> = channel.subscribe();

  const error = await run(
    source
      .take(1)
      .toArray()
      .map(() => undefined as RedisError | undefined)
      .catchTag("RedisError", (failure) => succeed(failure)),
  );

  expect(error).toBeInstanceOf(RedisError);
  expect(error).toMatchObject({ operation: "channel.subscribe", cause: driverFailure });
});
