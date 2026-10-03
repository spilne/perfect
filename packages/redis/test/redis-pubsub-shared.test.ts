import { describe, expect, test } from "bun:test";
import { run, type Eff } from "@spilne/perfect-core";
import { RedisPubSub } from "../src/redis-pubsub";
import type { RedisClient } from "../src/redis-client";

const unsafeRun = <A>(effect: Eff<A, unknown>): Promise<A> => run(effect as any);

// A fake Redis that counts subscriber connections and SUBSCRIBE calls.
function fakeRedis() {
  let connections = 0;
  let closed = 0;
  const subscribeCalls: string[] = [];
  const unsubscribeCalls: string[] = [];
  const listeners = new Map<string, Set<(...args: any[]) => void>>();
  const subscriber: Partial<RedisClient> = {
    async subscribe(channel) {
      subscribeCalls.push(channel);
    },
    async unsubscribe(channel) {
      unsubscribeCalls.push(channel);
    },
    on(event, listener) {
      const set = listeners.get(event) ?? new Set();
      set.add(listener);
      listeners.set(event, set);
    },
    off(event, listener) {
      listeners.get(event)?.delete(listener);
    },
    disconnect() {
      closed++;
    },
  };
  const client: Partial<RedisClient> = {
    duplicate() {
      connections++;
      return subscriber as RedisClient;
    },
    async publish(channel, message) {
      for (const listener of listeners.get("message") ?? []) listener(channel, message);
      return 1;
    },
  };
  return {
    client: client as RedisClient,
    connections: () => connections,
    closed: () => closed,
    subscribeCalls,
    unsubscribeCalls,
  };
}

describe("RedisPubSub shares one subscriber connection", () => {
  test("many subscriptions open one connection and subscribe once", async () => {
    const fake = fakeRedis();
    const pubsub = RedisPubSub.make<number>({ redis: fake.client, channel: "events" });

    const streams = await Promise.all([1, 2, 3].map(() => unsafeRun(pubsub.subscribe)));
    const received = streams.map((stream) => unsafeRun(stream.take(1).toArray()));
    await unsafeRun(pubsub.publish(42));

    expect(await Promise.all(received)).toEqual([[42], [42], [42]]);
    expect(fake.connections()).toBe(1);
    expect(fake.subscribeCalls).toEqual(["events"]);
  });

  test("the channel stays subscribed until the last subscriber leaves", async () => {
    const fake = fakeRedis();
    const pubsub = RedisPubSub.make<number>({ redis: fake.client, channel: "events" });

    const first = await unsafeRun(pubsub.subscribe);
    const second = await unsafeRun(pubsub.subscribe);
    // The first subscriber takes one message and leaves.
    const firstDone = unsafeRun(first.take(1).toArray());
    const secondGot = unsafeRun(second.take(2).toArray());
    await unsafeRun(pubsub.publish(1));
    await firstDone;
    expect(fake.unsubscribeCalls).toEqual([]);

    // The second still receives, then leaves too: now Redis unsubscribes
    // and the connection is closed.
    await unsafeRun(pubsub.publish(2));
    expect(await secondGot).toEqual([1, 2]);
    expect(fake.unsubscribeCalls).toEqual(["events"]);
    expect(fake.closed()).toBe(1);
  });
});
