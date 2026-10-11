import type { RedisClientEvents } from "../src/redis-client";

export type FakeListeners = { [K in keyof RedisClientEvents]: Set<RedisClientEvents[K]> };

// Listener sets for a fake subscriber connection, one per event, so a fake
// on()/off() can store each listener with its real type.
export function fakeListeners(): FakeListeners {
  return { message: new Set(), pmessage: new Set(), error: new Set(), close: new Set() };
}
