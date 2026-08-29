import { expect, test } from "bun:test";
import { run, succeed } from "@spilne/perfect-core";
import { RedisCacheStore } from "../src/redis-cache-store";
import { RedisDeferred } from "../src/redis-deferred";
import type { RedisClient } from "../src/redis-client";

test("driver failures remain typed RedisError values", async () => {
  const redis = {
    get: async () => {
      throw new Error("connection lost");
    },
  } as unknown as RedisClient;
  const cache = RedisCacheStore.make<string, number>({ redis });

  const recovered = cache
    .get("key")
    .map(() => ({ operation: "unexpected", cause: undefined as unknown }))
    .catchTag("RedisError", (error) => succeed({ operation: error.operation, cause: error.cause }));

  const result = await run(recovered);
  expect(result.operation).toBe("cache.get");
  expect(result.cause).toBeInstanceOf(Error);
});

test("corrupted deferred payloads fail as RedisError rather than defects", async () => {
  const redis = {
    get: async () => "not-json",
  } as unknown as RedisClient;
  const deferred = RedisDeferred.make<number>({ redis, key: "deferred" });

  const operation = await run(
    deferred.await
      .map(() => "unexpected")
      .catchTag("RedisError", (error) => succeed(error.operation)),
  );

  expect(operation).toBe("deferred.read");
});
