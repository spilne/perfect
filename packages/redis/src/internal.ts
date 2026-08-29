import { async, fail, fromPromise, succeed } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { Eff, Throws } from "@spilne/perfect-core";
import { closeRedisClient, type RedisClient } from "./redis-client";
import { RedisError, toRedisError } from "./redis-error";

export function redisEff<A>(
  operation: string,
  thunk: () => Promise<A>,
): Eff<A, Throws<RedisError>> {
  return fromPromise(thunk, (cause) => toRedisError(operation, cause));
}

export function redisBlocking<A>(
  redis: RedisClient,
  operation: string,
  run: (client: RedisClient) => Promise<A>,
): Eff<A, Throws<RedisError>> {
  return async<A, RedisError>((resume) => {
    let client: RedisClient | null = null;
    let canceled = false;

    void Promise.resolve(redis.duplicate()).then(
      async (duplicate) => {
        client = duplicate;
        try {
          const value = await run(duplicate);
          if (!canceled) resume(succeed(value));
        } catch (cause) {
          if (!canceled) resume(fail(toRedisError(operation, cause)));
        } finally {
          closeRedisClient(duplicate);
        }
      },
      (cause) => {
        if (!canceled) resume(fail(toRedisError(operation, cause)));
      },
    );

    return () => {
      canceled = true;
      if (client) closeRedisClient(client);
    };
  });
}

export function encode<T>(codec: Codec<T>, value: T): string {
  return JSON.stringify(codec.encode(value));
}

export function decode<T>(codec: Codec<T>, value: string): T {
  return codec.decode(JSON.parse(value));
}

export function numberResult(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number(value);
  throw new TypeError(`Expected numeric Redis result, received ${String(value)}`);
}

export function redisKeyFamily(key: string): string {
  return /\{[^{}]+\}/.test(key) ? key : `{${key}}`;
}
