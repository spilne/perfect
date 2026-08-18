import type { CacheStore, Eff, Throws } from "@perfect/core";
import type { Codec } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { decode, encode, redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

export interface RedisCacheStoreConfig<K, V> {
  redis: RedisClient;
  prefix?: string;
  ttlMs?: number;
  codec?: Codec<V>;
  encodeKey?: (key: K) => string;
}

export class RedisCacheStore<K, V> implements CacheStore<K, V, Throws<RedisError>> {
  private readonly prefix: string;
  private readonly defaultTtlMs: number | undefined;
  private readonly codec: Codec<V>;
  private readonly encodeKey: (key: K) => string;

  constructor(
    private readonly redis: RedisClient,
    config: Omit<RedisCacheStoreConfig<K, V>, "redis"> = {},
  ) {
    this.prefix = config.prefix ?? "perfect:cache:";
    this.defaultTtlMs = config.ttlMs;
    this.codec = config.codec ?? (JsonCodec as Codec<V>);
    this.encodeKey = config.encodeKey ?? String;
  }

  static make<K, V>(config: RedisCacheStoreConfig<K, V>): RedisCacheStore<K, V> {
    return new RedisCacheStore(config.redis, config);
  }

  private key(key: K): string {
    return `${this.prefix}${this.encodeKey(key)}`;
  }

  get(key: K): Eff<V | undefined, Throws<RedisError>> {
    return redisEff("cache.get", async () => {
      const raw = await this.redis.get(this.key(key));
      return raw === null ? undefined : decode(this.codec, raw);
    });
  }

  set(key: K, value: V, ttlMs?: number): Eff<void, Throws<RedisError>> {
    return redisEff("cache.set", async () => {
      const ttl = ttlMs ?? this.defaultTtlMs;
      if (ttl !== undefined && ttl <= 0) {
        await this.redis.del(this.key(key));
        return;
      }
      const encoded = encode(this.codec, value);
      if (ttl === undefined || ttl === Infinity) await this.redis.set(this.key(key), encoded);
      else await this.redis.set(this.key(key), encoded, "PX", ttl);
    });
  }

  delete(key: K): Eff<void, Throws<RedisError>> {
    return redisEff("cache.delete", async () => {
      await this.redis.del(this.key(key));
    });
  }

  has(key: K): Eff<boolean, Throws<RedisError>> {
    return redisEff("cache.has", async () => Boolean(await this.redis.exists(this.key(key))));
  }

  clear(): Eff<void, Throws<RedisError>> {
    return redisEff("cache.clear", async () => {
      let cursor = "0";
      do {
        const [next, keys] = await this.redis.scan(
          cursor,
          "MATCH",
          `${this.prefix}*`,
          "COUNT",
          100,
        );
        cursor = next;
        if (keys.length > 0) await this.redis.del(...keys);
      } while (cursor !== "0");
    });
  }

  get size(): Eff<number, Throws<RedisError>> {
    return redisEff("cache.size", async () => {
      let cursor = "0";
      let total = 0;
      do {
        const [next, keys] = await this.redis.scan(
          cursor,
          "MATCH",
          `${this.prefix}*`,
          "COUNT",
          100,
        );
        cursor = next;
        total += keys.length;
      } while (cursor !== "0");
      return total;
    });
  }
}
