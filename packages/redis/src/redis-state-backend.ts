import type { Codec, StateBackend, CheckpointName } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { decode, encode, redisKeyFamily } from "./internal";
import type { RedisClient } from "./redis-client";

const COPY_HASH_SCRIPT = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
return redis.call('COPY', KEYS[1], KEYS[2], 'REPLACE')
`;

export interface RedisStateBackendConfig<V = unknown> {
  redis: RedisClient;
  key?: string;
  codec?: Codec<V>;
}

export class RedisStateBackend<V = unknown> implements StateBackend<string, V> {
  private readonly redis: RedisClient;
  private readonly liveKey: string;
  private readonly key: string;
  private readonly codec: Codec<V>;

  constructor(config: RedisStateBackendConfig<V>) {
    this.redis = config.redis;
    this.key = redisKeyFamily(config.key ?? "perfect-state");
    this.liveKey = `${this.key}:live`;
    this.codec = config.codec ?? (JsonCodec as Codec<V>);
  }

  async get(key: string): Promise<V | undefined> {
    const value = await this.redis.hget(this.liveKey, key);
    return value === null ? undefined : decode(this.codec, value);
  }

  async put(key: string, value: V): Promise<void> {
    await this.redis.hset(this.liveKey, key, encode(this.codec, value));
  }

  async delete(key: string): Promise<void> {
    await this.redis.hdel(this.liveKey, key);
  }

  keys(): Promise<string[]> {
    return this.redis.hkeys(this.liveKey);
  }

  async entries(): Promise<[string, V][]> {
    const values = await this.redis.hgetall(this.liveKey);
    return Object.entries(values).map(([key, value]) => [key, decode(this.codec, value)]);
  }

  async checkpoint(params: { name: CheckpointName }): Promise<void> {
    await this.redis.eval(COPY_HASH_SCRIPT, 2, this.liveKey, this.checkpointKey(params.name));
  }

  async restore(params: { name: CheckpointName }): Promise<void> {
    await this.redis.eval(COPY_HASH_SCRIPT, 2, this.checkpointKey(params.name), this.liveKey);
  }

  async clear(): Promise<void> {
    await this.redis.del(this.liveKey);
  }

  private checkpointKey(name: CheckpointName): string {
    return `${this.key}:checkpoint:${name}`;
  }
}
