import { fail, sleep, succeed } from "@perfect/core";
import type { Codec } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import type { Eff, Queue, Throws } from "@perfect/core";
import { QueueClosed } from "@perfect/core";
import { decode, encode, numberResult, redisBlocking, redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const OFFER_SCRIPT = `
if redis.call('HGET', KEYS[2], 'closed') == '1' then return -1 end
local capacity = tonumber(ARGV[1])
if capacity >= 0 and redis.call('LLEN', KEYS[1]) >= capacity then return 0 end
redis.call('LPUSH', KEYS[1], ARGV[2])
return 1
`;

const TAKE_ALL_SCRIPT = `
local values = redis.call('LRANGE', KEYS[1], 0, -1)
redis.call('DEL', KEYS[1])
return values
`;

export interface RedisQueueConfig<A> {
  redis: RedisClient;
  key: string;
  capacity?: number;
  pollIntervalMs?: number;
  codec?: Codec<A>;
}

export class RedisQueue<A> implements Queue<A, Throws<RedisError>> {
  private readonly dataKey: string;
  private readonly metaKey: string;
  private readonly capacity: number;
  private readonly pollIntervalMs: number;
  private readonly codec: Codec<A>;

  private constructor(
    private readonly redis: RedisClient,
    config: Omit<RedisQueueConfig<A>, "redis">,
  ) {
    this.dataKey = `${config.key}:data`;
    this.metaKey = `${config.key}:meta`;
    this.capacity = config.capacity ?? -1;
    this.pollIntervalMs = config.pollIntervalMs ?? 100;
    this.codec = config.codec ?? (JsonCodec as Codec<A>);
  }

  static make<A>(config: RedisQueueConfig<A>): RedisQueue<A> {
    if (
      config.capacity !== undefined &&
      (!Number.isInteger(config.capacity) || config.capacity < 1)
    ) {
      throw new Error("RedisQueue.make: capacity must be a positive integer");
    }
    return new RedisQueue(config.redis, config);
  }

  offer(value: A): Eff<boolean, Throws<RedisError> | Throws<QueueClosed>> {
    const loop = (): Eff<boolean, Throws<RedisError> | Throws<QueueClosed>> =>
      redisEff("queue.offer", async () =>
        numberResult(
          await this.redis.eval(
            OFFER_SCRIPT,
            2,
            this.dataKey,
            this.metaKey,
            this.capacity,
            encode(this.codec, value),
          ),
        ),
      ).flatMap((result) => {
        if (result === 1) return succeed(true);
        if (result === -1) return fail(new QueueClosed());
        return sleep(this.pollIntervalMs).flatMap(() => loop());
      });
    return loop();
  }

  take(): Eff<A, Throws<RedisError> | Throws<QueueClosed>> {
    const loop = (): Eff<A, Throws<RedisError> | Throws<QueueClosed>> =>
      redisBlocking(this.redis, "queue.take", (client) =>
        client.brpop(this.dataKey, Math.max(0.001, this.pollIntervalMs / 1000)),
      ).flatMap((result) => {
        if (result) return redisEff("queue.decode", async () => decode(this.codec, result[1]));
        return this.isClosed.flatMap((closed) => (closed ? fail(new QueueClosed()) : loop()));
      });
    return loop();
  }

  takeAll(): Eff<A[], Throws<RedisError>> {
    return redisEff("queue.takeAll", async () => {
      const result = await this.redis.eval(TAKE_ALL_SCRIPT, 1, this.dataKey);
      if (!Array.isArray(result)) throw new TypeError("Redis queue returned an invalid list");
      return result
        .map(String)
        .reverse()
        .map((raw) => decode(this.codec, raw));
    });
  }

  offerAll(values: A[]): Eff<void, Throws<RedisError> | Throws<QueueClosed>> {
    return values.reduce<Eff<void, Throws<RedisError> | Throws<QueueClosed>>>(
      (effect, value) => effect.flatMap(() => this.offer(value).map(() => undefined)),
      succeed(undefined),
    );
  }

  get size(): Eff<number, Throws<RedisError>> {
    return redisEff("queue.size", () => this.redis.llen(this.dataKey));
  }

  get isClosed(): Eff<boolean, Throws<RedisError>> {
    return redisEff(
      "queue.isClosed",
      async () => (await this.redis.hget(this.metaKey, "closed")) === "1",
    );
  }

  get isShutdown(): Eff<boolean, Throws<RedisError>> {
    return this.isClosed;
  }

  close(): Eff<void, Throws<RedisError>> {
    return redisEff("queue.close", async () => {
      await this.redis.hset(this.metaKey, "closed", "1");
    });
  }

  shutdown(): Eff<void, Throws<RedisError>> {
    return this.close();
  }

  get awaitClose(): Eff<void, Throws<RedisError>> {
    const loop = (): Eff<void, Throws<RedisError>> =>
      this.isClosed.flatMap((closed) =>
        closed ? succeed(undefined) : sleep(this.pollIntervalMs).flatMap(() => loop()),
      );
    return loop();
  }

  get awaitShutdown(): Eff<void, Throws<RedisError>> {
    return this.awaitClose;
  }
}
