import { ensuring, sleep, succeed } from "@spilne/perfect-core";
import type { Eff, Semaphore, Throws } from "@spilne/perfect-core";
import { numberResult, redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const ACQUIRE_SCRIPT = `
local available = tonumber(redis.call('GET', KEYS[1]) or '0')
local requested = tonumber(ARGV[1])
if available < requested then return -1 end
redis.call('DECRBY', KEYS[1], requested)
return available - requested
`;

const RELEASE_SCRIPT = `
local available = tonumber(redis.call('GET', KEYS[1]) or '0')
local released = tonumber(ARGV[1])
local maximum = tonumber(ARGV[2])
local next = math.min(maximum, available + released)
redis.call('SET', KEYS[1], next)
return next
`;

export interface RedisSemaphoreConfig {
  redis: RedisClient;
  key: string;
  permits: number;
  pollIntervalMs?: number;
}

export class RedisSemaphore implements Semaphore<Throws<RedisError>> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly permits: number,
    private readonly pollIntervalMs: number,
  ) {}

  static make(config: RedisSemaphoreConfig): Eff<RedisSemaphore, Throws<RedisError>> {
    if (!Number.isInteger(config.permits) || config.permits < 1) {
      throw new Error("RedisSemaphore.make: permits must be a positive integer");
    }
    const semaphore = new RedisSemaphore(
      config.redis,
      config.key,
      config.permits,
      config.pollIntervalMs ?? 25,
    );
    return redisEff("semaphore.initialize", async () => {
      await config.redis.set(config.key, String(config.permits), "NX");
      return semaphore;
    });
  }

  private acquireMany(n: number): Eff<void, Throws<RedisError>> {
    const loop = (): Eff<void, Throws<RedisError>> =>
      redisEff("semaphore.acquire", async () =>
        numberResult(await this.redis.eval(ACQUIRE_SCRIPT, 1, this.key, n)),
      ).flatMap((remaining) =>
        remaining >= 0 ? succeed(undefined) : sleep(this.pollIntervalMs).flatMap(() => loop()),
      );
    return loop();
  }

  private releaseMany(n: number): Eff<void, Throws<RedisError>> {
    return redisEff("semaphore.release", async () => {
      await this.redis.eval(RELEASE_SCRIPT, 1, this.key, n, this.permits);
    });
  }

  acquire(): Eff<void, Throws<RedisError>> {
    return this.acquireMany(1);
  }

  release(): Eff<void, Throws<RedisError>> {
    return this.releaseMany(1);
  }

  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<RedisError>> {
    return this.withPermits(1, eff);
  }

  withPermits<A, S>(n: number, eff: Eff<A, S>): Eff<A, S | Throws<RedisError>> {
    if (n <= 0) return eff;
    if (!Number.isInteger(n) || n > this.permits) {
      throw new Error(`RedisSemaphore.withPermits: n must be between 1 and ${this.permits}`);
    }
    return this.acquireMany(n).flatMap(() => ensuring(eff, this.releaseMany(n)));
  }

  get available(): Eff<number, Throws<RedisError>> {
    return redisEff("semaphore.available", async () =>
      Number((await this.redis.get(this.key)) ?? 0),
    );
  }
}
