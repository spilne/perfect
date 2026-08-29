import { fail, sleep, succeed } from "@spilne/perfect-core";
import type { Eff, RateLimitExceeded, RateLimiter, Throws } from "@spilne/perfect-core";
import { redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const RATE_LIMIT_SCRIPT = `
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local consume = tonumber(ARGV[3])
local member = ARGV[4]
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
local granted = 0
if count < limit then
  granted = 1
  if consume == 1 then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    count = count + 1
  end
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
local retry = 0
local reset = now
if count >= limit and #oldest > 0 then retry = math.max(1, tonumber(oldest[2]) + window - now) end
if #newest > 0 then reset = tonumber(newest[2]) + window end
return { granted, retry, math.max(0, limit - count), reset, now }
`;

interface RateLimitState {
  readonly granted: boolean;
  readonly retryAfterMs: number;
  readonly remaining: number;
  readonly resetAt: number;
  readonly now: number;
}

export interface RedisRateLimiterConfig {
  redis: RedisClient;
  key: string;
  limit: number;
  windowMs: number;
}

export class RedisRateLimiter implements RateLimiter<Throws<RedisError>> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly limit: number,
    private readonly windowMs: number,
  ) {}

  static make(config: RedisRateLimiterConfig): RedisRateLimiter {
    if (!Number.isInteger(config.limit) || config.limit < 1) {
      throw new Error("RedisRateLimiter.make: limit must be a positive integer");
    }
    if (!(config.windowMs > 0)) {
      throw new Error("RedisRateLimiter.make: windowMs must be positive");
    }
    return new RedisRateLimiter(config.redis, config.key, config.limit, config.windowMs);
  }

  private inspect(consume: boolean, operation: string): Eff<RateLimitState, Throws<RedisError>> {
    return redisEff(operation, async () => {
      const result = await this.redis.eval(
        RATE_LIMIT_SCRIPT,
        1,
        this.key,
        this.windowMs,
        this.limit,
        consume ? 1 : 0,
        crypto.randomUUID(),
      );
      if (!Array.isArray(result) || result.length < 5) {
        throw new TypeError("Redis rate limiter returned an invalid result");
      }
      const numbers = result.map(Number);
      return {
        granted: numbers[0] === 1,
        retryAfterMs: Math.max(0, numbers[1] ?? 0),
        remaining: Math.max(0, numbers[2] ?? 0),
        resetAt: numbers[3] ?? 0,
        now: numbers[4] ?? 0,
      };
    });
  }

  get acquire(): Eff<void, Throws<RedisError> | Throws<RateLimitExceeded>> {
    return this.inspect(true, "rateLimiter.acquire").flatMap((state) =>
      state.granted
        ? succeed(undefined)
        : fail<RateLimitExceeded>({
            _tag: "RateLimitExceeded",
            retryAfterMs: state.retryAfterMs,
          }),
    );
  }

  get tryAcquire(): Eff<boolean, Throws<RedisError>> {
    return this.inspect(true, "rateLimiter.tryAcquire").map((state) => state.granted);
  }

  get acquireWaiting(): Eff<void, Throws<RedisError>> {
    const loop = (): Eff<void, Throws<RedisError>> =>
      this.inspect(true, "rateLimiter.acquireWaiting").flatMap((state) =>
        state.granted
          ? succeed(undefined)
          : sleep(Math.max(1, state.retryAfterMs)).flatMap(() => loop()),
      );
    return loop();
  }

  withLimit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<RedisError> | Throws<RateLimitExceeded>> {
    return this.acquire.flatMap(() => eff);
  }

  withLimitWaiting<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<RedisError>> {
    return this.acquireWaiting.flatMap(() => eff);
  }

  get remaining(): Eff<number, Throws<RedisError>> {
    return this.inspect(false, "rateLimiter.remaining").map((state) => state.remaining);
  }

  get resetAt(): Eff<number, Throws<RedisError>> {
    return this.inspect(false, "rateLimiter.resetAt").map((state) => state.resetAt);
  }

  get nextSlotIn(): Eff<number, Throws<RedisError>> {
    return this.inspect(false, "rateLimiter.nextSlotIn").map((state) => state.retryAfterMs);
  }
}
