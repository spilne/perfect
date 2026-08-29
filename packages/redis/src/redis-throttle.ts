import type { Eff, Throttle, Throws } from "@spilne/perfect-core";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";
import { RedisRateLimiter } from "./redis-rate-limiter";

export interface RedisThrottleConfig {
  redis: RedisClient;
  key: string;
  permits: number;
  windowMs: number;
}

export class RedisThrottle implements Throttle<Throws<RedisError>> {
  private constructor(private readonly limiter: RedisRateLimiter) {}

  static make(config: RedisThrottleConfig): RedisThrottle {
    return new RedisThrottle(
      RedisRateLimiter.make({
        redis: config.redis,
        key: config.key,
        limit: config.permits,
        windowMs: config.windowMs,
      }),
    );
  }

  get acquire(): Eff<void, Throws<RedisError>> {
    return this.limiter.acquireWaiting;
  }

  get tryAcquire(): Eff<boolean, Throws<RedisError>> {
    return this.limiter.tryAcquire;
  }

  withPermit<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<RedisError>> {
    return this.limiter.withLimitWaiting(eff);
  }

  get remaining(): Eff<number, Throws<RedisError>> {
    return this.limiter.remaining;
  }

  get nextSlotIn(): Eff<number, Throws<RedisError>> {
    return this.limiter.nextSlotIn;
  }
}
