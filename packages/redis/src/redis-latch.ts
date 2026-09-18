import { succeed } from "@spilne/perfect-core";
import type { Eff, Latch, Throws } from "@spilne/perfect-core";
import { numberResult, redisBlocking, redisEff, redisKeyFamily } from "./internal.js";
import type { RedisClient } from "./redis-client.js";
import { RedisError } from "./redis-error.js";

const COUNT_DOWN_SCRIPT = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current <= 0 then return 0 end
local next = math.max(0, current - tonumber(ARGV[1]))
redis.call('SET', KEYS[1], next)
if next == 0 then redis.call('LPUSH', KEYS[2], '1') end
return next
`;

export interface RedisLatchConfig {
  redis: RedisClient;
  key: string;
  count: number;
}

export class RedisLatch implements Latch<Throws<RedisError>> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly countKey: string,
    private readonly notifyKey: string,
  ) {}

  static make(config: RedisLatchConfig): Eff<RedisLatch, Throws<RedisError>> {
    if (!Number.isInteger(config.count) || config.count < 0) {
      throw new Error("RedisLatch.make: count must be a non-negative integer");
    }
    const key = redisKeyFamily(config.key);
    const latch = new RedisLatch(config.redis, `${key}:count`, `${key}:notify`);
    return redisEff("latch.initialize", async () => {
      await config.redis.set(`${key}:count`, String(config.count), "NX");
      return latch;
    });
  }

  get countDown(): Eff<void, Throws<RedisError>> {
    return this.countDownBy(1);
  }

  countDownBy(n: number): Eff<void, Throws<RedisError>> {
    if (n <= 0) return succeed(undefined);
    return redisEff("latch.countDown", async () => {
      await this.redis.eval(COUNT_DOWN_SCRIPT, 2, this.countKey, this.notifyKey, n);
    });
  }

  get await(): Eff<void, Throws<RedisError>> {
    return this.remaining.flatMap((remaining) => {
      if (remaining === 0) return succeed(undefined);
      return redisBlocking(this.redis, "latch.await", (client) => client.brpop(this.notifyKey, 0), {
        // Another waiter needs the wake-up token this one took.
        giveBack: (token) => {
          if (token) void this.redis.rpush(this.notifyKey, "1").catch(() => {});
        },
      }).flatMap(() =>
        redisEff("latch.notify", async () => {
          await this.redis.rpush(this.notifyKey, "1");
        }),
      );
    });
  }

  get remaining(): Eff<number, Throws<RedisError>> {
    return redisEff("latch.remaining", async () =>
      Math.max(0, numberResult((await this.redis.get(this.countKey)) ?? "0")),
    );
  }
}
