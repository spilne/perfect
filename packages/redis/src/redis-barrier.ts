import { succeed } from "@perfect/core";
import type { Barrier, Eff, Throws } from "@perfect/core";
import { numberResult, redisBlocking, redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const ARRIVE_SCRIPT = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local parties = tonumber(ARGV[1])
if current >= parties then return parties end
local next = current + 1
redis.call('SET', KEYS[1], next)
if next == parties then redis.call('LPUSH', KEYS[2], '1') end
return next
`;

export interface RedisBarrierConfig {
  redis: RedisClient;
  key: string;
  parties: number;
}

export class RedisBarrier implements Barrier<Throws<RedisError>> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly countKey: string,
    private readonly notifyKey: string,
    private readonly parties: number,
  ) {}

  static make(config: RedisBarrierConfig): Eff<RedisBarrier, Throws<RedisError>> {
    if (!Number.isInteger(config.parties) || config.parties < 1) {
      throw new Error("RedisBarrier.make: parties must be a positive integer");
    }
    const barrier = new RedisBarrier(
      config.redis,
      `${config.key}:count`,
      `${config.key}:notify`,
      config.parties,
    );
    return redisEff("barrier.initialize", async () => {
      await config.redis.set(`${config.key}:count`, "0", "NX");
      return barrier;
    });
  }

  get await(): Eff<void, Throws<RedisError>> {
    return redisEff("barrier.arrive", async () =>
      numberResult(
        await this.redis.eval(ARRIVE_SCRIPT, 2, this.countKey, this.notifyKey, this.parties),
      ),
    ).flatMap((arrived) => {
      if (arrived >= this.parties) return succeed(undefined);
      return redisBlocking(this.redis, "barrier.await", async (client) => {
        await client.brpop(this.notifyKey, 0);
      }).flatMap(() =>
        redisEff("barrier.notify", async () => {
          await this.redis.rpush(this.notifyKey, "1");
        }),
      );
    });
  }

  get arrived(): Eff<number, Throws<RedisError>> {
    return redisEff("barrier.arrived", async () =>
      Math.min(this.parties, numberResult((await this.redis.get(this.countKey)) ?? "0")),
    );
  }
}
