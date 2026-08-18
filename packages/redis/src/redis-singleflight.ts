import { Cause, ensuring, fail, succeed } from "@perfect/core";
import type { Eff, Singleflight, Throws } from "@perfect/core";
import type { Codec } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { redisBlocking, redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const RELEASE_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

type Outcome<A, E> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly error: E };

interface EncodedOutcome {
  readonly ok: boolean;
  readonly data: unknown;
}

export interface RedisSingleflightConfig {
  redis: RedisClient;
  prefix?: string;
  timeoutMs?: number;
  codec?: Codec<unknown>;
}

export class RedisSingleflight implements Singleflight<Throws<RedisError>> {
  private readonly prefix: string;
  private readonly timeoutMs: number;
  private readonly codec: Codec<unknown>;

  constructor(
    private readonly redis: RedisClient,
    config: Omit<RedisSingleflightConfig, "redis"> = {},
  ) {
    this.prefix = config.prefix ?? "perfect:singleflight:";
    this.timeoutMs = config.timeoutMs ?? 30_000;
    this.codec = config.codec ?? JsonCodec;
  }

  static make(config: RedisSingleflightConfig): RedisSingleflight {
    return new RedisSingleflight(config.redis, config);
  }

  do<A, E>(key: string, eff: Eff<A, Throws<E>>): Eff<A, Throws<RedisError> | Throws<E>> {
    const lockKey = `${this.prefix}${key}:lock`;

    const attempt = (): Eff<A, Throws<RedisError> | Throws<E>> => {
      const requestId = crypto.randomUUID();
      return redisEff("singleflight.acquire", () =>
        this.redis.set(lockKey, requestId, "NX", "PX", this.timeoutMs),
      ).flatMap((acquired) =>
        acquired
          ? this.runLeader(lockKey, requestId, eff)
          : redisEff("singleflight.owner", () => this.redis.get(lockKey)).flatMap((owner) =>
              owner === null ? attempt() : this.runFollower(`${lockKey}:result:${owner}`, attempt),
            ),
      );
    };

    return attempt();
  }

  private runLeader<A, E>(
    lockKey: string,
    requestId: string,
    eff: Eff<A, Throws<E>>,
  ): Eff<A, Throws<RedisError> | Throws<E>> {
    const resultKey = `${lockKey}:result:${requestId}`;
    const captured: Eff<Outcome<A, E>, never> = eff
      .map((value): Outcome<A, E> => ({ ok: true, value }))
      .catchAllCause((cause) => {
        const typed = Cause.firstFail(cause);
        return succeed({
          ok: false,
          error: (typed === null ? Cause.squash(cause) : typed.value) as E,
        } as Outcome<A, E>);
      });

    return captured.flatMap((outcome) =>
      ensuring(
        redisEff("singleflight.publish", async () => {
          const encoded: EncodedOutcome = {
            ok: outcome.ok,
            data: this.codec.encode(outcome.ok ? outcome.value : outcome.error),
          };
          await this.redis.rpush(resultKey, JSON.stringify(encoded));
          await this.redis.pexpire(resultKey, this.timeoutMs);
        }),
        redisEff("singleflight.release", async () => {
          await this.redis.eval(RELEASE_SCRIPT, 1, lockKey, requestId);
        }),
      ).flatMap(() => (outcome.ok ? succeed(outcome.value) : fail(outcome.error))),
    );
  }

  private runFollower<A, E>(
    resultKey: string,
    retry: () => Eff<A, Throws<RedisError> | Throws<E>>,
  ): Eff<A, Throws<RedisError> | Throws<E>> {
    return redisBlocking(this.redis, "singleflight.await", (client) =>
      client.brpop(resultKey, Math.max(0.001, this.timeoutMs / 1000)),
    ).flatMap((result) => {
      if (result === null) return retry();
      return redisEff("singleflight.republish", async () => {
        await this.redis.rpush(resultKey, result[1]);
        await this.redis.pexpire(resultKey, this.timeoutMs);
        const encoded = JSON.parse(result[1]) as EncodedOutcome;
        return {
          ok: encoded.ok,
          data: this.codec.decode(encoded.data),
        };
      }).flatMap((outcome) => (outcome.ok ? succeed(outcome.data as A) : fail(outcome.data as E)));
    });
  }
}
