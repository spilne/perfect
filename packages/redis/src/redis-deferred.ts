import { fail as failEff, succeed as succeedEff } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { JsonCodec } from "@spilne/perfect-core/connect";
import type { Deferred, Eff, Throws } from "@spilne/perfect-core";
import { numberResult, redisBlocking, redisEff, redisKeyFamily } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const COMPLETE_SCRIPT = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[2], '1')
return 1
`;

type Envelope =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly error: unknown };

type ReadResult<A, E> =
  | { readonly done: false }
  | { readonly done: true; readonly ok: true; readonly value: A }
  | { readonly done: true; readonly ok: false; readonly error: E };

export interface RedisDeferredConfig<A, E> {
  redis: RedisClient;
  key: string;
  valueCodec?: Codec<A>;
  errorCodec?: Codec<E>;
  timeoutMs?: number;
}

export class RedisDeferred<A, E = never> implements Deferred<A, E, Throws<RedisError>> {
  private readonly valueKey: string;
  private readonly notifyKey: string;
  private readonly valueCodec: Codec<A>;
  private readonly errorCodec: Codec<E>;
  private readonly timeoutMs: number;

  private constructor(config: RedisDeferredConfig<A, E>) {
    this.redis = config.redis;
    const key = redisKeyFamily(config.key);
    this.valueKey = `${key}:value`;
    this.notifyKey = `${key}:notify`;
    this.valueCodec = config.valueCodec ?? (JsonCodec as Codec<A>);
    this.errorCodec = config.errorCodec ?? (JsonCodec as Codec<E>);
    this.timeoutMs = config.timeoutMs ?? 0;
  }

  private readonly redis: RedisClient;

  static make<A, E = never>(config: RedisDeferredConfig<A, E>): RedisDeferred<A, E> {
    return new RedisDeferred(config);
  }

  succeed(value: A): Eff<boolean, Throws<RedisError>> {
    return this.complete(() => ({ ok: true, value: this.valueCodec.encode(value) }));
  }

  fail(error: E): Eff<boolean, Throws<RedisError>> {
    return this.complete(() => ({ ok: false, error: this.errorCodec.encode(error) }));
  }

  private complete(envelope: () => Envelope): Eff<boolean, Throws<RedisError>> {
    return redisEff("deferred.complete", async () => {
      const result = await this.redis.eval(
        COMPLETE_SCRIPT,
        2,
        this.valueKey,
        this.notifyKey,
        JSON.stringify(envelope()),
      );
      return numberResult(result) === 1;
    });
  }

  get await(): Eff<A, Throws<RedisError> | Throws<E>> {
    const read = (): Eff<ReadResult<A, E>, Throws<RedisError>> =>
      redisEff("deferred.read", async () => {
        const raw = await this.redis.get(this.valueKey);
        if (raw === null) return { done: false } as const;
        const envelope = JSON.parse(raw) as Envelope;
        return envelope.ok
          ? ({ done: true, ok: true, value: this.valueCodec.decode(envelope.value) } as const)
          : ({ done: true, ok: false, error: this.errorCodec.decode(envelope.error) } as const);
      });

    return read().flatMap((result) => {
      if (result.done) return result.ok ? succeedEff(result.value) : failEff(result.error);
      const timeoutSeconds = this.timeoutMs === 0 ? 0 : Math.max(0.001, this.timeoutMs / 1000);
      return redisBlocking(this.redis, "deferred.await", async (client) => {
        const result = await client.brpop(this.notifyKey, timeoutSeconds);
        if (!result) throw new Error(`RedisDeferred timed out after ${this.timeoutMs}ms`);
      })
        .flatMap(() => redisEff("deferred.notify", () => this.redis.rpush(this.notifyKey, "1")))
        .flatMap(() => read())
        .flatMap((resolved): Eff<A, Throws<E> | Throws<RedisError>> => {
          if (!resolved.done) {
            return failEff(new RedisError({ operation: "deferred.await", cause: "value missing" }));
          }
          return resolved.ok ? succeedEff(resolved.value) : failEff(resolved.error);
        });
    });
  }

  get isDone(): Eff<boolean, Throws<RedisError>> {
    return redisEff("deferred.isDone", async () => Boolean(await this.redis.exists(this.valueKey)));
  }
}
