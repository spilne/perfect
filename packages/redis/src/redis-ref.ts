import type { Codec } from "@spilne/perfect-core/connect";
import { JsonCodec } from "@spilne/perfect-core/connect";
import { succeed } from "@spilne/perfect-core";
import type { Eff, Ref, Throws } from "@spilne/perfect-core";
import { type RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";
import { decode, encode, numberResult, redisEff } from "./internal";

const CAS_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`;

export interface RedisRefConfig<A> {
  redis: RedisClient;
  key: string;
  initial: A;
  codec?: Codec<A>;
}

export class RedisRef<A> implements Ref<A, Throws<RedisError>> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly codec: Codec<A>,
  ) {}

  static make<A>(config: RedisRefConfig<A>): Eff<RedisRef<A>, Throws<RedisError>> {
    const codec = config.codec ?? (JsonCodec as Codec<A>);
    const ref = new RedisRef(config.redis, config.key, codec);
    return redisEff("ref.initialize", async () => {
      await config.redis.set(config.key, encode(codec, config.initial), "NX");
      return ref;
    });
  }

  get get(): Eff<A, Throws<RedisError>> {
    return redisEff("ref.get", async () => {
      const raw = await this.redis.get(this.key);
      if (raw === null) throw new Error(`RedisRef key disappeared: ${this.key}`);
      return decode(this.codec, raw);
    });
  }

  set(value: A): Eff<void, Throws<RedisError>> {
    return redisEff("ref.set", async () => {
      await this.redis.set(this.key, encode(this.codec, value));
    });
  }

  update(f: (a: A) => A): Eff<void, Throws<RedisError>> {
    return this.modify((current) => [undefined, f(current)]);
  }

  modify<B>(f: (a: A) => [B, A]): Eff<B, Throws<RedisError>> {
    const attempt = (): Eff<B, Throws<RedisError>> =>
      redisEff("ref.modify.read", async () => {
        const raw = await this.redis.get(this.key);
        if (raw === null) throw new Error(`RedisRef key disappeared: ${this.key}`);
        return { raw, current: decode(this.codec, raw) };
      })
        .map(({ raw, current }) => {
          const [result, next] = f(current);
          return { raw, result, next };
        })
        .flatMap(({ raw, result, next }) =>
          redisEff("ref.modify.compareAndSet", async () =>
            numberResult(
              await this.redis.eval(CAS_SCRIPT, 1, this.key, raw, encode(this.codec, next)),
            ),
          ).flatMap((updated) => (updated === 1 ? succeed(result) : attempt())),
        );
    return attempt();
  }

  getAndSet(value: A): Eff<A, Throws<RedisError>> {
    return this.modify((current) => [current, value]);
  }

  getAndUpdate(f: (a: A) => A): Eff<A, Throws<RedisError>> {
    return this.modify((current) => [current, f(current)]);
  }

  updateAndGet(f: (a: A) => A): Eff<A, Throws<RedisError>> {
    return this.modify((current) => {
      const next = f(current);
      return [next, next];
    });
  }
}
