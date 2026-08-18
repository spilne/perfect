import { QueueClosed, fail } from "@perfect/core";
import type { Eff, Throws } from "@perfect/core";
import type { Codec, Sinkable, Streamable } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { Stream } from "@perfect/core/stream";
import { encode, redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError, toRedisError } from "./redis-error";
import { RedisPubSub } from "./redis-pubsub";

export interface RedisChannelConfig<T> {
  redis: RedisClient;
  channel: string;
  codec?: Codec<T>;
  bufferCapacity?: number;
}

export class RedisChannel<T>
  implements Streamable<T, Throws<RedisError>>, Sinkable<T, Throws<RedisError>>
{
  readonly codec: Codec<T>;
  private readonly redis: RedisClient;
  private readonly channel: string;
  private readonly bufferCapacity: number;

  constructor(config: RedisChannelConfig<T>) {
    this.redis = config.redis;
    this.channel = config.channel;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.bufferCapacity = config.bufferCapacity ?? 1024;
  }

  static make<T>(config: RedisChannelConfig<T>): RedisChannel<T> {
    return new RedisChannel(config);
  }

  publish(value: T): Eff<void, Throws<RedisError>> {
    return redisEff("channel.publish", async () => {
      await this.redis.publish(this.channel, encode(this.codec, value));
    });
  }

  subscribe(): Stream<T, Throws<RedisError>> {
    return this.createSubscription({ target: this.channel, pattern: false });
  }

  subscribePattern(pattern: string): Stream<T, Throws<RedisError>> {
    return this.createSubscription({ target: pattern, pattern: true });
  }

  private createSubscription(params: {
    target: string;
    pattern: boolean;
  }): Stream<T, Throws<RedisError>> {
    const pubsub = RedisPubSub.make({
      redis: this.redis,
      channel: this.channel,
      codec: this.codec,
      bufferCapacity: this.bufferCapacity,
    });
    const subscription = params.pattern ? pubsub.subscribePattern(params.target) : pubsub.subscribe;

    return this.flattenSubscription(subscription).onFinalize(pubsub.shutdown());
  }

  private flattenSubscription(
    subscription: Eff<Stream<T, Throws<RedisError> | Throws<QueueClosed>>, Throws<RedisError>>,
  ): Stream<T, Throws<RedisError>> {
    return Stream.fromEffect(subscription)
      .flatMap((stream) => stream)
      .catch<T, Throws<RedisError>>((cause) => {
        if (cause instanceof QueueClosed) return Stream.empty();
        const underlying = cause instanceof RedisError ? cause.cause : cause;
        return Stream.fromEffect(fail(toRedisError("channel.subscribe", underlying)));
      });
  }

  subscriberCount(): Eff<number, Throws<RedisError>> {
    return redisEff("channel.subscriberCount", async () => {
      const raw = await this.redis.pubsub("NUMSUB", this.channel);
      if (!Array.isArray(raw) || raw.length < 2) {
        throw new TypeError("Redis PUBSUB NUMSUB returned an invalid result");
      }
      return Number(raw[1]);
    });
  }

  patternSubscriberCount(): Eff<number, Throws<RedisError>> {
    return redisEff("channel.patternSubscriberCount", async () =>
      Number(await this.redis.pubsub("NUMPAT")),
    );
  }
}
