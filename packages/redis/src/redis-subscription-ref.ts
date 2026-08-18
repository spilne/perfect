import type { Eff, QueueClosed, SubscriptionRef, Throws } from "@perfect/core";
import type { Codec } from "@perfect/core/connect";
import { Stream } from "@perfect/core/stream";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";
import { RedisPubSub } from "./redis-pubsub";
import { RedisRef } from "./redis-ref";

export interface RedisSubscriptionRefConfig<A> {
  redis: RedisClient;
  key: string;
  initial: A;
  channel?: string;
  codec?: Codec<A>;
}

export class RedisSubscriptionRef<A> implements SubscriptionRef<A, Throws<RedisError>> {
  private constructor(
    private readonly ref: RedisRef<A>,
    private readonly pubsub: RedisPubSub<A>,
  ) {}

  static make<A>(
    config: RedisSubscriptionRefConfig<A>,
  ): Eff<RedisSubscriptionRef<A>, Throws<RedisError>> {
    return RedisRef.make(config).map(
      (ref) =>
        new RedisSubscriptionRef(
          ref,
          RedisPubSub.make({
            redis: config.redis,
            channel: config.channel ?? `${config.key}:changes`,
            codec: config.codec,
          }),
        ),
    );
  }

  get get(): Eff<A, Throws<RedisError>> {
    return this.ref.get;
  }

  set(value: A): Eff<void, Throws<RedisError>> {
    return this.ref
      .set(value)
      .flatMap(() => this.pubsub.publish(value))
      .map(() => undefined);
  }

  update(f: (a: A) => A): Eff<void, Throws<RedisError>> {
    return this.ref
      .modify((current) => {
        const next = f(current);
        return [next, next];
      })
      .flatMap((next) => this.pubsub.publish(next))
      .map(() => undefined);
  }

  get changes(): Eff<Stream<A, Throws<RedisError> | Throws<QueueClosed>>, Throws<RedisError>> {
    return this.pubsub.subscribe.flatMap((changes) =>
      this.ref.get.map(
        (current) =>
          Stream.of(current).concat(changes) as Stream<A, Throws<RedisError> | Throws<QueueClosed>>,
      ),
    );
  }

  shutdown(): Eff<void, Throws<RedisError>> {
    return this.pubsub.shutdown();
  }
}

export { RedisSubscriptionRef as RedisSignal };
