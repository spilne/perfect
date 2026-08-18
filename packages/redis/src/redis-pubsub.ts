import { Queue as QueueNS, run, succeed, sync } from "@perfect/core";
import type { Eff, PubSub, Queue, QueueClosed, Throws } from "@perfect/core";
import type { Codec } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { Stream } from "@perfect/core/stream";
import { decode, encode, numberResult, redisEff } from "./internal";
import { closeRedisClient, type RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

interface Subscription<A> {
  readonly client: RedisClient;
  readonly queue: Queue<A>;
  readonly onMessage: (...args: any[]) => void;
  readonly onClose: (...args: any[]) => void;
  readonly target: string;
  readonly pattern: boolean;
  closed: boolean;
}

export interface RedisPubSubConfig<A> {
  redis: RedisClient;
  channel: string;
  codec?: Codec<A>;
}

export class RedisPubSub<A> implements PubSub<A, Throws<RedisError>> {
  private readonly codec: Codec<A>;
  private readonly subscriptions = new Set<Subscription<A>>();
  private stopped = false;

  constructor(
    private readonly redis: RedisClient,
    private readonly channel: string,
    codec?: Codec<A>,
  ) {
    this.codec = codec ?? (JsonCodec as Codec<A>);
  }

  static make<A>(config: RedisPubSubConfig<A>): RedisPubSub<A> {
    return new RedisPubSub(config.redis, config.channel, config.codec);
  }

  publish(value: A): Eff<boolean, Throws<RedisError>> {
    return sync(() => this.stopped).flatMap((stopped) =>
      stopped
        ? succeed(false)
        : redisEff(
            "pubsub.publish",
            async () => (await this.redis.publish(this.channel, encode(this.codec, value))) > 0,
          ),
    );
  }

  get subscribe(): Eff<Stream<A, Throws<RedisError> | Throws<QueueClosed>>, Throws<RedisError>> {
    return this.createSubscription({ target: this.channel, pattern: false });
  }

  subscribePattern(
    pattern: string,
  ): Eff<Stream<A, Throws<RedisError> | Throws<QueueClosed>>, Throws<RedisError>> {
    return this.createSubscription({ target: pattern, pattern: true });
  }

  private createSubscription(params: {
    target: string;
    pattern: boolean;
  }): Eff<Stream<A, Throws<RedisError> | Throws<QueueClosed>>, Throws<RedisError>> {
    return QueueNS.unbounded<A>().flatMap((queue) => {
      if (this.stopped) {
        return queue
          .close()
          .map(
            () => Stream.fromQueue(queue) as Stream<A, Throws<RedisError> | Throws<QueueClosed>>,
          );
      }

      return redisEff("pubsub.subscribe", async () => {
        const client = await this.redis.duplicate();
        const onMessage = (...args: any[]) => {
          const raw = params.pattern ? args[2] : args[1];
          if (typeof raw !== "string") return;
          try {
            const value = decode(this.codec, raw);
            void run(queue.offer(value).catch(() => succeed(false)));
          } catch {
            // Malformed messages are isolated from the subscription.
          }
        };
        const onClose = () => {
          void run(queue.close());
        };

        client.on(params.pattern ? "pmessage" : "message", onMessage);
        client.on("error", onClose);
        client.on("close", onClose);
        try {
          if (params.pattern) await client.psubscribe(params.target);
          else await client.subscribe(params.target);
        } catch (cause) {
          this.removeListeners(client, onMessage, onClose, params.pattern);
          closeRedisClient(client);
          throw cause;
        }

        const subscription: Subscription<A> = {
          client,
          queue,
          onMessage,
          onClose,
          target: params.target,
          pattern: params.pattern,
          closed: false,
        };
        this.subscriptions.add(subscription);
        return subscription;
      }).map((subscription) =>
        (Stream.fromQueue(queue) as Stream<A, Throws<QueueClosed>>).onFinalize(
          this.closeSubscription(subscription),
        ),
      );
    });
  }

  shutdown(): Eff<void, Throws<RedisError>> {
    return sync(() => {
      this.stopped = true;
      return Array.from(this.subscriptions);
    }).flatMap((subscriptions) =>
      subscriptions.reduce<Eff<void, Throws<RedisError>>>(
        (effect, subscription) => effect.flatMap(() => this.closeSubscription(subscription)),
        succeed(undefined),
      ),
    );
  }

  get subscriberCount(): Eff<number, Throws<RedisError>> {
    return redisEff("pubsub.subscriberCount", async () => {
      const result = await this.redis.pubsub("NUMSUB", this.channel);
      if (!Array.isArray(result) || result.length < 2) {
        throw new TypeError("Redis PUBSUB NUMSUB returned an invalid result");
      }
      return Number(result[1]);
    });
  }

  get patternSubscriberCount(): Eff<number, Throws<RedisError>> {
    return redisEff("pubsub.patternSubscriberCount", async () =>
      numberResult(await this.redis.pubsub("NUMPAT")),
    );
  }

  private closeSubscription(subscription: Subscription<A>): Eff<void, Throws<RedisError>> {
    return sync(() => {
      if (subscription.closed) return false;
      subscription.closed = true;
      this.subscriptions.delete(subscription);
      this.removeListeners(
        subscription.client,
        subscription.onMessage,
        subscription.onClose,
        subscription.pattern,
      );
      return true;
    }).flatMap((shouldClose) => {
      if (!shouldClose) return succeed(undefined);
      return redisEff("pubsub.unsubscribe", async () => {
        try {
          if (subscription.pattern) await subscription.client.punsubscribe(subscription.target);
          else await subscription.client.unsubscribe(subscription.target);
        } finally {
          closeRedisClient(subscription.client);
        }
      }).ensuring(subscription.queue.close());
    });
  }

  private removeListeners(
    client: RedisClient,
    onMessage: (...args: any[]) => void,
    onClose: (...args: any[]) => void,
    pattern: boolean,
  ): void {
    const remove = client.off?.bind(client) ?? client.removeListener?.bind(client);
    remove?.(pattern ? "pmessage" : "message", onMessage);
    remove?.("error", onClose);
    remove?.("close", onClose);
  }
}
