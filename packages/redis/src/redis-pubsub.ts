import { Queue as QueueNS, fail, runSync, succeed, sync } from "@perfect/core";
import type { Eff, PubSub, Queue, QueueClosed, Throws } from "@perfect/core";
import type { Codec } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { Stream } from "@perfect/core/stream";
import { decode, encode, numberResult, redisEff } from "./internal";
import { closeRedisClient, type RedisClient } from "./redis-client";
import { RedisError, toRedisError } from "./redis-error";

type SubscriptionEvent<A> =
  | { readonly _tag: "Value"; readonly value: A }
  | { readonly _tag: "Error"; readonly error: RedisError };

interface Subscription<A> {
  readonly client: RedisClient;
  readonly buffer: SubscriptionBuffer<A>;
  readonly onMessage: (...args: any[]) => void;
  readonly onError: (...args: any[]) => void;
  readonly onClose: (...args: any[]) => void;
  readonly target: string;
  readonly pattern: boolean;
  closed: boolean;
}

interface SubscriptionBuffer<A> {
  readonly queue: Queue<SubscriptionEvent<A>>;
  readonly capacity: number;
  terminalError: RedisError | null;
}

export interface RedisPubSubConfig<A> {
  redis: RedisClient;
  channel: string;
  codec?: Codec<A>;
  bufferCapacity?: number;
}

export class RedisPubSub<A> implements PubSub<A, Throws<RedisError>> {
  private readonly codec: Codec<A>;
  private readonly subscriptions = new Set<Subscription<A>>();
  private readonly bufferCapacity: number;
  private stopped = false;

  constructor(
    private readonly redis: RedisClient,
    private readonly channel: string,
    codec?: Codec<A>,
    bufferCapacity = 1024,
  ) {
    if (!Number.isInteger(bufferCapacity) || bufferCapacity < 1) {
      throw new Error("RedisPubSub.make: bufferCapacity must be a positive integer");
    }
    this.codec = codec ?? (JsonCodec as Codec<A>);
    this.bufferCapacity = bufferCapacity;
  }

  static make<A>(config: RedisPubSubConfig<A>): RedisPubSub<A> {
    return new RedisPubSub(config.redis, config.channel, config.codec, config.bufferCapacity);
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
    return QueueNS.bounded<SubscriptionEvent<A>>(this.bufferCapacity).flatMap((queue) => {
      const buffer: SubscriptionBuffer<A> = {
        queue,
        capacity: this.bufferCapacity,
        terminalError: null,
      };
      if (this.stopped) {
        return queue.close().map(() => this.subscriptionStream(buffer));
      }

      return redisEff("pubsub.subscribe", async () => {
        const client = await this.redis.duplicate();
        const onMessage = (...args: any[]) => {
          const raw = params.pattern ? args[2] : args[1];
          if (typeof raw !== "string") return;
          try {
            const value = decode(this.codec, raw);
            this.offerEvent(buffer, { _tag: "Value", value });
          } catch (cause) {
            this.offerEvent(buffer, {
              _tag: "Error",
              error: toRedisError("pubsub.decode", cause),
            });
          }
        };
        const onError = (cause: unknown) => {
          this.offerEvent(buffer, {
            _tag: "Error",
            error: toRedisError("pubsub.subscription", cause),
          });
        };
        const onClose = () => {
          this.closeBuffer(buffer);
        };

        client.on(params.pattern ? "pmessage" : "message", onMessage);
        client.on("error", onError);
        client.on("close", onClose);
        try {
          if (params.pattern) await client.psubscribe(params.target);
          else await client.subscribe(params.target);
        } catch (cause) {
          this.removeListeners(client, onMessage, onError, onClose, params.pattern);
          closeRedisClient(client);
          throw cause;
        }

        const subscription: Subscription<A> = {
          client,
          buffer,
          onMessage,
          onError,
          onClose,
          target: params.target,
          pattern: params.pattern,
          closed: false,
        };
        this.subscriptions.add(subscription);
        return subscription;
      }).map((subscription) =>
        this.subscriptionStream(buffer).onFinalize(this.closeSubscription(subscription)),
      );
    });
  }

  private subscriptionStream(buffer: SubscriptionBuffer<A>): Stream<A, Throws<RedisError>> {
    return Stream.unfoldEffect(buffer.queue, (current) =>
      current
        .take()
        .flatMap((event) =>
          event._tag === "Value"
            ? succeed<[A, Queue<SubscriptionEvent<A>>]>([event.value, current])
            : fail(event.error),
        )
        .catchTag("QueueClosed", () =>
          buffer.terminalError ? fail(buffer.terminalError) : succeed(null),
        ),
    );
  }

  private offerEvent(buffer: SubscriptionBuffer<A>, event: SubscriptionEvent<A>): void {
    if (buffer.terminalError) return;
    if (event._tag === "Error") {
      buffer.terminalError = event.error;
      this.closeBuffer(buffer);
      return;
    }

    try {
      if (runSync(buffer.queue.size) >= buffer.capacity) {
        buffer.terminalError = new RedisError({
          operation: "pubsub.overflow",
          cause: new Error(`subscription buffer exceeded ${buffer.capacity} messages`),
        });
        this.closeBuffer(buffer);
        return;
      }
      runSync(buffer.queue.offer(event) as Eff<boolean, never>);
    } catch {
      this.closeBuffer(buffer);
    }
  }

  private closeBuffer(buffer: SubscriptionBuffer<A>): void {
    runSync(buffer.queue.close());
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
        subscription.onError,
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
      }).ensuring(subscription.buffer.queue.close());
    });
  }

  private removeListeners(
    client: RedisClient,
    onMessage: (...args: any[]) => void,
    onError: (...args: any[]) => void,
    onClose: (...args: any[]) => void,
    pattern: boolean,
  ): void {
    const remove = client.off?.bind(client) ?? client.removeListener?.bind(client);
    remove?.(pattern ? "pmessage" : "message", onMessage);
    remove?.("error", onError);
    remove?.("close", onClose);
  }
}
