import { Queue as QueueNS, fail, runSync, succeed, sync } from "@spilne/perfect-core";
import type { Eff, PubSub, Queue, QueueClosed, Throws } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { JsonCodec } from "@spilne/perfect-core/connect";
import { Stream } from "@spilne/perfect-core/stream";
import { decode, encode, numberResult, redisEff } from "./internal.js";
import { closeRedisClient, type RedisClient } from "./redis-client.js";
import { RedisError, toRedisError } from "./redis-error.js";

type SubscriptionEvent<A> =
  | { readonly _tag: "Value"; readonly value: A }
  | { readonly _tag: "Error"; readonly error: RedisError };

interface Subscription<A> {
  readonly buffer: SubscriptionBuffer<A>;
  readonly target: string;
  readonly pattern: boolean;
  closed: boolean;
}

// All subscriptions of one RedisPubSub share one subscriber connection.
// (Each used to open its own.) Redis subscribes a connection, not a caller,
// so each channel or pattern is subscribed once, when its first subscriber
// arrives, and unsubscribed when its last one leaves.
interface SharedConnection<A> {
  readonly client: RedisClient;
  // Subscriptions by channel, and by pattern.
  readonly channels: Map<string, Set<Subscription<A>>>;
  readonly patterns: Map<string, Set<Subscription<A>>>;
  // The SUBSCRIBE / PSUBSCRIBE in flight or done, by target. Later
  // subscribers wait for the same one.
  readonly ready: Map<string, Promise<unknown>>;
  readonly detach: () => void;
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
  // The shared subscriber connection, opened on first use.
  private connection: Promise<SharedConnection<A>> | null = null;

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
        const subscription: Subscription<A> = {
          buffer,
          target: params.target,
          pattern: params.pattern,
          closed: false,
        };
        const shared = await this.sharedConnection();
        const byTarget = params.pattern ? shared.patterns : shared.channels;
        const key = this.readyKey(params);
        let group = byTarget.get(params.target);
        if (group === undefined) {
          group = new Set();
          byTarget.set(params.target, group);
          shared.ready.set(
            key,
            params.pattern
              ? shared.client.psubscribe(params.target)
              : shared.client.subscribe(params.target),
          );
        }
        group.add(subscription);
        this.subscriptions.add(subscription);
        try {
          await shared.ready.get(key);
        } catch (cause) {
          // The subscribe failed: undo this subscription.
          await this.leave(subscription).catch(() => {});
          throw cause;
        }
        return subscription;
      }).map((subscription) =>
        this.subscriptionStream(buffer).onFinalize(this.closeSubscription(subscription)),
      );
    });
  }

  private readyKey(params: { target: string; pattern: boolean }): string {
    return `${params.pattern ? "pattern" : "channel"}:${params.target}`;
  }

  // Open the shared connection once; concurrent callers wait for the same one.
  private sharedConnection(): Promise<SharedConnection<A>> {
    if (this.connection === null) {
      const opening = this.openConnection();
      this.connection = opening;
      // A failed open is forgotten, so the next subscriber tries again.
      opening.catch(() => {
        if (this.connection === opening) this.connection = null;
      });
    }
    return this.connection;
  }

  private async openConnection(): Promise<SharedConnection<A>> {
    const client = await this.redis.duplicate();
    const channels = new Map<string, Set<Subscription<A>>>();
    const patterns = new Map<string, Set<Subscription<A>>>();

    const deliver = (subscribers: Set<Subscription<A>> | undefined, raw: unknown) => {
      if (subscribers === undefined || typeof raw !== "string") return;
      let event: SubscriptionEvent<A>;
      try {
        event = { _tag: "Value", value: decode(this.codec, raw) };
      } catch (cause) {
        event = { _tag: "Error", error: toRedisError("pubsub.decode", cause) };
      }
      for (const subscription of subscribers) this.offerEvent(subscription.buffer, event);
    };
    const onMessage = (channel: string, raw: unknown) => deliver(channels.get(channel), raw);
    const onPatternMessage = (pattern: string, _channel: string, raw: unknown) =>
      deliver(patterns.get(pattern), raw);
    // The connection broke: every subscription on it fails, and the next
    // subscriber opens a new connection.
    const failAll = (event: SubscriptionEvent<A> | null) => {
      if (this.connection !== null) this.connection = null;
      for (const group of [...channels.values(), ...patterns.values()]) {
        for (const subscription of group) {
          if (event === null) this.closeBuffer(subscription.buffer);
          else this.offerEvent(subscription.buffer, event);
        }
      }
      channels.clear();
      patterns.clear();
    };
    const onError = (cause: unknown) =>
      failAll({ _tag: "Error", error: toRedisError("pubsub.subscription", cause) });
    const onClose = () => failAll(null);

    client.on("message", onMessage);
    client.on("pmessage", onPatternMessage);
    client.on("error", onError);
    client.on("close", onClose);
    const detach = () => {
      const remove = client.off?.bind(client) ?? client.removeListener?.bind(client);
      remove?.("message", onMessage);
      remove?.("pmessage", onPatternMessage);
      remove?.("error", onError);
      remove?.("close", onClose);
    };
    return { client, channels, patterns, ready: new Map(), detach };
  }

  // Remove a subscription. The last subscriber of a target unsubscribes it,
  // and the last subscription overall closes the shared connection.
  private async leave(subscription: Subscription<A>): Promise<void> {
    this.subscriptions.delete(subscription);
    const connection = this.connection;
    if (connection === null) return;
    const shared = await connection;
    const byTarget = subscription.pattern ? shared.patterns : shared.channels;
    const group = byTarget.get(subscription.target);
    if (group === undefined || !group.delete(subscription) || group.size > 0) return;
    byTarget.delete(subscription.target);
    shared.ready.delete(this.readyKey(subscription));
    try {
      if (subscription.pattern) await shared.client.punsubscribe(subscription.target);
      else await shared.client.unsubscribe(subscription.target);
    } finally {
      // Nobody subscribed any more (and nobody arrived meanwhile): close it.
      if (
        shared.channels.size === 0 &&
        shared.patterns.size === 0 &&
        this.connection === connection
      ) {
        this.connection = null;
        shared.detach();
        closeRedisClient(shared.client);
      }
    }
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
      return true;
    }).flatMap((shouldClose) => {
      if (!shouldClose) return succeed(undefined);
      return redisEff("pubsub.unsubscribe", () => this.leave(subscription)).ensuring(
        subscription.buffer.queue.close(),
      );
    });
  }
}
