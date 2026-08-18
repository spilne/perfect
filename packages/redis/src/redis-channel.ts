import { sync } from "@perfect/core";
import type { Codec, Sinkable, Streamable } from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { Stream } from "@perfect/core/stream";
import { decode, encode } from "./internal";
import { closeRedisClient, type RedisClient } from "./redis-client";

export interface RedisChannelConfig<T> {
  redis: RedisClient;
  channel: string;
  codec?: Codec<T>;
  retryDelayMs?: number;
}

export class RedisChannel<T> implements Streamable<T>, Sinkable<T> {
  readonly codec: Codec<T>;
  private readonly retryDelayMs: number;
  private readonly redis: RedisClient;
  private readonly channel: string;

  constructor(config: RedisChannelConfig<T>) {
    this.redis = config.redis;
    this.channel = config.channel;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.retryDelayMs = config.retryDelayMs ?? 250;
  }

  static make<T>(config: RedisChannelConfig<T>): RedisChannel<T> {
    return new RedisChannel(config);
  }

  async publish(value: T): Promise<void> {
    await this.redis.publish(this.channel, encode(this.codec, value));
  }

  subscribe(): Stream<T, never> {
    return this.createSubscription({ target: this.channel, pattern: false });
  }

  subscribePattern(pattern: string): Stream<T, never> {
    return this.createSubscription({ target: pattern, pattern: true });
  }

  private createSubscription(params: { target: string; pattern: boolean }): Stream<T, never> {
    return Stream.async<T, never>((emit) => {
      let running = true;
      let client: RedisClient | undefined;
      let listener: ((first: string, second: string, third?: string) => void) | undefined;

      const connect = async () => {
        while (running) {
          try {
            client = await this.redis.duplicate();
            listener = (first, second, third) => {
              const target = first;
              const raw = params.pattern ? third : second;
              if (!running || target !== params.target || raw === undefined) return;
              try {
                emit(decode(this.codec, raw));
              } catch {}
            };
            client.on(params.pattern ? "pmessage" : "message", listener);
            if (params.pattern) await client.psubscribe(params.target);
            else await client.subscribe(params.target);
            return;
          } catch {
            if (client) closeRedisClient(client);
            client = undefined;
            if (running) await this.delay(this.retryDelayMs);
          }
        }
      };

      void connect();
      return sync(() => () => {
        running = false;
        if (!client) return;
        if (listener) {
          const remove = client.off?.bind(client) ?? client.removeListener?.bind(client);
          remove?.(params.pattern ? "pmessage" : "message", listener);
        }
        const activeClient = client;
        const unsubscribe = params.pattern
          ? activeClient.punsubscribe(params.target)
          : activeClient.unsubscribe(params.target);
        void unsubscribe.catch(() => {}).finally(() => closeRedisClient(activeClient));
      });
    });
  }

  async subscriberCount(): Promise<number> {
    const raw = await this.redis.pubsub("NUMSUB", this.channel);
    if (!Array.isArray(raw) || raw.length < 2) {
      throw new TypeError("Redis PUBSUB NUMSUB returned an invalid result");
    }
    return Number(raw[1]);
  }

  async patternSubscriberCount(): Promise<number> {
    return Number(await this.redis.pubsub("NUMPAT"));
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
