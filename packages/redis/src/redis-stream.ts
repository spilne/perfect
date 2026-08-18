import { sync } from "@perfect/core";
import type {
  Acknowledgeable,
  AcknowledgeOptions,
  Codec,
  ConsumerGroup,
  Envelope,
  KeyedSinkable,
  Offset,
  Replayable,
  Streamable,
} from "@perfect/core/connect";
import { JsonCodec } from "@perfect/core/connect";
import { Stream } from "@perfect/core/stream";
import { decode, encode } from "./internal";
import { closeRedisClient, type RedisClient } from "./redis-client";

export interface RedisStreamConfig<T> {
  redis: RedisClient;
  stream: string;
  group: string;
  consumer?: string;
  codec?: Codec<T>;
  blockMs?: number;
  count?: number;
  retryDelayMs?: number;
}

export interface RedisClaimedMessage<T> {
  readonly id: string;
  readonly value: T;
}

export interface RedisStreamInfo {
  readonly length: number;
  readonly groups: number;
  readonly lastId: string;
}

interface RedisStreamEntry {
  readonly id: string;
  readonly fields: string[];
}

export class RedisStream<T>
  implements Streamable<T>, KeyedSinkable<T>, Acknowledgeable<T>, Replayable<T>
{
  readonly codec: Codec<T>;
  private readonly redis: RedisClient;
  private readonly stream: string;
  private readonly group: string;
  private readonly consumer: string;
  private readonly blockMs: number;
  private readonly count: number;
  private readonly retryDelayMs: number;

  constructor(config: RedisStreamConfig<T>) {
    this.redis = config.redis;
    this.stream = config.stream;
    this.group = config.group;
    this.consumer = config.consumer ?? crypto.randomUUID();
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.blockMs = config.blockMs ?? 5000;
    this.count = config.count ?? 10;
    this.retryDelayMs = config.retryDelayMs ?? 250;
  }

  static make<T>(config: RedisStreamConfig<T>): RedisStream<T> {
    return new RedisStream(config);
  }

  async ensureGroup(params?: { group?: string; offset?: Offset }): Promise<void> {
    const group = params?.group ?? this.group;
    const start = this.toRedisOffset(params?.offset);
    try {
      await this.redis.xgroup("CREATE", this.stream, group, start, "MKSTREAM");
    } catch (cause) {
      if (!this.isBusyGroup(cause)) throw cause;
      if (params?.offset) {
        await this.redis.xgroup("SETID", this.stream, group, start);
      }
    }
  }

  async publish(value: T, params?: { key: string }): Promise<void> {
    const fields: string[] = ["data", encode(this.codec, value)];
    if (params?.key !== undefined) fields.push("key", params.key);
    await this.redis.xadd(this.stream, "*", ...fields);
  }

  subscribe(params?: { group?: ConsumerGroup }): Stream<T, never> {
    return this.createReadStream({ group: params?.group, autoAck: true });
  }

  subscribeFrom(params: { offset: Offset; group?: ConsumerGroup }): Stream<T, never> {
    return this.createReadStream({ group: params.group, offset: params.offset, autoAck: true });
  }

  subscribeAck(params?: AcknowledgeOptions): Stream<Envelope<T>, never> {
    return this.createReadStream({ group: params?.group, offset: params?.offset, autoAck: false });
  }

  async claimPending(params: {
    minIdleMs: number;
    count: number;
    group?: ConsumerGroup;
  }): Promise<RedisClaimedMessage<T>[]> {
    const group = params.group ?? this.group;
    await this.ensureGroup({ group });
    const pending = await this.redis.xpending(this.stream, group, "-", "+", params.count);
    if (!Array.isArray(pending)) return [];

    const ids = pending
      .filter((entry) => Array.isArray(entry) && Number(entry[2]) >= params.minIdleMs)
      .map((entry) => String(entry[0]));
    if (ids.length === 0) return [];

    const claimed = await this.redis.xclaim(
      this.stream,
      group,
      this.consumer,
      params.minIdleMs,
      ...ids,
    );
    return this.parseClaimed(claimed).map((entry) => ({
      id: entry.id,
      value: this.decodeEntry(entry),
    }));
  }

  async acknowledge(id: string, params?: { group?: ConsumerGroup }): Promise<boolean> {
    return (await this.redis.xack(this.stream, params?.group ?? this.group, id)) > 0;
  }

  async info(): Promise<RedisStreamInfo> {
    const raw = await this.redis.xinfo("STREAM", this.stream);
    if (!Array.isArray(raw)) {
      throw new TypeError("Redis XINFO STREAM returned an invalid result");
    }
    const fields = raw.map(String);
    const value = (name: string): string | undefined => {
      const index = fields.indexOf(name);
      return index === -1 ? undefined : fields[index + 1];
    };
    return {
      length: Number(value("length") ?? 0),
      groups: Number(value("groups") ?? 0),
      lastId: value("last-generated-id") ?? "0-0",
    };
  }

  private createReadStream(params: {
    group?: ConsumerGroup;
    offset?: Offset;
    autoAck: true;
  }): Stream<T, never>;
  private createReadStream(params: {
    group?: ConsumerGroup;
    offset?: Offset;
    autoAck: false;
  }): Stream<Envelope<T>, never>;
  private createReadStream(params: {
    group?: ConsumerGroup;
    offset?: Offset;
    autoAck: boolean;
  }): Stream<T | Envelope<T>, never> {
    const group = params.group ?? this.group;

    return Stream.async<T | Envelope<T>, never>((emit) => {
      let running = true;
      let client: RedisClient | undefined;
      let groupReady = false;

      const poll = async () => {
        while (running) {
          try {
            if (!groupReady) {
              await this.ensureGroup({ group, offset: params.offset });
              groupReady = true;
            }
            if (!running) return;
            client = await this.redis.duplicate();

            while (running) {
              const raw = await client.xreadgroup(
                "GROUP",
                group,
                this.consumer,
                "COUNT",
                this.count,
                "BLOCK",
                this.blockMs,
                "STREAMS",
                this.stream,
                ">",
              );
              const entries = this.parseRead(raw);
              if (entries.length === 0) continue;

              if (params.autoAck) {
                for (const entry of entries) emit(this.decodeEntry(entry));
                await this.redis.xack(this.stream, group, ...entries.map((entry) => entry.id));
              } else {
                for (const entry of entries) emit(this.toEnvelope(entry, group));
              }
            }
          } catch {
            if (client) closeRedisClient(client);
            client = undefined;
            if (running) await this.delay(this.retryDelayMs);
          }
        }
      };

      void poll();
      return sync(() => () => {
        running = false;
        if (client) closeRedisClient(client);
      });
    });
  }

  private toEnvelope(entry: RedisStreamEntry, group: string): Envelope<T> {
    return {
      value: this.decodeEntry(entry),
      ack: async () => {
        await this.redis.xack(this.stream, group, entry.id);
      },
      nack: async () => {},
      metadata: {
        id: entry.id,
        stream: this.stream,
        group,
        consumer: this.consumer,
        key: this.field(entry.fields, "key"),
      },
    };
  }

  private decodeEntry(entry: RedisStreamEntry): T {
    const raw = this.field(entry.fields, "data");
    if (raw === undefined) throw new TypeError(`Redis Stream entry ${entry.id} has no data field`);
    return decode(this.codec, raw);
  }

  private parseRead(raw: unknown): RedisStreamEntry[] {
    if (!Array.isArray(raw)) return [];
    const entries: RedisStreamEntry[] = [];
    for (const streamResult of raw) {
      if (!Array.isArray(streamResult) || !Array.isArray(streamResult[1])) continue;
      for (const message of streamResult[1]) {
        if (!Array.isArray(message) || !Array.isArray(message[1])) continue;
        entries.push({ id: String(message[0]), fields: message[1].map(String) });
      }
    }
    return entries;
  }

  private parseClaimed(raw: unknown): RedisStreamEntry[] {
    if (!Array.isArray(raw)) return [];
    return raw.flatMap((message) => {
      if (!Array.isArray(message) || !Array.isArray(message[1])) return [];
      return [{ id: String(message[0]), fields: message[1].map(String) }];
    });
  }

  private field(fields: string[], name: string): string | undefined {
    const index = fields.indexOf(name);
    return index === -1 ? undefined : fields[index + 1];
  }

  private toRedisOffset(offset?: Offset): string {
    if (!offset || offset.type === "earliest") return "0-0";
    if (offset.type === "latest") return "$";
    if (offset.type === "timestamp") return `${Math.max(0, Math.floor(offset.value))}-0`;
    return offset.value;
  }

  private isBusyGroup(cause: unknown): boolean {
    return cause instanceof Error && cause.message.includes("BUSYGROUP");
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
