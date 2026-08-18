import { async as asyncEff, fail, succeed, sync } from "@perfect/core";
import type { Eff, Throws } from "@perfect/core";
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
import { decode, encode, redisEff } from "./internal";
import { closeRedisClient, type RedisClient } from "./redis-client";
import { RedisError, toRedisError } from "./redis-error";

export interface RedisStreamConfig<T> {
  redis: RedisClient;
  stream: string;
  group: string;
  consumer?: string;
  codec?: Codec<T>;
  blockMs?: number;
  count?: number;
  recovery?: RedisStreamRecoveryConfig;
}

export interface RedisStreamRecoveryConfig {
  minIdleMs: number;
  count?: number;
  maxDeliveries?: number;
  deadLetterStream?: string;
  deleteAfterDeadLetter?: boolean;
}

export interface RedisClaimedMessage<T> {
  readonly id: string;
  readonly value: T;
}

export interface RedisRecoveredMessage<T> extends RedisClaimedMessage<T> {
  readonly deliveries: number;
}

export interface RedisRecoveryResult<T> {
  readonly nextStart: string;
  readonly messages: readonly RedisRecoveredMessage<T>[];
  readonly deadLetteredIds: readonly string[];
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
  implements
    Streamable<T, Throws<RedisError>>,
    KeyedSinkable<T, Throws<RedisError>>,
    Acknowledgeable<T, Throws<RedisError>>,
    Replayable<T, Throws<RedisError>>
{
  readonly codec: Codec<T>;
  private readonly redis: RedisClient;
  private readonly stream: string;
  private readonly group: string;
  private readonly consumer: string;
  private readonly blockMs: number;
  private readonly count: number;
  private readonly recovery?: RedisStreamRecoveryConfig;

  constructor(config: RedisStreamConfig<T>) {
    this.redis = config.redis;
    this.stream = config.stream;
    this.group = config.group;
    this.consumer = config.consumer ?? crypto.randomUUID();
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.blockMs = config.blockMs ?? 5000;
    this.count = config.count ?? 10;
    this.recovery = config.recovery;
    if (this.recovery) this.validateRecovery(this.recovery);
  }

  static make<T>(config: RedisStreamConfig<T>): RedisStream<T> {
    return new RedisStream(config);
  }

  ensureGroup(params?: { group?: string; offset?: Offset }): Eff<void, Throws<RedisError>> {
    return redisEff("stream.ensureGroup", () => this.ensureGroupPromise(params));
  }

  private async ensureGroupPromise(params?: { group?: string; offset?: Offset }): Promise<void> {
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

  publish(value: T, params?: { key: string }): Eff<void, Throws<RedisError>> {
    return redisEff("stream.publish", async () => {
      const fields: string[] = ["data", encode(this.codec, value)];
      if (params?.key !== undefined) fields.push("key", params.key);
      await this.redis.xadd(this.stream, "*", ...fields);
    });
  }

  subscribe(params?: { group?: ConsumerGroup }): Stream<T, Throws<RedisError>> {
    return this.createReadStream({ group: params?.group, autoAck: true });
  }

  subscribeFrom(params: { offset: Offset; group?: ConsumerGroup }): Stream<T, Throws<RedisError>> {
    return this.createReadStream({ group: params.group, offset: params.offset, autoAck: true });
  }

  subscribeAck(
    params?: AcknowledgeOptions,
  ): Stream<Envelope<T, Throws<RedisError>>, Throws<RedisError>> {
    return this.createReadStream({ group: params?.group, offset: params?.offset, autoAck: false });
  }

  claimPending(params: {
    minIdleMs: number;
    count: number;
    group?: ConsumerGroup;
  }): Eff<RedisClaimedMessage<T>[], Throws<RedisError>> {
    return this.recoverPending({ ...params, start: "0-0" }).map((result) =>
      result.messages.map(({ id, value }) => ({ id, value })),
    );
  }

  recoverPending(
    params: RedisStreamRecoveryConfig & {
      group?: ConsumerGroup;
      start?: string;
    },
  ): Eff<RedisRecoveryResult<T>, Throws<RedisError>> {
    this.validateRecovery(params);
    return redisEff("stream.recoverPending", async () => {
      const group = params.group ?? this.group;
      const recovered = await this.recoverPendingEntries(group, params.start ?? "0-0", params);
      return {
        nextStart: recovered.nextStart,
        messages: recovered.entries.map(({ entry, deliveries }) => ({
          id: entry.id,
          value: this.decodeEntry(entry),
          deliveries,
        })),
        deadLetteredIds: recovered.deadLetteredIds,
      };
    });
  }

  acknowledge(id: string, params?: { group?: ConsumerGroup }): Eff<boolean, Throws<RedisError>> {
    return redisEff(
      "stream.acknowledge",
      async () => (await this.redis.xack(this.stream, params?.group ?? this.group, id)) > 0,
    );
  }

  info(): Eff<RedisStreamInfo, Throws<RedisError>> {
    return redisEff("stream.info", async () => {
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
    });
  }

  private createReadStream(params: {
    group?: ConsumerGroup;
    offset?: Offset;
    autoAck: true;
  }): Stream<T, Throws<RedisError>>;
  private createReadStream(params: {
    group?: ConsumerGroup;
    offset?: Offset;
    autoAck: false;
  }): Stream<Envelope<T, Throws<RedisError>>, Throws<RedisError>>;
  private createReadStream(params: {
    group?: ConsumerGroup;
    offset?: Offset;
    autoAck: boolean;
  }): Stream<T | Envelope<T, Throws<RedisError>>, Throws<RedisError>> {
    const group = params.group ?? this.group;
    let activeClient: RedisClient | undefined;
    let recoveryCursor = "0-0";
    const acquire = asyncEff<RedisClient, RedisError>((resume) => {
      let canceled = false;

      void (async () => {
        await this.ensureGroupPromise({ group, offset: params.offset });
        if (canceled) return;
        const client = await this.redis.duplicate();
        if (canceled) {
          closeRedisClient(client);
          return;
        }
        activeClient = client;
        resume(succeed(client));
      })().catch((cause) => {
        if (!canceled) resume(fail(toRedisError("stream.subscribe", cause)));
      });

      return () => {
        canceled = true;
        if (activeClient) {
          closeRedisClient(activeClient);
          activeClient = undefined;
        }
      };
    });

    const source = Stream.fromEffect(acquire).flatMap((client) => {
      const read: Eff<Array<T | Envelope<T, Throws<RedisError>>>, Throws<RedisError>> = redisEff(
        "stream.read",
        async () => {
          let entries: RedisStreamEntry[] = [];
          if (this.recovery) {
            const recovered = await this.recoverPendingEntries(
              group,
              recoveryCursor,
              this.recovery,
            );
            recoveryCursor = recovered.nextStart;
            entries = recovered.entries.map(({ entry }) => entry);
          }
          if (entries.length === 0) {
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
            entries = this.parseRead(raw);
          }
          if (params.autoAck) {
            const values = entries.map((entry) => this.decodeEntry(entry));
            if (entries.length > 0) {
              await this.redis.xack(this.stream, group, ...entries.map((entry) => entry.id));
            }
            return values;
          }
          return entries.map((entry) => this.toEnvelope(entry, group));
        },
      );

      return Stream.repeat(read).flatMap((values) => Stream.fromArray(values));
    });

    return source.onFinalize(
      sync(() => {
        if (!activeClient) return;
        closeRedisClient(activeClient);
        activeClient = undefined;
      }),
    );
  }

  private toEnvelope(entry: RedisStreamEntry, group: string): Envelope<T, Throws<RedisError>> {
    return {
      value: this.decodeEntry(entry),
      ack: () =>
        redisEff("stream.ack", async () => {
          await this.redis.xack(this.stream, group, entry.id);
        }),
      nack: () => succeed(undefined),
      metadata: {
        topic: this.stream,
        partition: 0,
        offset: entry.id,
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

  private async recoverPendingEntries(
    group: string,
    start: string,
    config: RedisStreamRecoveryConfig,
  ): Promise<{
    nextStart: string;
    entries: Array<{ entry: RedisStreamEntry; deliveries: number }>;
    deadLetteredIds: string[];
  }> {
    await this.ensureGroupPromise({ group });
    const count = config.count ?? this.count;
    let nextStart = "0-0";
    let entries: RedisStreamEntry[];

    if (this.redis.xautoclaim) {
      const raw = await this.redis.xautoclaim(
        this.stream,
        group,
        this.consumer,
        config.minIdleMs,
        start,
        "COUNT",
        count,
      );
      if (!Array.isArray(raw)) return { nextStart, entries: [], deadLetteredIds: [] };
      nextStart = String(raw[0] ?? "0-0");
      entries = this.parseClaimed(raw[1]);
    } else {
      const pending = await this.redis.xpending(this.stream, group, "-", "+", count);
      if (!Array.isArray(pending)) return { nextStart, entries: [], deadLetteredIds: [] };
      const ids = pending
        .filter((entry) => Array.isArray(entry) && Number(entry[2]) >= config.minIdleMs)
        .map((entry) => String(entry[0]));
      if (ids.length === 0) return { nextStart, entries: [], deadLetteredIds: [] };
      entries = this.parseClaimed(
        await this.redis.xclaim(this.stream, group, this.consumer, config.minIdleMs, ...ids),
      );
    }

    const retained: Array<{ entry: RedisStreamEntry; deliveries: number }> = [];
    const deadLetteredIds: string[] = [];
    for (const entry of entries) {
      const deliveries = await this.deliveryCount(group, entry.id);
      if (
        config.deadLetterStream &&
        config.maxDeliveries !== undefined &&
        deliveries >= config.maxDeliveries
      ) {
        await this.redis.xadd(
          config.deadLetterStream,
          "*",
          ...entry.fields,
          "source-stream",
          this.stream,
          "source-group",
          group,
          "source-id",
          entry.id,
          "deliveries",
          deliveries,
        );
        await this.redis.xack(this.stream, group, entry.id);
        if (config.deleteAfterDeadLetter && this.redis.xdel) {
          await this.redis.xdel(this.stream, entry.id);
        }
        deadLetteredIds.push(entry.id);
      } else {
        retained.push({ entry, deliveries });
      }
    }

    return { nextStart, entries: retained, deadLetteredIds };
  }

  private async deliveryCount(group: string, id: string): Promise<number> {
    const raw = await this.redis.xpending(this.stream, group, id, id, 1);
    const entry = Array.isArray(raw) ? raw[0] : undefined;
    return Array.isArray(entry) ? Number(entry[3] ?? 1) : 1;
  }

  private validateRecovery(config: RedisStreamRecoveryConfig): void {
    if (!Number.isFinite(config.minIdleMs) || config.minIdleMs < 0) {
      throw new Error("RedisStream recovery minIdleMs must be non-negative");
    }
    if (config.count !== undefined && (!Number.isInteger(config.count) || config.count < 1)) {
      throw new Error("RedisStream recovery count must be a positive integer");
    }
    if (
      config.maxDeliveries !== undefined &&
      (!Number.isInteger(config.maxDeliveries) || config.maxDeliveries < 1)
    ) {
      throw new Error("RedisStream recovery maxDeliveries must be a positive integer");
    }
    if (config.maxDeliveries !== undefined && !config.deadLetterStream) {
      throw new Error("RedisStream recovery maxDeliveries requires deadLetterStream");
    }
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
}
