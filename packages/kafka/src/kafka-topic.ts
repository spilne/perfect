// ---------------------------------------------------------------------------
// KafkaTopic<T> — Kafka topic implementing the connect contracts
//
// Implements: Partitionable, Replayable, Acknowledgeable, KeyedSinkable, Checkpointable
//
// Works with any KafkaClient implementation:
//   - kafkajs / @confluentinc/kafka-javascript (callback-based consumer)
//   - @platformatic/kafka (stream-based consumer)
//
// Ported from promin's kafka-topic.ts (Effect-TS → Eff): StreamPipeline.from
// dropped (Stream is used directly), `Stream.async((emit) => ... Effect.promise)`
// became perfect's `Stream.async((emit) => sync(...))`. Emission is per-message;
// promin's opt-in `batchEmit` (chunk-per-FetchResponse) is deferred — perfect's
// `Stream.async` emit is per-value.
// ---------------------------------------------------------------------------

import { sync } from "@perfect/core";
import { Stream } from "@perfect/core/stream";
import { JsonCodec, OffsetTracker } from "@perfect/core/connect";
import type {
  KeyedSinkable,
  Partitionable,
  Replayable,
  Acknowledgeable,
  Checkpointable,
  ConsumerGroup,
  Envelope,
  Codec,
  Offset,
  Partition,
} from "@perfect/core/connect";
import type { KafkaClient, KafkaConsumerOptions, KafkaProducer, KafkaMessage } from "./kafka-types";
import { type TopicName, type GroupId, PartitionId, KafkaOffset } from "./brands";

export interface KafkaTopicConfig<T> {
  /** Kafka client instance. */
  kafka: KafkaClient;
  /** Topic name. */
  topic: TopicName;
  /** Consumer group ID. */
  groupId: GroupId;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /**
   * Consumer timeout tuning (sessionTimeout / maxPollInterval /
   * heartbeatInterval), passed to every consumer this topic creates. Raise
   * `maxPollInterval` when handlers do slow I/O — a handler that outlives it
   * gets the consumer kicked → rebalance → redelivery loop.
   */
  consumerOptions?: Omit<KafkaConsumerOptions, "groupId">;
}

export class KafkaTopic<T>
  implements
    Partitionable<T>,
    Replayable<T>,
    Acknowledgeable<T>,
    KeyedSinkable<T>,
    Checkpointable<T>
{
  readonly codec: Codec<T>;
  private readonly kafka: KafkaClient;
  private readonly topic: TopicName;
  private readonly groupId: GroupId;
  private readonly consumerOptions?: Omit<KafkaConsumerOptions, "groupId">;

  private producer?: KafkaProducer;
  private _partitions?: number;

  constructor(config: KafkaTopicConfig<T>) {
    this.kafka = config.kafka;
    this.topic = config.topic;
    this.groupId = config.groupId;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.consumerOptions = config.consumerOptions;
  }

  /** Partition count — fetched from broker on first access. */
  get partitions(): number {
    return this._partitions ?? 1;
  }

  /** Fetch and cache the partition count from the broker. */
  async fetchPartitions(): Promise<number> {
    if (this._partitions) return this._partitions;
    const admin = this.kafka.admin();
    await admin.connect();
    if (admin.fetchTopicPartitionCount) {
      this._partitions = await admin.fetchTopicPartitionCount(this.topic);
    }
    await admin.disconnect();
    return this._partitions ?? 1;
  }

  // =========================================================================
  // Sinkable — publish messages
  // =========================================================================

  async publish(value: T, params?: { key: string }): Promise<void> {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    const encoded = this.codec.encode(value);
    await this.producer.send({
      topic: this.topic,
      messages: [
        {
          key: params?.key ?? null,
          value: JSON.stringify(encoded),
        },
      ],
    });
  }

  async publishBatch(messages: { value: T; key?: string }[]): Promise<void> {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    await this.producer.send({
      topic: this.topic,
      messages: messages.map((m) => ({
        key: m.key ?? null,
        value: JSON.stringify(this.codec.encode(m.value)),
      })),
    });
  }

  // =========================================================================
  // Streamable — subscribe to messages
  // =========================================================================

  subscribe(params?: { group?: ConsumerGroup; partitions?: Partition[] }): Stream<T, never> {
    return this.createConsumerStream(params?.group);
  }

  // =========================================================================
  // Replayable — subscribe from offset
  // =========================================================================

  subscribeFrom(params: { offset: Offset; group?: ConsumerGroup }): Stream<T, never> {
    return this.createConsumerStream(params.group, params.offset);
  }

  // =========================================================================
  // Acknowledgeable — manual ack/nack
  // =========================================================================

  subscribeAck(params?: {
    group?: ConsumerGroup;
    commitIntervalMs?: number;
    fromBeginning?: boolean;
  }): Stream<Envelope<T>, never> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = params?.group ?? this.groupId;
    const commitIntervalMs = params?.commitIntervalMs ?? 1000;
    const consumerOptions = this.consumerOptions;

    // Cleanup is wired twice on purpose: returned from the register effect
    // (runs on close/interruption) AND as an explicit .onFinalize (runs on
    // normal downstream completion, e.g. `take(n)` consuming exactly n
    // values — a path where Stream.async's own cleanup does not fire). The
    // idempotency guard inside `cleanup` makes double-firing safe.
    let cleanup: (() => void) | undefined;

    const stream = Stream.async<Envelope<T>, never>((emit) => {
      const consumer = kafka.consumer({ groupId, ...consumerOptions });
      const tracker = new OffsetTracker();
      let commitTimer: ReturnType<typeof setInterval> | undefined;
      let stopped = false;
      let flushing = false;

      const flushCommits = async () => {
        // Re-entrancy guard: the interval can fire again while a slow
        // commitOffsets is still in flight; overlapping commits could land
        // out of order. Skipping is safe — committable() recomputes next tick.
        if (flushing) return;
        flushing = true;
        try {
          const committable = tracker.committable();
          if (committable.size === 0) return;

          const offsets = [...committable.entries()].map(([partition, offset]) => ({
            topic,
            partition,
            offset: KafkaOffset(offset.toString()),
          }));

          await consumer.commitOffsets(offsets);
        } catch {
          // Commit failed — will retry on next interval
        } finally {
          flushing = false;
        }
      };

      const makeEnvelope = (msg: KafkaMessage): Envelope<T> => {
        const raw = msg.message.value;
        const str = raw instanceof Buffer ? raw.toString() : (raw as string);
        const value = codec.decode(JSON.parse(str));
        const offset = Number(msg.message.offset);
        const partition = msg.partition;

        // Seed the frontier in delivery order (before any concurrent acks),
        // so commits resume from the right offset on a non-zero start or a
        // rebalance/seek rewind.
        tracker.observe(partition, offset);

        return {
          value,
          ack: async () => {
            tracker.complete(partition, offset);
          },
          nack: async () => {},
          metadata: {
            topic: msg.topic,
            partition,
            offset: msg.message.offset,
            key: msg.message.key?.toString(),
            timestamp: msg.message.timestamp,
          },
        };
      };

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: params?.fromBeginning ?? false });

        commitTimer = setInterval(flushCommits, commitIntervalMs);

        if (consumer.stream) {
          // Platformatic-style per-message iteration.
          for await (const msg of consumer.stream()) {
            if (stopped) break;
            emit(makeEnvelope(msg));
          }
        } else if (consumer.run) {
          await consumer.run({
            autoCommit: false,
            eachMessage: async (msg: KafkaMessage) => {
              if (stopped) return;
              emit(makeEnvelope(msg));
            },
          });
        }
      };

      let cleaned = false;
      cleanup = () => {
        if (cleaned) return;
        cleaned = true;
        stopped = true;
        if (commitTimer) clearInterval(commitTimer);
        // Cleanup must be synchronous for Stream.async — fire and forget the
        // final flush + disconnect.
        void flushCommits().finally(() => consumer.disconnect().catch(() => {}));
      };

      return sync(() => {
        run().catch(() => {});
        return cleanup;
      });
    });

    return stream.onFinalize(sync(() => cleanup?.()));
  }

  // =========================================================================
  // Checkpointable — offset management
  // =========================================================================

  async commitOffset(params: { group: ConsumerGroup; offset: string }): Promise<void> {
    const consumer = this.kafka.consumer({ groupId: params.group });
    await consumer.connect();
    await consumer.commitOffsets([
      { topic: this.topic, partition: PartitionId(0), offset: KafkaOffset(params.offset) },
    ]);
    await consumer.disconnect();
  }

  async getCommittedOffset(params: { group: ConsumerGroup }): Promise<string | null> {
    const admin = this.kafka.admin();
    await admin.connect();
    const offsets = await admin.fetchOffsets({ groupId: params.group, topics: [this.topic] });
    await admin.disconnect();
    const topicOffset = offsets.find((o) => o.topic === this.topic);
    if (!topicOffset || topicOffset.partitions.length === 0) return null;
    return topicOffset.partitions[0]!.offset;
  }

  // =========================================================================
  // Internal — consumer stream creation
  // =========================================================================

  private createConsumerStream(group?: ConsumerGroup, offset?: Offset): Stream<T, never> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = group ?? this.groupId;
    const consumerOptions = this.consumerOptions;

    // Same double-wiring as subscribeAck — see the comment there.
    let cleanup: (() => void) | undefined;

    const stream = Stream.async<T, never>((emit) => {
      const consumer = kafka.consumer({ groupId, ...consumerOptions });
      let stopped = false;

      const decodeMessage = (msg: KafkaMessage): T => {
        const raw = msg.message.value;
        const str = raw instanceof Buffer ? raw.toString() : (raw as string);
        return codec.decode(JSON.parse(str));
      };

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({
          topic,
          fromBeginning: offset?.type === "earliest",
        });

        if (offset?.type === "timestamp") {
          const admin = kafka.admin();
          await admin.connect();
          const result = await admin.fetchTopicOffsetsByTimestamp(topic, offset.value);
          await admin.disconnect();
          if (consumer.seek) {
            for (const p of result) {
              consumer.seek({ topic, partition: p.partition, offset: p.offset });
            }
          }
        }

        if (consumer.stream) {
          // Platformatic stream mode — per-message iteration.
          for await (const msg of consumer.stream()) {
            if (stopped) break;
            emit(decodeMessage(msg));
          }
        } else if (consumer.run) {
          await consumer.run({
            eachMessage: async (msg: KafkaMessage) => {
              if (stopped) return;
              emit(decodeMessage(msg));
            },
          });
        }
      };

      let cleaned = false;
      cleanup = () => {
        if (cleaned) return;
        cleaned = true;
        stopped = true;
        void consumer.disconnect().catch(() => {});
      };

      return sync(() => {
        run().catch(() => {});
        return cleanup;
      });
    });

    return stream.onFinalize(sync(() => cleanup?.()));
  }

  // =========================================================================
  // Cleanup
  // =========================================================================

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
  }
}
