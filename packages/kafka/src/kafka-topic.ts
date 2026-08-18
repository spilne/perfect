// ---------------------------------------------------------------------------
// KafkaTopic<T> — Kafka topic implementing the connect contracts
//
// Implements: Partitionable, Replayable, Acknowledgeable, KeyedSinkable, Checkpointable
//
// Works with any KafkaClient implementation:
//   - kafkajs / @confluentinc/kafka-javascript (callback-based consumer)
//   - @platformatic/kafka (stream-based consumer)
//
// Ported from promin's kafka-topic.ts (Effect-TS → Eff). Callback drivers use
// Stream.asyncChunks when batchEmit is enabled so one Kafka fetch batch remains
// one native Stream chunk.
// ---------------------------------------------------------------------------

import { fromPromise, succeed, sync } from "@perfect/core";
import type { Eff, Throws } from "@perfect/core";
import { Chunk, Stream } from "@perfect/core/stream";
import { JsonCodec, OffsetTracker } from "@perfect/core/connect";
import type {
  KeyedSinkable,
  Partitionable,
  Replayable,
  Acknowledgeable,
  AcknowledgeOptions,
  ManagedAcknowledgeable,
  ManagedAcknowledgementSubscription,
  PartitionAssignment,
  PartitionLifecycle,
  Checkpointable,
  ConsumerGroup,
  Envelope,
  Codec,
  Offset,
  Partition,
} from "@perfect/core/connect";
import type {
  KafkaClient,
  KafkaConsumer,
  KafkaConsumerOptions,
  KafkaProducer,
  KafkaMessage,
  KafkaBatchPayload,
} from "./kafka-types";
import { type TopicName, type GroupId, PartitionId, KafkaOffset } from "./brands";
import { KafkaError, toKafkaError } from "./kafka-error";

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
   * Preserve callback-driver fetch batches as Stream chunks. This opts into
   * `eachBatch`; drivers must support it. Stream-based drivers remain
   * per-message. Default: false.
   */
  batchEmit?: boolean;
  /**
   * Consumer timeout tuning (sessionTimeout / maxPollInterval /
   * heartbeatInterval), passed to every consumer this topic creates. Raise
   * `maxPollInterval` when handlers do slow I/O — a handler that outlives it
   * gets the consumer kicked → rebalance → redelivery loop.
   */
  consumerOptions?: Omit<KafkaConsumerOptions, "groupId">;
}

export interface KafkaAckOptions extends AcknowledgeOptions {
  readonly commitIntervalMs?: number;
  readonly autoCommit?: boolean;
  readonly fromBeginning?: boolean;
}

export interface KafkaAckSubscription<T> extends ManagedAcknowledgementSubscription<
  T,
  Throws<KafkaError>
> {
  readonly stream: Stream<Envelope<T, Throws<KafkaError>>, Throws<KafkaError>>;
  readonly consumer: KafkaConsumer;
  readonly topic: TopicName;
  readonly groupId: GroupId;
  /** Stops and disconnects the explicitly owned consumer. */
  close(): Promise<void>;
}

export class KafkaTopic<T>
  implements
    Partitionable<T, Throws<KafkaError>>,
    Replayable<T, Throws<KafkaError>>,
    Acknowledgeable<T, Throws<KafkaError>>,
    ManagedAcknowledgeable<T, Throws<KafkaError>>,
    KeyedSinkable<T, Throws<KafkaError>>,
    Checkpointable<T, Throws<KafkaError>>
{
  readonly codec: Codec<T>;
  private readonly kafka: KafkaClient;
  private readonly topic: TopicName;
  private readonly groupId: GroupId;
  private readonly batchEmit: boolean;
  private readonly consumerOptions?: Omit<KafkaConsumerOptions, "groupId">;

  private producer?: KafkaProducer;
  private _partitions?: number;

  constructor(config: KafkaTopicConfig<T>) {
    this.kafka = config.kafka;
    this.topic = config.topic;
    this.groupId = config.groupId;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.batchEmit = config.batchEmit ?? false;
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

  publish(value: T, params?: { key: string }): Eff<void, Throws<KafkaError>> {
    return fromPromise(
      async () => {
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
      },
      (cause) => toKafkaError("topic.publish", this.topic, cause),
    );
  }

  publishBatch(messages: { value: T; key?: string }[]): Eff<void, Throws<KafkaError>> {
    return fromPromise(
      async () => {
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
      },
      (cause) => toKafkaError("topic.publishBatch", this.topic, cause),
    );
  }

  // =========================================================================
  // Streamable — subscribe to messages
  // =========================================================================

  subscribe(params?: {
    group?: ConsumerGroup;
    partitions?: Partition[];
  }): Stream<T, Throws<KafkaError>> {
    return this.createConsumerStream(params?.group);
  }

  // =========================================================================
  // Replayable — subscribe from offset
  // =========================================================================

  subscribeFrom(params: { offset: Offset; group?: ConsumerGroup }): Stream<T, Throws<KafkaError>> {
    return this.createConsumerStream(params.group, params.offset);
  }

  // =========================================================================
  // Acknowledgeable — manual ack/nack
  // =========================================================================

  subscribeAck(
    params?: KafkaAckOptions,
  ): Stream<Envelope<T, Throws<KafkaError>>, Throws<KafkaError>> {
    const subscription = this.subscribeAckWithHandle(params);
    return subscription.stream.onFinalize(
      fromPromise(
        () => subscription.close(),
        (cause) => toKafkaError("topic.unsubscribe", this.topic, cause),
      ),
    );
  }

  subscribeAckManaged(params?: AcknowledgeOptions): KafkaAckSubscription<T> {
    return this.subscribeAckWithHandle(params);
  }

  subscribeAckWithHandle(params?: KafkaAckOptions): KafkaAckSubscription<T> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = params?.group ?? this.groupId;
    const commitIntervalMs = params?.commitIntervalMs ?? 1000;
    const autoCommit = params?.autoCommit ?? true;
    const batchEmit = this.batchEmit;
    const offset = params?.offset ?? (params?.fromBeginning ? { type: "earliest" } : undefined);
    const consumerOptions = this.consumerOptions;
    const consumer = kafka.consumer({ groupId, ...consumerOptions });
    const tracker = new OffsetTracker();
    let commitTimer: ReturnType<typeof setInterval> | undefined;
    let stopped = false;
    let flushPromise: Promise<void> | null = null;
    let closePromise: Promise<void> | null = null;
    let pendingCommit: Array<{
      topic: TopicName;
      partition: PartitionId;
      offset: KafkaOffset;
    }> | null = null;
    let lifecycle: PartitionLifecycle | undefined;
    const activePartitions = new Set<Partition>();
    let activeGeneration: number | undefined;
    const removeAssigned = consumer.onPartitionsAssigned?.(async (assignment) => {
      for (const partition of assignment.partitions) activePartitions.add(partition);
      activeGeneration = assignment.generation;
      await lifecycle?.assigned({
        partitions: assignment.partitions,
        generation: assignment.generation,
      });
    });
    const handleRevocation = async (assignment: PartitionAssignment) => {
      await lifecycle?.revoking({
        partitions: assignment.partitions,
        generation: assignment.generation,
      });
      await flushCommits();
      for (const partition of assignment.partitions) activePartitions.delete(partition);
    };
    const removeRevoked = consumer.onPartitionsRevoked?.(handleRevocation);

    const flushCommits = (): Promise<void> => {
      if (!autoCommit) return Promise.resolve();
      if (flushPromise) return flushPromise;
      flushPromise = (async () => {
        if (pendingCommit === null) {
          const committable = tracker.committable();
          if (committable.size === 0) return;
          pendingCommit = [...committable.entries()].map(([partition, nextOffset]) => ({
            topic,
            partition,
            offset: KafkaOffset(nextOffset.toString()),
          }));
        }

        await consumer.commitOffsets(pendingCommit);
        pendingCommit = null;
      })().finally(() => {
        flushPromise = null;
      });
      return flushPromise;
    };

    const close = (): Promise<void> => {
      if (closePromise) return closePromise;
      stopped = true;
      if (commitTimer) clearInterval(commitTimer);
      closePromise = (async () => {
        let failure: unknown;
        try {
          if (activePartitions.size > 0) {
            await handleRevocation({
              partitions: [...activePartitions],
              generation: activeGeneration,
            });
          } else {
            await flushCommits();
          }
        } catch (cause) {
          failure = cause;
        }
        removeAssigned?.();
        removeRevoked?.();
        try {
          await consumer.disconnect();
        } catch (cause) {
          if (failure === undefined) failure = cause;
        }
        if (failure !== undefined) throw failure;
      })();
      return closePromise;
    };

    const makeEnvelope = (msg: KafkaMessage): Envelope<T, Throws<KafkaError>> => {
      const raw = msg.message.value;
      const str = raw instanceof Buffer ? raw.toString() : (raw as string);
      const value = codec.decode(JSON.parse(str));
      const offset = Number(msg.message.offset);
      const partition = msg.partition;

      tracker.observe(partition, offset);

      return {
        value,
        ack: () => sync(() => tracker.complete(partition, offset)),
        nack: () => succeed(undefined),
        metadata: {
          topic: msg.topic,
          partition,
          offset: msg.message.offset,
          key: msg.message.key?.toString(),
          timestamp: msg.message.timestamp,
        },
      };
    };

    const register = (
      emitBatch: (batch: Envelope<T, Throws<KafkaError>>[]) => void,
      closeStream: () => void,
      failStream: (error: unknown) => void,
    ) => {
      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: offset?.type === "earliest" });

        if (autoCommit) {
          commitTimer = setInterval(
            () =>
              void flushCommits().catch((cause) =>
                failStream(toKafkaError("topic.commit", topic, cause)),
              ),
            commitIntervalMs,
          );
        }

        if (consumer.stream) {
          // Platformatic-style per-message iteration.
          await this.seekConsumer(consumer, offset);
          for await (const msg of consumer.stream()) {
            if (stopped) break;
            emitBatch([makeEnvelope(msg)]);
          }
        } else if (consumer.run) {
          if (batchEmit) {
            await this.runCallbackConsumer({
              consumer,
              offset,
              autoCommit: false,
              onBatch: async ({ batch }) => {
                if (stopped || batch.messages.length === 0) return;
                emitBatch(
                  batch.messages.map((message) =>
                    makeEnvelope({ topic: batch.topic, partition: batch.partition, message }),
                  ),
                );
              },
            });
          } else {
            await this.runCallbackConsumer({
              consumer,
              offset,
              autoCommit: false,
              onMessage: async (msg) => {
                if (stopped) return;
                emitBatch([makeEnvelope(msg)]);
              },
            });
          }
        }
      };

      return sync(() => {
        void run().then(
          () => {
            if (consumer.stream) closeStream();
          },
          (cause) => failStream(toKafkaError("topic.subscribe", topic, cause)),
        );
        return () => {};
      });
    };

    const stream = batchEmit
      ? Stream.asyncChunks<Envelope<T, Throws<KafkaError>>, Throws<KafkaError>>(
          (emit, closeStream, failStream) =>
            register((batch) => emit(Chunk.fromArray(batch)), closeStream, failStream),
        )
      : Stream.async<Envelope<T, Throws<KafkaError>>, Throws<KafkaError>>(
          (emit, closeStream, failStream) =>
            register(
              (batch) => {
                for (const envelope of batch) emit(envelope);
              },
              closeStream,
              failStream,
            ),
        );

    return {
      stream,
      consumer,
      topic,
      groupId,
      setPartitionLifecycle(next) {
        lifecycle = next;
      },
      close,
    };
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

  private async seekConsumer(consumer: KafkaConsumer, offset?: Offset): Promise<void> {
    if (!offset || !consumer.seek) return;

    if (offset.type === "timestamp") {
      const admin = this.kafka.admin();
      await admin.connect();
      const result = await admin.fetchTopicOffsetsByTimestamp(this.topic, offset.value);
      await admin.disconnect();
      for (const partition of result) {
        consumer.seek({
          topic: this.topic,
          partition: partition.partition,
          offset: partition.offset,
        });
      }
      return;
    }

    const target =
      offset.type === "earliest"
        ? KafkaOffset("-2")
        : offset.type === "latest"
          ? KafkaOffset("-1")
          : KafkaOffset(offset.value);
    const partitions = await this.fetchPartitions();
    for (let partition = 0; partition < partitions; partition++) {
      consumer.seek({
        topic: this.topic,
        partition: PartitionId(partition),
        offset: target,
      });
    }
  }

  private async runCallbackConsumer(params: {
    consumer: KafkaConsumer;
    offset?: Offset;
    autoCommit?: boolean;
    onMessage?: (message: KafkaMessage) => Promise<void>;
    onBatch?: (payload: KafkaBatchPayload) => Promise<void>;
  }): Promise<void> {
    const { consumer, offset, autoCommit, onMessage, onBatch } = params;
    if (!consumer.run) return;

    const run = (beforeDelivery?: () => Promise<boolean>): Promise<void> => {
      if (onBatch) {
        return consumer.run!({
          autoCommit,
          eachBatch: beforeDelivery
            ? async (payload) => {
                if (await beforeDelivery()) await onBatch(payload);
              }
            : onBatch,
        });
      }
      if (!onMessage) return Promise.resolve();
      return consumer.run!({
        autoCommit,
        eachMessage: beforeDelivery
          ? async (message) => {
              if (await beforeDelivery()) await onMessage(message);
            }
          : onMessage,
      });
    };

    if (!offset || !consumer.seek) {
      await run();
      return;
    }

    let replayReady = false;
    let notifyStarted!: () => void;
    let releaseBuffered!: () => void;
    const started = new Promise<void>((resolve) => {
      notifyStarted = resolve;
    });
    const buffered = new Promise<void>((resolve) => {
      releaseBuffered = resolve;
    });

    const running = run(async () => {
      if (!replayReady) {
        notifyStarted();
        await buffered;
        return false;
      }
      return true;
    });

    await Promise.race([running, started]);
    try {
      await this.seekConsumer(consumer, offset);
    } finally {
      replayReady = true;
      releaseBuffered();
    }
    await running;
  }

  private createConsumerStream(
    group?: ConsumerGroup,
    offset?: Offset,
  ): Stream<T, Throws<KafkaError>> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = group ?? this.groupId;
    const batchEmit = this.batchEmit;
    const consumerOptions = this.consumerOptions;

    const register = (
      emitBatch: (batch: T[]) => void,
      closeStream: () => void,
      failStream: (error: unknown) => void,
    ) => {
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

        if (consumer.stream) {
          // Platformatic stream mode — per-message iteration.
          await this.seekConsumer(consumer, offset);
          for await (const msg of consumer.stream()) {
            if (stopped) break;
            emitBatch([decodeMessage(msg)]);
          }
        } else if (consumer.run) {
          if (batchEmit) {
            await this.runCallbackConsumer({
              consumer,
              offset,
              onBatch: async ({ batch }) => {
                if (stopped || batch.messages.length === 0) return;
                emitBatch(
                  batch.messages.map((message) =>
                    decodeMessage({ topic: batch.topic, partition: batch.partition, message }),
                  ),
                );
              },
            });
          } else {
            await this.runCallbackConsumer({
              consumer,
              offset,
              onMessage: async (msg) => {
                if (stopped) return;
                emitBatch([decodeMessage(msg)]);
              },
            });
          }
        }
      };

      let cleaned = false;
      const cleanup = () => {
        if (cleaned) return;
        cleaned = true;
        stopped = true;
        void consumer.disconnect().catch(() => {});
      };

      return sync(() => {
        void run().then(
          () => {
            if (consumer.stream) closeStream();
          },
          (cause) => failStream(toKafkaError("topic.subscribe", topic, cause)),
        );
        return cleanup;
      });
    };

    return batchEmit
      ? Stream.asyncChunks<T, Throws<KafkaError>>((emit, closeStream, failStream) =>
          register((batch) => emit(Chunk.fromArray(batch)), closeStream, failStream),
        )
      : Stream.async<T, Throws<KafkaError>>((emit, closeStream, failStream) =>
          register(
            (batch) => {
              for (const value of batch) emit(value);
            },
            closeStream,
            failStream,
          ),
        );
  }

  // =========================================================================
  // Cleanup
  // =========================================================================

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
  }
}
