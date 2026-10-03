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

import { fromPromise, succeed, sync } from "@spilne/perfect-core";
import type { Eff, Throws } from "@spilne/perfect-core";
import { Chunk, type EmitResult, Stream } from "@spilne/perfect-core/stream";
import { JsonCodec } from "@spilne/perfect-core/connect";
import type {
  KeyedSinkable,
  Partitionable,
  Replayable,
  Acknowledgeable,
  AcknowledgeOptions,
  ManagedAcknowledgeable,
  ManagedAcknowledgementSubscription,
  Checkpointable,
  ConsumerGroup,
  Envelope,
  Codec,
  Offset,
  Partition,
} from "@spilne/perfect-core/connect";
import type {
  KafkaAdmin,
  KafkaClient,
  KafkaConsumer,
  KafkaConsumerOptions,
  KafkaProducer,
  KafkaMessage,
  KafkaBatchPayload,
} from "./kafka-types.js";
import { type TopicName, type GroupId, PartitionId, KafkaOffset } from "./brands.js";
import { KafkaError, toKafkaError } from "./kafka-error.js";
import { AckSubscriptionLifecycle } from "./ack-subscription-lifecycle.js";

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

  // Connected once and shared. Kept as a promise so two publishes that
  // start together wait for the same producer instead of making two.
  private producer?: Promise<KafkaProducer>;
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
    await withAdmin(this.kafka, async (admin) => {
      if (admin.fetchTopicPartitionCount) {
        this._partitions = await admin.fetchTopicPartitionCount(this.topic);
      }
    });
    return this._partitions ?? 1;
  }

  private connectedProducer(): Promise<KafkaProducer> {
    if (this.producer === undefined) {
      const producer = this.kafka.producer();
      const ready = producer.connect().then(() => producer);
      this.producer = ready;
      // A failed connect is forgotten, so the next publish tries again.
      ready.catch(() => {
        if (this.producer === ready) this.producer = undefined;
      });
    }
    return this.producer;
  }

  // =========================================================================
  // Sinkable — publish messages
  // =========================================================================

  publish(value: T, params?: { key: string }): Eff<void, Throws<KafkaError>> {
    return fromPromise(
      async () => {
        const producer = await this.connectedProducer();
        const encoded = this.codec.encode(value);
        await producer.send({
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
        const producer = await this.connectedProducer();
        await producer.send({
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
    const lifecycle = new AckSubscriptionLifecycle({ consumer, topic, autoCommit });
    const tracker = lifecycle.tracker;

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
      emitBatch: (batch: Envelope<T, Throws<KafkaError>>[]) => EmitResult,
      closeStream: () => void,
      failStream: (error: unknown) => void,
    ) => {
      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: offset?.type === "earliest" });

        lifecycle.startCommitTimer({
          intervalMs: commitIntervalMs,
          onFailure: (cause) => failStream(toKafkaError("topic.commit", topic, cause)),
        });

        if (consumer.stream) {
          // Platformatic-style per-message iteration.
          await this.seekConsumer(consumer, offset);
          for await (const msg of consumer.stream()) {
            if (lifecycle.stopped) break;
            // Wait while the stream's buffer is full, so we stop reading
            // from Kafka until the consumer catches up.
            const wait = emitBatch([makeEnvelope(msg)]);
            if (wait) await wait;
          }
        } else if (consumer.run) {
          if (batchEmit) {
            await this.runCallbackConsumer({
              consumer,
              offset,
              autoCommit: false,
              onBatch: async ({ batch }) => {
                if (lifecycle.stopped || batch.messages.length === 0) return;
                // kafkajs waits for this promise before it fetches more, so
                // returning the backpressure promise slows Kafka down.
                await emitBatch(
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
                if (lifecycle.stopped) return;
                await emitBatch([makeEnvelope(msg)]);
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
                let wait: EmitResult = undefined;
                for (const envelope of batch) wait = emit(envelope) ?? wait;
                return wait;
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
        lifecycle.setPartitionLifecycle(next);
      },
      close: () => lifecycle.close(),
    };
  }

  // =========================================================================
  // Checkpointable — offset management
  // =========================================================================

  //
  // The Checkpointable contract stores a position as one string. For a
  // topic with one partition that is just the offset ("42"). A topic with
  // more partitions needs one offset per partition, written as
  // "partition:offset" pairs: "0:42,1:17". (Before, both methods silently
  // used partition 0 only.) commitOffsets / getCommittedOffsets take and
  // return a { partition: offset } map directly.

  async commitOffset(params: { group: ConsumerGroup; offset: string }): Promise<void> {
    const offsets = params.offset.includes(":")
      ? parseOffsetMap(params.offset)
      : await this.singlePartitionOffset(params.offset);
    await this.commitOffsets({ group: params.group, offsets });
  }

  async getCommittedOffset(params: { group: ConsumerGroup }): Promise<string | null> {
    const offsets = await this.getCommittedOffsets(params);
    const partitions = Object.keys(offsets);
    if (partitions.length === 0) return null;
    if (partitions.length === 1 && partitions[0] === "0") return offsets[0]!;
    return formatOffsetMap(offsets);
  }

  /** Commit one offset per partition, e.g. `{ 0: "42", 1: "17" }`. */
  async commitOffsets(params: {
    group: ConsumerGroup;
    offsets: Record<number, string>;
  }): Promise<void> {
    const consumer = this.kafka.consumer({ groupId: params.group });
    await consumer.connect();
    try {
      await consumer.commitOffsets(
        Object.entries(params.offsets).map(([partition, offset]) => ({
          topic: this.topic,
          partition: PartitionId(Number(partition)),
          offset: KafkaOffset(offset),
        })),
      );
    } finally {
      await consumer.disconnect();
    }
  }

  /** The committed offset of every partition that has one. */
  async getCommittedOffsets(params: { group: ConsumerGroup }): Promise<Record<number, string>> {
    const offsets = await withAdmin(this.kafka, (admin) =>
      admin.fetchOffsets({ groupId: params.group, topics: [this.topic] }),
    );
    const result: Record<number, string> = {};
    const topicOffsets = offsets.find((o) => o.topic === this.topic);
    for (const { partition, offset } of topicOffsets?.partitions ?? []) {
      // Kafka reports "-1" for a partition with nothing committed yet.
      if (offset !== "-1") result[partition] = offset;
    }
    return result;
  }

  private async singlePartitionOffset(offset: string): Promise<Record<number, string>> {
    const partitions = await this.fetchPartitions();
    if (partitions > 1) {
      throw new Error(
        `Topic ${this.topic} has ${partitions} partitions. Pass one offset per partition ` +
          `("0:42,1:17") or use commitOffsets.`,
      );
    }
    return { 0: offset };
  }

  // =========================================================================
  // Internal — consumer stream creation
  // =========================================================================

  private async seekConsumer(consumer: KafkaConsumer, offset?: Offset): Promise<void> {
    if (!offset || !consumer.seek) return;

    if (offset.type === "timestamp") {
      const result = await withAdmin(this.kafka, (admin) =>
        admin.fetchTopicOffsetsByTimestamp(this.topic, offset.value),
      );
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
      emitBatch: (batch: T[]) => EmitResult,
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
            const wait = emitBatch([decodeMessage(msg)]);
            if (wait) await wait;
          }
        } else if (consumer.run) {
          if (batchEmit) {
            await this.runCallbackConsumer({
              consumer,
              offset,
              onBatch: async ({ batch }) => {
                if (stopped || batch.messages.length === 0) return;
                await emitBatch(
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
                await emitBatch([decodeMessage(msg)]);
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
              let wait: EmitResult = undefined;
              for (const value of batch) wait = emit(value) ?? wait;
              return wait;
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
    const producer = this.producer;
    this.producer = undefined;
    if (producer !== undefined) await (await producer).disconnect();
  }
}

// Connect an admin client, use it, and always disconnect it, even when
// `use` throws. (Before, a throw left the client connected.)
async function withAdmin<A>(
  kafka: KafkaClient,
  use: (admin: KafkaAdmin) => Promise<A>,
): Promise<A> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    return await use(admin);
  } finally {
    await admin.disconnect();
  }
}

/** "0:42,1:17" → { 0: "42", 1: "17" } */
export function parseOffsetMap(text: string): Record<number, string> {
  const offsets: Record<number, string> = {};
  for (const pair of text.split(",")) {
    const [partition, offset] = pair.split(":");
    if (partition === undefined || offset === undefined || !/^\d+$/.test(partition.trim())) {
      throw new Error(`Invalid Kafka offset "${text}"; expected "partition:offset" pairs`);
    }
    offsets[Number(partition)] = offset.trim();
  }
  return offsets;
}

/** { 0: "42", 1: "17" } → "0:42,1:17" */
export function formatOffsetMap(offsets: Record<number, string>): string {
  return Object.entries(offsets)
    .map(([partition, offset]) => `${partition}:${offset}`)
    .join(",");
}
