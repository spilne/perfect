import { Admin, Consumer, Producer } from "@platformatic/kafka";
import type {
  AdminOptions,
  ConsumerOptions,
  Message,
  MessagesStream,
  ProducerOptions,
} from "@platformatic/kafka";
import type {
  KafkaAdmin,
  KafkaClient,
  KafkaConsumer,
  KafkaMessage,
  KafkaProducer,
} from "@perfect/kafka";
import { KafkaOffset, PartitionId, TopicName } from "@perfect/kafka";

type BinaryConsumerOptions = ConsumerOptions<Buffer, Buffer, Buffer, Buffer>;
type BinaryProducerOptions = ProducerOptions<Buffer, Buffer, Buffer, Buffer>;

declare global {
  interface RequestInit {
    dispatcher?: unknown;
  }
}

export interface PlatformaticAdapterConfig {
  readonly bootstrapBrokers: string[];
  readonly clientId?: string;
  readonly producerOptions?: Omit<BinaryProducerOptions, "bootstrapBrokers" | "clientId">;
  readonly consumerOptions?: Omit<
    BinaryConsumerOptions,
    "bootstrapBrokers" | "clientId" | "groupId"
  >;
  readonly adminOptions?: Omit<AdminOptions, "bootstrapBrokers" | "clientId">;
  readonly streamBufferCapacity?: number;
}

export function createPlatformaticClient(
  config: string | readonly string[] | PlatformaticAdapterConfig,
): KafkaClient {
  const normalized = normalizeConfig(config);
  const bootstrapBrokers = normalized.bootstrapBrokers;
  const clientId = normalized.clientId ?? "perfect";
  const streamBufferCapacity = Math.max(1, normalized.streamBufferCapacity ?? 128);

  return {
    producer(): KafkaProducer {
      const producer = new Producer({
        ...normalized.producerOptions,
        bootstrapBrokers,
        clientId: `${clientId}-producer`,
      });

      return {
        connect: async () => {},
        disconnect: () => producer.close(),
        send: async (params) => {
          await producer.send({
            messages: params.messages.map((message) => ({
              topic: params.topic,
              key: message.key === null ? undefined : Buffer.from(message.key),
              value: Buffer.from(message.value),
              headers: message.headers
                ? Object.fromEntries(
                    Object.entries(message.headers).map(([key, value]) => [
                      key,
                      Buffer.from(value),
                    ]),
                  )
                : undefined,
            })),
          });
        },
      };
    },

    consumer(config): KafkaConsumer {
      const configuredConsumer = normalized.consumerOptions ?? {};
      const usesConsumerGroupProtocol = configuredConsumer.groupProtocol === "consumer";
      const consumer = new Consumer({
        ...normalized.consumerOptions,
        bootstrapBrokers,
        clientId: `${clientId}-consumer-${crypto.randomUUID().slice(0, 8)}`,
        groupId: config.groupId,
        ...(usesConsumerGroupProtocol
          ? {}
          : {
              sessionTimeout:
                config.sessionTimeout ??
                ("sessionTimeout" in configuredConsumer
                  ? configuredConsumer.sessionTimeout
                  : undefined) ??
                30_000,
              heartbeatInterval:
                config.heartbeatInterval ??
                ("heartbeatInterval" in configuredConsumer
                  ? configuredConsumer.heartbeatInterval
                  : undefined) ??
                3_000,
              rebalanceTimeout:
                config.maxPollInterval ?? configuredConsumer.rebalanceTimeout ?? 60_000,
            }),
      } as BinaryConsumerOptions);

      let subscribedTopic = "";
      let fromBeginning = false;
      let stopped = false;
      let activeStream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | null = null;
      const offsets = new Map<number, bigint>();

      return {
        connect: async () => {},
        disconnect: async () => {
          stopped = true;
          const stream = activeStream;
          activeStream = null;
          if (stream?.isActive()) await stream.close();
          await consumer.close();
        },
        subscribe: async (params) => {
          subscribedTopic = params.topic;
          fromBeginning = params.fromBeginning ?? false;
        },
        async *stream(): AsyncIterable<KafkaMessage> {
          const stream = await consumer.consume(
            offsets.size > 0
              ? {
                  topics: [subscribedTopic],
                  mode: "manual",
                  offsets: [...offsets].map(([partition, offset]) => ({
                    topic: subscribedTopic,
                    partition,
                    offset,
                  })),
                  autocommit: false,
                }
              : {
                  topics: [subscribedTopic],
                  mode: fromBeginning ? "earliest" : "latest",
                  autocommit: false,
                },
          );
          activeStream = stream;

          try {
            for await (const message of eventIterable(stream, streamBufferCapacity)) {
              if (stopped) break;
              yield toKafkaMessage(message);
            }
          } finally {
            if (activeStream === stream) activeStream = null;
            if (stream.isActive()) await stream.close();
          }
        },
        commitOffsets: async (commits) => {
          await consumer.commit({
            offsets: commits.map((commit) => ({
              topic: commit.topic,
              partition: commit.partition,
              offset: BigInt(commit.offset),
              leaderEpoch: -1,
            })),
          });
        },
        seek: (params) => {
          if (params.offset === "-2") {
            fromBeginning = true;
            offsets.delete(params.partition);
          } else if (params.offset === "-1") {
            fromBeginning = false;
            offsets.delete(params.partition);
          } else {
            offsets.set(params.partition, BigInt(params.offset));
          }
        },
      };
    },

    admin(): KafkaAdmin {
      const admin = new Admin({
        ...normalized.adminOptions,
        bootstrapBrokers,
        clientId: `${clientId}-admin`,
      });

      return {
        connect: async () => {},
        disconnect: () => admin.close(),
        fetchOffsets: async (params) => {
          const groups = await admin.listConsumerGroupOffsets({ groups: [params.groupId] });
          const group = groups.find((candidate) => candidate.groupId === params.groupId);
          return params.topics.map((topic) => {
            const result = group?.topics.find((candidate) => candidate.name === topic);
            return {
              topic,
              partitions: (result?.partitions ?? []).map((partition) => ({
                partition: PartitionId(partition.partitionIndex),
                offset: KafkaOffset(String(partition.committedOffset)),
              })),
            };
          });
        },
        fetchTopicOffsetsByTimestamp: async (topic, timestamp) => {
          const metadata = await admin.metadata({ topics: [topic] });
          const count = metadata.topics.get(topic)?.partitionsCount ?? 1;
          const topics = await admin.listOffsets({
            topics: [
              {
                name: topic,
                partitions: Array.from({ length: count }, (_, partitionIndex) => ({
                  partitionIndex,
                  timestamp: BigInt(timestamp),
                })),
              },
            ],
          });
          return (topics[0]?.partitions ?? []).map((partition) => ({
            partition: PartitionId(partition.partitionIndex),
            offset: KafkaOffset(String(partition.offset)),
          }));
        },
        fetchTopicPartitionCount: async (topic) => {
          const metadata = await admin.metadata({ topics: [topic] });
          return metadata.topics.get(topic)?.partitionsCount ?? 1;
        },
        createTopics: async (params) => {
          for (const topic of params.topics) {
            await admin.createTopics({
              topics: [topic.topic],
              partitions: topic.numPartitions,
              replicas: topic.replicationFactor,
            });
          }
        },
      };
    },
  };
}

async function* eventIterable<T>(
  stream: MessagesStream<T, T, T, T>,
  capacity: number,
): AsyncIterable<Message<T, T, T, T>> {
  const buffer: Message<T, T, T, T>[] = [];
  let terminalError: unknown;
  let ended = false;
  let wake: (() => void) | null = null;

  const signal = () => {
    const waiter = wake;
    wake = null;
    waiter?.();
  };
  const onData = (message: Message<T, T, T, T>) => {
    buffer.push(message);
    if (buffer.length >= capacity) stream.pause();
    signal();
  };
  const onError = (error: Error) => {
    terminalError = error;
    ended = true;
    signal();
  };
  const onEnd = () => {
    ended = true;
    signal();
  };

  stream.on("data", onData);
  stream.once("error", onError);
  stream.once("end", onEnd);
  stream.once("close", onEnd);
  stream.resume();

  try {
    while (true) {
      const message = buffer.shift();
      if (message) {
        if (stream.isPaused() && buffer.length < capacity) stream.resume();
        yield message;
        continue;
      }
      if (terminalError !== undefined) throw terminalError;
      if (ended) return;
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
  } finally {
    stream.off("data", onData);
    stream.off("error", onError);
    stream.off("end", onEnd);
    stream.off("close", onEnd);
  }
}

function normalizeConfig(
  config: string | readonly string[] | PlatformaticAdapterConfig,
): PlatformaticAdapterConfig {
  if (typeof config === "string") return { bootstrapBrokers: [config] };
  if (Array.isArray(config)) return { bootstrapBrokers: [...config] };
  return config as PlatformaticAdapterConfig;
}

function toKafkaMessage(message: Message<Buffer, Buffer, Buffer, Buffer>): KafkaMessage {
  return {
    topic: TopicName(message.topic),
    partition: PartitionId(message.partition),
    message: {
      key: message.key ?? null,
      value: message.value ?? null,
      offset: KafkaOffset(String(message.offset)),
      timestamp: String(message.timestamp),
      headers: Object.fromEntries(
        [...message.headers].map(([key, value]) => [key.toString(), value]),
      ),
    },
  };
}
