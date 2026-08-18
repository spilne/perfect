import { Kafka, logLevel } from "kafkajs";
import type { KafkaConfig } from "kafkajs";
import type { KafkaAdmin, KafkaClient, KafkaConsumer, KafkaProducer } from "@perfect/kafka";
import { KafkaOffset, PartitionId, TopicName } from "@perfect/kafka";

export interface KafkajsAdapterConfig extends Omit<KafkaConfig, "brokers"> {
  readonly brokers: string[];
}

export function createKafkajsClient(
  config: string | readonly string[] | KafkajsAdapterConfig,
): KafkaClient {
  const kafka = new Kafka(normalizeConfig(config));

  return {
    producer(): KafkaProducer {
      const producer = kafka.producer();
      return {
        connect: () => producer.connect(),
        disconnect: () => producer.disconnect(),
        send: async (params) => {
          await producer.send(params);
        },
      };
    },

    consumer(config): KafkaConsumer {
      const consumer = kafka.consumer({
        groupId: config.groupId,
        sessionTimeout: config.sessionTimeout,
        heartbeatInterval: config.heartbeatInterval,
        rebalanceTimeout: config.maxPollInterval,
      });

      return {
        connect: () => consumer.connect(),
        disconnect: () => consumer.disconnect(),
        subscribe: (params) => consumer.subscribe(params),
        run: (params) =>
          consumer.run({
            autoCommit: params.autoCommit,
            eachMessage: params.eachMessage
              ? (payload) =>
                  params.eachMessage!({
                    topic: TopicName(payload.topic),
                    partition: PartitionId(payload.partition),
                    message: {
                      key: payload.message.key,
                      value: payload.message.value,
                      offset: KafkaOffset(payload.message.offset),
                      timestamp: payload.message.timestamp ?? "",
                      headers: payload.message.headers,
                    },
                  })
              : undefined,
            eachBatch: params.eachBatch
              ? (payload) =>
                  params.eachBatch!({
                    batch: {
                      topic: TopicName(payload.batch.topic),
                      partition: PartitionId(payload.batch.partition),
                      messages: payload.batch.messages.map((message) => ({
                        key: message.key,
                        value: message.value,
                        offset: KafkaOffset(message.offset),
                        timestamp: message.timestamp ?? "",
                        headers: message.headers,
                      })),
                    },
                  })
              : undefined,
          }),
        commitOffsets: (offsets) => consumer.commitOffsets(offsets),
        seek: (params) => consumer.seek(params),
      };
    },

    admin(): KafkaAdmin {
      const admin = kafka.admin();
      return {
        connect: () => admin.connect(),
        disconnect: () => admin.disconnect(),
        fetchOffsets: async (params) => {
          const offsets = await admin.fetchOffsets({
            groupId: params.groupId,
            topics: [...params.topics],
          });
          return offsets.map((topic) => ({
            topic: TopicName(topic.topic),
            partitions: topic.partitions.map((partition) => ({
              partition: PartitionId(partition.partition),
              offset: KafkaOffset(partition.offset),
            })),
          }));
        },
        fetchTopicOffsetsByTimestamp: async (topic, timestamp) => {
          const offsets = await admin.fetchTopicOffsetsByTimestamp(topic, timestamp);
          return offsets.map((offset) => ({
            partition: PartitionId(offset.partition),
            offset: KafkaOffset(offset.offset),
          }));
        },
        fetchTopicPartitionCount: async (topic) => {
          const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
          return metadata.topics[0]?.partitions.length ?? 1;
        },
        createTopics: async (params) => {
          await admin.createTopics({ topics: [...params.topics], waitForLeaders: true });
        },
      };
    },
  };
}

export async function createKafkajsTopic(
  config: string | readonly string[] | KafkajsAdapterConfig,
  topic: string,
  partitions = 1,
): Promise<void> {
  const kafka = new Kafka(normalizeConfig(config));
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
      waitForLeaders: true,
    });
  } finally {
    await admin.disconnect();
  }
}

function normalizeConfig(
  config: string | readonly string[] | KafkajsAdapterConfig,
): KafkajsAdapterConfig {
  if (typeof config === "string") return { brokers: [config], logLevel: logLevel.NOTHING };
  if (Array.isArray(config)) return { brokers: [...config], logLevel: logLevel.NOTHING };
  return config as KafkajsAdapterConfig;
}
