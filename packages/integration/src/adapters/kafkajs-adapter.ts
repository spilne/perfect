// ---------------------------------------------------------------------------
// kafkajs adapter — wraps kafkajs into the KafkaClient interface
//
// Brand boundary: kafkajs speaks plain strings/numbers; identifiers coming
// OUT of the driver are branded here (TopicName / PartitionId / KafkaOffset).
// Branded values going IN need no unwrapping — brands are subtypes of their
// underlying primitives.
// ---------------------------------------------------------------------------

import { Kafka } from "kafkajs";
import type { KafkaClient, KafkaConsumer, KafkaProducer, KafkaAdmin } from "@perfect/kafka";
import { TopicName, PartitionId, KafkaOffset } from "@perfect/kafka";

export function createKafkajsClient(broker: string): KafkaClient {
  const kafka = new Kafka({ brokers: [broker], logLevel: 0 });

  return {
    producer(): KafkaProducer {
      const p = kafka.producer();
      return {
        connect: () => p.connect(),
        disconnect: () => p.disconnect(),
        send: async (params) => {
          await p.send(params);
        },
      };
    },

    consumer(config): KafkaConsumer {
      const c = kafka.consumer({
        groupId: config.groupId,
        sessionTimeout: config.sessionTimeout,
        heartbeatInterval: config.heartbeatInterval,
        rebalanceTimeout: config.maxPollInterval,
      });
      return {
        connect: () => c.connect(),
        disconnect: () => c.disconnect(),
        subscribe: (params) =>
          c.subscribe({ topic: params.topic, fromBeginning: params.fromBeginning }),
        run: (params) =>
          c.run({
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
                    },
                  })
              : undefined,
            eachBatch: params.eachBatch
              ? (payload) =>
                  params.eachBatch!({
                    batch: {
                      topic: TopicName(payload.batch.topic),
                      partition: PartitionId(payload.batch.partition),
                      messages: payload.batch.messages.map((m) => ({
                        key: m.key,
                        value: m.value,
                        offset: KafkaOffset(m.offset),
                        timestamp: m.timestamp ?? "",
                      })),
                    },
                  })
              : undefined,
          }),
        commitOffsets: (offsets) => c.commitOffsets(offsets),
        seek: (params) => c.seek(params),
      };
    },

    admin(): KafkaAdmin {
      const a = kafka.admin();
      return {
        connect: () => a.connect(),
        disconnect: () => a.disconnect(),
        fetchOffsets: async (params) => {
          const offsets = await a.fetchOffsets({
            groupId: params.groupId,
            topics: [...params.topics],
          });
          return offsets.map((t) => ({
            topic: TopicName(t.topic),
            partitions: t.partitions.map((p) => ({
              partition: PartitionId(p.partition),
              offset: KafkaOffset(p.offset),
            })),
          }));
        },
        fetchTopicOffsetsByTimestamp: async (topic, timestamp) => {
          const offsets = await a.fetchTopicOffsetsByTimestamp(topic, timestamp);
          return offsets.map((o) => ({
            partition: PartitionId(o.partition),
            offset: KafkaOffset(o.offset),
          }));
        },
        fetchTopicPartitionCount: async (topic) => {
          const metadata = await a.fetchTopicMetadata({ topics: [topic] });
          return metadata.topics[0]?.partitions.length ?? 1;
        },
        createTopics: async (params) => {
          await a.createTopics({ topics: [...params.topics], waitForLeaders: true });
        },
      };
    },
  };
}

export async function createTopic(broker: string, topic: string, partitions = 1): Promise<void> {
  const kafka = new Kafka({ brokers: [broker], logLevel: 0 });
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    waitForLeaders: true,
  });
  await admin.disconnect();
}
