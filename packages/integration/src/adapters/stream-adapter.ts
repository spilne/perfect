// ---------------------------------------------------------------------------
// @platformatic/kafka adapter — wraps platformatic's stream-based consumer
// into the KafkaClient interface
//
// Note: requires Kafka API versions Redpanda doesn't fully implement — run
// this adapter against real Apache Kafka (see withApacheKafka + KAFKA_FULL=1
// in the test suite).
// ---------------------------------------------------------------------------

import { Producer, Consumer, Admin } from "@platformatic/kafka";
import type {
  KafkaClient,
  KafkaConsumer,
  KafkaProducer,
  KafkaAdmin,
  KafkaMessage,
} from "@perfect/kafka";
import { TopicName, PartitionId, KafkaOffset } from "@perfect/kafka";

export function createPlatformaticClient(broker: string): KafkaClient {
  return {
    producer(): KafkaProducer {
      const p = new Producer({
        bootstrapBrokers: [broker],
        clientId: "perfect-producer",
      });

      return {
        connect: async () => {},
        disconnect: async () => {
          await p.close();
        },
        send: async (params) => {
          await (p as any).send({
            messages: params.messages.map((m) => ({
              topic: params.topic,
              key: m.key ? Buffer.from(m.key) : undefined,
              value: Buffer.from(m.value),
            })),
          });
        },
      };
    },

    consumer(config): KafkaConsumer {
      const c = new Consumer({
        bootstrapBrokers: [broker],
        clientId: `perfect-consumer-${crypto.randomUUID().slice(0, 8)}`,
        groupId: config.groupId,
      });

      let subscribedTopic = "";
      let fromBeginning = false;
      let stopped = false;

      return {
        connect: async () => {},
        disconnect: async () => {
          stopped = true;
          await c.close();
        },

        subscribe: async (params) => {
          subscribedTopic = params.topic;
          fromBeginning = params.fromBeginning ?? false;
        },

        stream(): AsyncIterable<KafkaMessage> {
          const consumeStream = (c as any).consume({
            topics: [subscribedTopic],
            mode: fromBeginning ? "earliest" : "latest",
            autocommit: false,
          });

          return {
            [Symbol.asyncIterator]() {
              return {
                async next() {
                  if (stopped) return { value: undefined as any, done: true };
                  try {
                    const msg = await consumeStream;
                    if (!msg) return { value: undefined as any, done: true };

                    // Platformatic returns messages via for-await
                    const result: KafkaMessage = {
                      topic: TopicName((msg as any).topic ?? subscribedTopic),
                      partition: PartitionId((msg as any).partition ?? 0),
                      message: {
                        key: (msg as any).key?.toString() ?? null,
                        value: (msg as any).value?.toString() ?? null,
                        offset: KafkaOffset(String((msg as any).offset ?? 0)),
                        timestamp: String((msg as any).timestamp ?? Date.now()),
                      },
                    };
                    return { value: result, done: false };
                  } catch {
                    return { value: undefined as any, done: true };
                  }
                },
              };
            },
          };
        },

        commitOffsets: async (offsets) => {
          await (c as any).commit({
            offsets: offsets.map((o) => ({
              topic: o.topic,
              partition: o.partition,
              offset: BigInt(o.offset),
            })),
          });
        },
      };
    },

    admin(): KafkaAdmin {
      const a = new Admin({
        bootstrapBrokers: [broker],
        clientId: "perfect-admin",
      });

      return {
        connect: async () => {},
        disconnect: async () => {
          await a.close();
        },

        fetchOffsets: async (params) => {
          const result = await (a as any).listConsumerGroupOffsets({
            groups: [{ groupId: params.groupId }],
          });

          return params.topics.map((topic) => ({
            topic,
            partitions: (result as any[])
              .flatMap((g: any) => g.partitions ?? [])
              .filter((p: any) => p.topic === topic)
              .map((p: any) => ({
                partition: PartitionId(p.partition),
                offset: KafkaOffset(String(p.offset)),
              })),
          }));
        },

        fetchTopicOffsetsByTimestamp: async (topic, timestamp) => {
          const result = await (a as any).listOffsets({
            topics: [{ name: topic, partitions: [{ index: 0, timestamp: BigInt(timestamp) }] }],
          });

          return (result as any[]).map((p: any) => ({
            partition: PartitionId(p.partition ?? 0),
            offset: KafkaOffset(String(p.offset ?? 0)),
          }));
        },
      };
    },
  };
}
