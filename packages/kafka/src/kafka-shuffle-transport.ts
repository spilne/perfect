// ---------------------------------------------------------------------------
// KafkaShuffleTransport — ShuffleTransport implementation backed by Kafka
//
// Creates repartition channels using KafkaTopic. Each channel is a Kafka topic
// that data is published to (keyed by the keyBy function) and consumed from.
// ---------------------------------------------------------------------------

import type {
  ShuffleTransport,
  ChannelName,
  ConsumerGroup,
  Codec,
  Streamable,
  Acknowledgeable,
  KeyedSinkable,
} from "@perfect/core/connect";
import { KafkaTopic } from "./kafka-topic";
import type { KafkaClient } from "./kafka-types";
import { TopicName } from "./brands";

export interface KafkaShuffleTransportConfig {
  /** Kafka client instance. */
  kafka: KafkaClient;
  /** Number of partitions for repartition topics. Default: 6. */
  partitions?: number;
}

export class KafkaShuffleTransport implements ShuffleTransport {
  private readonly kafka: KafkaClient;
  private readonly partitions: number;
  private readonly topics = new Map<ChannelName, KafkaTopic<unknown>>();

  constructor(config: KafkaShuffleTransportConfig) {
    this.kafka = config.kafka;
    this.partitions = config.partitions ?? 6;
  }

  async getOrCreateRepartitionChannel<T>(params: {
    name: ChannelName;
    group: ConsumerGroup;
    codec: Codec<T>;
  }): Promise<{
    source: Streamable<T> & Acknowledgeable<T>;
    sink: KeyedSinkable<T>;
  }> {
    // A repartition channel is realized as a Kafka topic of the same name.
    const topicName = TopicName(params.name);

    // Ensure topic exists
    const admin = this.kafka.admin();
    await admin.connect();
    if (admin.createTopics) {
      await admin.createTopics({
        topics: [
          {
            topic: topicName,
            numPartitions: this.partitions,
            replicationFactor: 1,
          },
        ],
      });
    }
    await admin.disconnect();

    // Create KafkaTopic for this repartition channel
    const kt = new KafkaTopic<T>({
      kafka: this.kafka,
      topic: topicName,
      groupId: params.group,
      codec: params.codec,
    });
    this.topics.set(params.name, kt as KafkaTopic<unknown>);

    return { source: kt, sink: kt };
  }

  /** Disconnect all managed topics. */
  async disconnect(): Promise<void> {
    await Promise.all([...this.topics.values()].map((t) => t.disconnect()));
    this.topics.clear();
  }
}
