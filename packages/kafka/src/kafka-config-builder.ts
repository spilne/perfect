import type { Codec } from "@perfect/core/connect";
import { GroupId, TopicName } from "./brands";
import { KafkaTopic } from "./kafka-topic";
import type { KafkaTopicConfig } from "./kafka-topic";
import type { KafkaClient, KafkaConsumerOptions } from "./kafka-types";

export class KafkaConfigBuilder<T> {
  private kafkaClient?: KafkaClient;
  private topicName?: TopicName;
  private groupId?: GroupId;
  private messageCodec?: Codec<T>;
  private emitBatches?: boolean;
  private options?: Omit<KafkaConsumerOptions, "groupId">;

  static forType<T>(): KafkaConfigBuilder<T> {
    return new KafkaConfigBuilder<T>();
  }

  client(kafka: KafkaClient): this {
    this.kafkaClient = kafka;
    return this;
  }

  topic(topic: string | TopicName): this {
    this.topicName = TopicName(topic);
    return this;
  }

  group(groupId: string | GroupId): this {
    this.groupId = GroupId(groupId);
    return this;
  }

  codec(codec: Codec<T>): this {
    this.messageCodec = codec;
    return this;
  }

  batchEmit(enabled = true): this {
    this.emitBatches = enabled;
    return this;
  }

  consumer(options: Omit<KafkaConsumerOptions, "groupId">): this {
    this.options = options;
    return this;
  }

  toConfig(): KafkaTopicConfig<T> {
    if (!this.kafkaClient) throw new TypeError("Kafka config requires a client");
    if (!this.topicName) throw new TypeError("Kafka config requires a topic");
    if (!this.groupId) throw new TypeError("Kafka config requires a consumer group");
    return {
      kafka: this.kafkaClient,
      topic: this.topicName,
      groupId: this.groupId,
      codec: this.messageCodec,
      batchEmit: this.emitBatches,
      consumerOptions: this.options,
    };
  }

  build(): KafkaTopic<T> {
    return new KafkaTopic(this.toConfig());
  }
}

export function kafkaConfig<T>(): KafkaConfigBuilder<T> {
  return KafkaConfigBuilder.forType<T>();
}
