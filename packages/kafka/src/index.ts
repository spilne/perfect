// @perfect/kafka — Kafka backend for the @perfect/core/connect contracts.
//
// Kafka-specific by design: offsets, partitions, consumer groups, and the
// eachMessage/stream driver model live here. The queue-agnostic layer
// (Envelope, OffsetTracker, autoCommitBatchWithin, …) is @perfect/core/connect.

export { KafkaTopic, type KafkaTopicConfig } from "./kafka-topic";
export { commitBatchWithin, type CommitBatchWithinConfig } from "./commit-batch-within";
export { KafkaShuffleTransport, type KafkaShuffleTransportConfig } from "./kafka-shuffle-transport";
export type {
  KafkaClient,
  KafkaConsumer,
  KafkaConsumerOptions,
  KafkaProducer,
  KafkaAdmin,
  KafkaMessage,
  KafkaBatchPayload,
  KafkaOutgoingMessage,
  KafkaOffsetCommit,
  KafkaTopicOffsets,
  KafkaPartitionOffset,
} from "./kafka-types";
