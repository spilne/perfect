// @perfect/kafka — Kafka backend for the @perfect/core/connect contracts.
//
// Kafka-specific by design: offsets, partitions, consumer groups, and the
// eachMessage/stream driver model live here. The queue-agnostic layer
// (Envelope, OffsetTracker, autoCommitBatchWithin, …) is @perfect/core/connect.

export { TopicName, GroupId, PartitionId, KafkaOffset } from "./brands";
export {
  KafkaTopic,
  type KafkaTopicConfig,
  type KafkaAckOptions,
  type KafkaAckSubscription,
} from "./kafka-topic";
export { commitBatchWithin, type CommitBatchWithinConfig } from "./commit-batch-within";
export { KafkaCommitError } from "./kafka-error";
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
