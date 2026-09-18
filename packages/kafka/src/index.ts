// @spilne/perfect-kafka — Kafka backend for the @spilne/perfect-core/connect contracts.
//
// Kafka-specific by design: offsets, partitions, consumer groups, and the
// eachMessage/stream driver model live here. The queue-agnostic layer
// (Envelope, OffsetTracker, autoCommitBatchWithin, …) is @spilne/perfect-core/connect.

export { TopicName, GroupId, PartitionId, KafkaOffset } from "./brands.js";
export {
  KafkaTopic,
  type KafkaTopicConfig,
  type KafkaAckOptions,
  type KafkaAckSubscription,
} from "./kafka-topic.js";
export { commitBatchWithin, type CommitBatchWithinConfig } from "./commit-batch-within.js";
export { KafkaCommitError } from "./kafka-error.js";
export { KafkaError } from "./kafka-error.js";
export { KafkaConfigBuilder, kafkaConfig } from "./kafka-config-builder.js";
export {
  KafkaShuffleTransport,
  type KafkaShuffleTransportConfig,
} from "./kafka-shuffle-transport.js";
export type {
  KafkaClient,
  KafkaConsumer,
  KafkaConsumerOptions,
  KafkaPartitionAssignment,
  KafkaProducer,
  KafkaAdmin,
  KafkaMessage,
  KafkaBatchPayload,
  KafkaOutgoingMessage,
  KafkaOffsetCommit,
  KafkaTopicOffsets,
  KafkaPartitionOffset,
} from "./kafka-types.js";
