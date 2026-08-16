// @perfect/core/connect — queue-agnostic endpoint contracts.
//
// The "any queue" layer: capability interfaces every messaging backend
// implements (Kafka, Redis Streams, in-memory, …) plus the helpers that are
// shared across log-shaped backends. Backends live in their own packages
// (@perfect/kafka, …); this subpath has no backend dependencies.

export type {
  Streamable,
  Sinkable,
  KeyedSinkable,
  Partitionable,
  Replayable,
  Acknowledgeable,
  Checkpointable,
  Envelope,
  Offset,
  ShuffleTransport,
  LeaderElection,
} from "./contracts";

export { ConsumerGroup, Partition, ChannelName } from "./contracts";

export {
  isStreamable,
  isSinkable,
  isKeyedSinkable,
  isPartitionable,
  isReplayable,
  isAcknowledgeable,
  isCheckpointable,
} from "./contracts";

export type { Codec } from "./codec";
export { JsonCodec, codecFromSchema, codecTuple, codecRecord, codecArray } from "./codec";
export { LosslessJsonCodec } from "./lossless-codec";
export { canonicalJSON, payloadHash } from "./canonicalize";

export { OffsetTracker } from "./offset-tracker";
export { type StateBackend, CheckpointName, InMemoryState } from "./state-backend";
export { autoCommitBatchWithin } from "./auto-commit";
