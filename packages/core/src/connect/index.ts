// @spilne/perfect-core/connect — queue-agnostic endpoint contracts.
//
// The "any queue" layer: capability interfaces every messaging backend
// implements (Kafka, Redis Streams, in-memory, …) plus the helpers that are
// shared across log-shaped backends. Backends live in their own packages
// (@spilne/perfect-kafka, …); this subpath has no backend dependencies.

export type {
  Streamable,
  Sinkable,
  TransactionalSinkable,
  KeyedSinkable,
  Partitionable,
  Replayable,
  Acknowledgeable,
  AcknowledgeOptions,
  ManagedAcknowledgeable,
  ManagedAcknowledgementSubscription,
  PartitionAssignment,
  PartitionLifecycle,
  Checkpointable,
  Envelope,
  TransactionalEnvelope,
  Offset,
  ShuffleTransport,
  LeaderElection,
} from "./contracts.js";

export {
  ConsumerGroup,
  Partition,
  ChannelName,
  TopologyId,
  StageId,
  TopologyInstanceId,
  SourceRecordId,
  StateCheckpointId,
  LeaseEpoch,
} from "./contracts.js";

export {
  isStreamable,
  isSinkable,
  isTransactionalSinkable,
  isKeyedSinkable,
  isPartitionable,
  isReplayable,
  isAcknowledgeable,
  isManagedAcknowledgeable,
  isTransactionalEnvelope,
  isCheckpointable,
} from "./contracts.js";

export type { Codec } from "./codec.js";
export { JsonCodec, codecFromSchema, codecTuple, codecRecord, codecArray } from "./codec.js";
export { LosslessJsonCodec } from "./lossless-codec.js";
export { canonicalJSON, payloadHash } from "./canonicalize.js";

export { OffsetTracker } from "./offset-tracker.js";
export { type StateBackend, CheckpointName, InMemoryState } from "./state-backend.js";
export {
  type StatePartitionScope,
  type StatePartitionLease,
  type StateMutation,
  type PartitionStateCommit,
  type PartitionCommitResult,
  type PartitionStateSnapshot,
  type PartitionedStateBackend,
  type TransactionalPartitionedStateBackend,
  InMemoryPartitionedState,
  isTransactionalPartitionedStateBackend,
} from "./partitioned-state-backend.js";
export { AckError, autoCommitBatchWithin } from "./auto-commit.js";
