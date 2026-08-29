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
} from "./contracts";

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
} from "./contracts";

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
} from "./contracts";

export type { Codec } from "./codec";
export { JsonCodec, codecFromSchema, codecTuple, codecRecord, codecArray } from "./codec";
export { LosslessJsonCodec } from "./lossless-codec";
export { canonicalJSON, payloadHash } from "./canonicalize";

export { OffsetTracker } from "./offset-tracker";
export { type StateBackend, CheckpointName, InMemoryState } from "./state-backend";
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
} from "./partitioned-state-backend";
export { AckError, autoCommitBatchWithin } from "./auto-commit";
