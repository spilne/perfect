// ---------------------------------------------------------------------------
// Branded topology identifiers.
//
// The shared connect layer owns these because topology runners and durable
// backend adapters exchange them without depending on each other.
// ---------------------------------------------------------------------------

export {
  ConsumerGroup,
  ChannelName,
  CheckpointName,
  TopologyId,
  StageId,
  TopologyInstanceId,
  SourceRecordId,
  StateCheckpointId,
  LeaseEpoch,
} from "@spilne/perfect-core/connect";
