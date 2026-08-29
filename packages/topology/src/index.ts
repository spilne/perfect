// @spilne/perfect-topology — stateful stream processing on top of @spilne/perfect-core.
//
// Declarative processing DAGs with keyed state, time windows, stream joins,
// deduplication, and checkpointing. Ported from promin's stream-topology
// (StreamPipeline → Stream; ShuffleTransport now lives in @spilne/perfect-core/connect).

export {
  StageId,
  ConsumerGroup,
  ChannelName,
  CheckpointName,
  TopologyId,
  TopologyInstanceId,
  SourceRecordId,
  StateCheckpointId,
  LeaseEpoch,
} from "./brands";

export { StreamTopology, KeyedTopology, WindowedTopology, BuiltTopology } from "./stream-topology";

export { TopologyRunner } from "./topology-runner";

export { DistributedRunner } from "./distributed-runner";
export type { DistributedTopologyConfig } from "./distributed-runner";

export { planStages } from "./stage-planner";
export type { StagePlan, TopologyStage } from "./stage-planner";

export { analyze as analyzeTopology } from "./topology-analyzer";
export type { TopologyWarning } from "./topology-analyzer";

export { WindowManager } from "./window-manager";

export { JoinBuffer, type JoinedPair } from "./join-buffer";

export { InMemoryState } from "./state-backend";
export type { StateBackend } from "./state-backend";

export type { ShuffleTransport } from "@spilne/perfect-core/connect";

export type {
  TimeWindow,
  WindowType,
  AggregateSpec,
  ProcessSpec,
  JoinConfig,
  TopologyConfig,
  TopologyHandle,
  TopologyMetrics,
  BackpressureStats,
  CompiledTopology,
  ShuffleNode,
} from "./types";
