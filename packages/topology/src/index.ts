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
} from "./brands.js";

export {
  StreamTopology,
  KeyedTopology,
  WindowedTopology,
  BuiltTopology,
} from "./stream-topology.js";

export { TopologyRunner } from "./topology-runner.js";

export { DistributedRunner } from "./distributed-runner.js";
export type { DistributedTopologyConfig } from "./distributed-runner.js";

export { planStages } from "./stage-planner.js";
export type { StagePlan, TopologyStage } from "./stage-planner.js";

export { analyze as analyzeTopology } from "./topology-analyzer.js";
export type { TopologyWarning } from "./topology-analyzer.js";

export { WindowManager } from "./window-manager.js";

export { JoinBuffer, type JoinedPair } from "./join-buffer.js";

export { InMemoryState } from "./state-backend.js";
export type { StateBackend } from "./state-backend.js";

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
} from "./types.js";
