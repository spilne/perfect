// ---------------------------------------------------------------------------
// StreamTopology types — stateful stream processing with windows, joins, checkpointing
// ---------------------------------------------------------------------------

import type {
  ChannelName,
  ConsumerGroup,
  StageId,
  TopologyId,
  TopologyInstanceId,
} from "@spilne/perfect-core/connect";
import type { ExitT } from "@spilne/perfect-core";
import type { StateBackend } from "./state-backend";
import type { PartitionedStateBackend } from "@spilne/perfect-core/connect";

export interface TimeWindow {
  start: number;
  end: number;
}

export type WindowType =
  | { type: "tumbling"; windowMs: number }
  | { type: "sliding"; windowMs: number; slideMs: number }
  | { type: "session"; gapMs: number };

export interface AggregateSpec<S, T, U> {
  init: () => S;
  add: (state: S, value: T) => S;
  emit: (key: string, window: TimeWindow, state: S) => U;
}

export interface ProcessSpec<S, T, U> {
  init: () => S;
  process: (state: S, value: T) => { state: S; emit?: U };
}

export interface JoinConfig {
  windowMs: number;
}

export interface TopologyConfig {
  group: ConsumerGroup;
  /** Requires source, sink, and partition state to share one transaction domain. */
  deliveryGuarantee?: "at-least-once" | "exactly-once";
  /** Stable identity used to namespace durable state. Defaults to `group`. */
  topologyId?: TopologyId;
  /** Stable stage identity. DistributedRunner supplies this automatically. */
  stageId?: StageId;
  /** Unique process identity used to own fenced partition leases. */
  instanceId?: TopologyInstanceId;
  stateBackend?: StateBackend<string, unknown>;
  /** Atomic, partition-scoped state. Required for hardened distributed state. */
  partitionedStateBackend?: PartitionedStateBackend<unknown>;
  /** Lease duration for partition ownership. Default: 30 seconds. */
  partitionLeaseMs?: number;
  checkpointIntervalMs?: number;
  /** Max items buffered between stages before backpressure kicks in. Default: unbounded. */
  maxBufferSize?: number;
  /** Max items emitted per second across the topology. Default: unlimited. */
  maxItemsPerSecond?: number;
  /** Max entries in the dedup seen-set before oldest are evicted. Default: 100_000. */
  maxDedupeSize?: number;
  /** Called when backpressure is applied (buffer full). */
  onBackpressure?: (stats: BackpressureStats) => void;
  /** Ack every N items instead of per-item. Default: 100. Set to 1 for per-item ack. */
  ackBatchSize?: number;
  /** Flush a partial ack batch after this many milliseconds. Default: 1_000. */
  ackMaxWaitMs?: number;
}

export interface BackpressureStats {
  /** Current buffer fill level (0-1). */
  fillRatio: number;
  /** Number of items in the buffer. */
  bufferedItems: number;
  /** Max buffer capacity. */
  maxBuffer: number;
  /** Timestamp of the event. */
  timestamp: number;
}

export interface TopologyHandle {
  shutdown(): Promise<void>;
  /** Wait for every branch and inspect typed failures, defects, or interruption. */
  awaitExit(): Promise<readonly ExitT<unknown, void>[]>;
  isRunning(): boolean;
  /** Get current topology metrics. */
  metrics(): TopologyMetrics;
}

export interface TopologyMetrics {
  /** Total items processed since start. */
  itemsProcessed: number;
  /** Items processed per second (rolling average). */
  itemsPerSecond: number;
  /** Current buffer fill levels by operator. */
  bufferStats: { operator: string; buffered: number; capacity: number }[];
  /** Number of keys in dedup set. */
  dedupeSize: number;
  /** Number of active windows. */
  activeWindows: number;
  /** Number of buffered join items (left + right). */
  joinBufferSize: number;
}

// ---------------------------------------------------------------------------
// Topology plan nodes — the logical plan for the processing DAG
// ---------------------------------------------------------------------------

export type TopologyNode<_T = unknown> =
  | SourceNode<unknown>
  | MapNode<unknown>
  | FilterNode<any>
  | MapAsyncNode<any>
  | KeyByNode<any>
  | ShuffleNode<any>
  | WindowNode<any>
  | AggregateNode<any>
  | ProcessNode<any>
  | DedupeNode<any>
  | JoinNode<any>
  | SinkNode<any>;

export interface SourceNode<_T> {
  type: "source";
  source: unknown; // Streamable & Acknowledgeable
}

export interface MapNode<T> {
  type: "map";
  parent: TopologyNode;
  fn: (value: unknown) => T;
}

export interface FilterNode<T> {
  type: "filter";
  parent: TopologyNode;
  fn: (value: T) => boolean;
}

export interface MapAsyncNode<T> {
  type: "mapAsync";
  parent: TopologyNode;
  concurrency: number;
  fn: (value: unknown) => Promise<T>;
}

export interface KeyByNode<T> {
  type: "keyBy";
  parent: TopologyNode;
  keyFn: (value: T) => string;
}

export interface ShuffleNode<_T> {
  type: "shuffle";
  parent: TopologyNode;
  /** Optional explicit repartition channel name. Auto-generated if omitted. */
  topicName?: ChannelName;
}

export interface WindowNode<_T> {
  type: "window";
  parent: TopologyNode;
  windowType: WindowType;
}

export interface AggregateNode<T> {
  type: "aggregate";
  parent: TopologyNode;
  spec: AggregateSpec<unknown, unknown, T>;
}

export interface ProcessNode<T> {
  type: "process";
  parent: TopologyNode;
  spec: ProcessSpec<unknown, unknown, T>;
}

export interface DedupeNode<T> {
  type: "dedupe";
  parent: TopologyNode;
  keyFn: (value: T) => string;
}

export interface JoinNode<_T> {
  type: "join";
  left: TopologyNode;
  right: TopologyNode;
  config: JoinConfig;
}

export interface SinkNode<_T> {
  type: "sink";
  parent: TopologyNode;
  sink: unknown; // Sinkable
}

export interface CompiledTopology {
  nodes: TopologyNode[];
  sinks: SinkNode<unknown>[];
}
