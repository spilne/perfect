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
import type { StateBackend } from "./state-backend.js";
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
  /**
   * Combine two partial results. Session windows need it when a record
   * arrives between two sessions and joins them into one; without it the
   * sessions stay separate.
   */
  merge?: (a: S, b: S) => S;
}

/** Options shared by the window steps. */
export interface WindowOptions extends StepOptions {
  /**
   * How long to wait for records that arrive out of order. A window closes
   * once the partition has seen a record this much past the window's end.
   * Records for windows that already closed are dropped (and counted in
   * `metrics().lateRecords`). Default: 0.
   */
  allowedLatenessMs?: number;
}

export interface ProcessSpec<S, T, U> {
  init: () => S;
  process: (state: S, value: T) => { state: S; emit?: U };
}

export interface JoinConfig {
  windowMs: number;
  /**
   * A stable name for this step's saved state. Without it the state is
   * saved under the step's position (e.g. "process:0"), so adding a step of
   * the same kind earlier in the topology hands this step's state to the
   * other one. Letters, digits, "_", "-" and "." only.
   */
  name?: string;
}

/** Options for a stateful step. */
export interface StepOptions {
  /**
   * A stable name for this step's saved state. Without it the state is
   * saved under the step's position (e.g. "process:0"), so adding a step of
   * the same kind earlier in the topology hands this step's state to the
   * other one. Letters, digits, "_", "-" and "." only.
   */
  name?: string;
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
  /**
   * How long (ms) to remember which source records were already processed,
   * to drop redelivered duplicates. Applies when the runner keeps state
   * itself: the default in-memory state, or a plain `stateBackend`. (A
   * `partitionedStateBackend` takes its own `processedRetentionMs`.)
   * Default: forever, which grows without bound on a long-running topology.
   */
  processedRetentionMs?: number;
  /**
   * Called with warnings found when the topology starts, such as stateful
   * steps without a `name` while state is saved durably. Default:
   * console.warn.
   */
  onWarning?: (message: string) => void;
  /** @deprecated Not called yet; it has no effect. */
  onBackpressure?: (stats: BackpressureStats) => void;
  /**
   * Commit and ack up to this many records of a partition together: one
   * state commit (one round trip to Redis or Postgres) for the whole batch,
   * then the acks. Default: 100. Acks wait until their batch is committed
   * (at most ackMaxWaitMs); set 1 to commit and ack every record on its own.
   * Not used with exactly-once delivery, which commits each record in its own
   * transaction.
   */
  ackBatchSize?: number;
  /** Commit a batch that isn't full after this many ms. Default: 1_000. */
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
  /**
   * Source records finished since start, including ones that were filtered
   * out or skipped as duplicates.
   */
  itemsProcessed: number;
  /** itemsProcessed divided by the seconds since start (not a recent rate). */
  itemsPerSecond: number;
  /** @deprecated Always empty for now. */
  bufferStats: { operator: string; buffered: number; capacity: number }[];
  /** Number of keys in dedup set. */
  dedupeSize: number;
  /** Number of active windows. */
  activeWindows: number;
  /** Number of buffered join items (left + right). */
  joinBufferSize: number;
  /** Records dropped because the windows they belong to had already closed. */
  lateRecords: number;
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
  | EventTimeNode<any>
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

export interface EventTimeNode<T> {
  type: "eventTime";
  parent: TopologyNode;
  fn: (value: T) => number;
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
  allowedLatenessMs?: number;
  name?: string;
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
  name?: string;
}

export interface DedupeNode<T> {
  type: "dedupe";
  parent: TopologyNode;
  keyFn: (value: T) => string;
  name?: string;
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
