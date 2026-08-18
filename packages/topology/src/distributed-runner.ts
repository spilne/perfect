// ---------------------------------------------------------------------------
// DistributedRunner — multi-stage topology execution with shuffle
//
// Splits a topology at shuffle boundaries into stages, connects stages
// through repartition channels provided by ShuffleTransport, then runs
// each stage via TopologyRunner.
//
// If the topology has no shuffle nodes, delegates directly to TopologyRunner.
//
// The ShuffleTransport interface lives in @perfect/core/connect (kafka
// implements it, topology consumes it); DistributedTopologyConfig is local.
// ---------------------------------------------------------------------------

import { StreamTopology, BuiltTopology } from "./stream-topology";
import { TopologyRunner } from "./topology-runner";
import { planStages } from "./stage-planner";
import type { TopologyHandle, TopologyMetrics, TopologyConfig } from "./types";
import type { StateBackend } from "./state-backend";
import type {
  Streamable,
  Acknowledgeable,
  KeyedSinkable,
  ShuffleTransport,
  ChannelName,
  PartitionedStateBackend,
  TopologyId,
  TopologyInstanceId,
} from "@perfect/core/connect";
import { JsonCodec, ConsumerGroup, TopologyId as makeTopologyId } from "@perfect/core/connect";
import type { Eff } from "@perfect/core";

/**
 * Config for DistributedRunner — extends TopologyConfig with shuffle transport.
 */
export interface DistributedTopologyConfig {
  group: ConsumerGroup;
  deliveryGuarantee?: "at-least-once" | "exactly-once";
  shuffleTransport: ShuffleTransport;
  stateBackend?: StateBackend<string, unknown>;
  partitionedStateBackend?: PartitionedStateBackend<unknown>;
  topologyId?: TopologyId;
  instanceId?: TopologyInstanceId;
  partitionLeaseMs?: number;
  checkpointIntervalMs?: number;
  maxBufferSize?: number;
  maxItemsPerSecond?: number;
  maxDedupeSize?: number;
  ackBatchSize?: number;
  ackMaxWaitMs?: number;
}

/**
 * Run a topology with distributed shuffle support.
 *
 * If the topology contains `.shuffle()` nodes, it is split into stages
 * connected through repartition channels (e.g. Kafka topics). Each stage
 * runs independently via TopologyRunner. Multiple instances with the same
 * group ID share partitions via consumer groups.
 *
 * If there are no shuffle nodes, delegates directly to TopologyRunner.
 *
 * @example
 * ```ts
 * const topology = StreamTopology.source(events)
 *   .keyBy(e => e.userId)
 *   .shuffle()
 *   .tumbling(60_000)
 *   .count()
 *   .to(output);
 *
 * const handle = await DistributedRunner.run(topology, {
 *   group: "counter",
 *   shuffleTransport: new KafkaShuffleTransport({ kafka }),
 * });
 * ```
 */
export class DistributedRunner {
  static async run(
    topology: BuiltTopology,
    config: DistributedTopologyConfig,
  ): Promise<TopologyHandle> {
    const plan = planStages({
      compiled: topology.compiled,
      group: config.group,
    });

    const hasDistributedState = plan.stages.some((stage) =>
      stage.nodes.some((node) => ["aggregate", "process", "dedupe", "join"].includes(node.type)),
    );
    if (
      config.stateBackend &&
      !config.partitionedStateBackend &&
      plan.stages.length > 1 &&
      hasDistributedState
    ) {
      throw new TypeError(
        "DistributedRunner requires partitionedStateBackend for stateful multi-stage execution",
      );
    }

    // No shuffles — delegate to TopologyRunner
    if (plan.stages.length === 1 && plan.repartitionTopics.length === 0) {
      return TopologyRunner.run(topology, config as TopologyConfig);
    }

    // Create repartition channels
    const channels = new Map<
      ChannelName,
      { source: Streamable<unknown> & Acknowledgeable<unknown>; sink: KeyedSinkable<unknown> }
    >();

    for (const topicName of plan.repartitionTopics) {
      const channel = await config.shuffleTransport.getOrCreateRepartitionChannel({
        name: topicName,
        group: config.group,
        codec: JsonCodec,
      });
      channels.set(topicName, channel);
    }

    // Run each stage
    const handles: TopologyHandle[] = [];

    for (const stage of plan.stages) {
      // Build stage source
      let stageSource: (Streamable<unknown> & Acknowledgeable<unknown>) | undefined;
      if (stage.source === "original") {
        // Use the original source from the topology
        stageSource = undefined; // TopologyRunner will use the source node
      } else {
        stageSource = channels.get(stage.source.repartitionTopic)!.source;
      }

      // Build stage sink (repartition publish)
      let stageSink: KeyedSinkable<unknown> | undefined;
      if (stage.sink !== "terminal") {
        stageSink = channels.get(stage.sink.repartitionTopic)!.sink;
      }

      // Build sub-topology for this stage
      const stageTopology = buildStageTopology({
        stage,
        stageSource,
        stageSink,
        originalTopology: topology,
      });

      const handle = await TopologyRunner.run(stageTopology, {
        // Each stage is its own consumer group — derived, so rebrand.
        group: ConsumerGroup(`${config.group}-${stage.id}`),
        deliveryGuarantee: config.deliveryGuarantee,
        topologyId: config.topologyId ?? makeTopologyId(config.group),
        stageId: stage.id,
        instanceId: config.instanceId,
        stateBackend: config.stateBackend,
        partitionedStateBackend: config.partitionedStateBackend,
        partitionLeaseMs: config.partitionLeaseMs,
        checkpointIntervalMs: config.checkpointIntervalMs,
        maxBufferSize: config.maxBufferSize,
        maxItemsPerSecond: config.maxItemsPerSecond,
        maxDedupeSize: config.maxDedupeSize,
        ackBatchSize: config.ackBatchSize,
        ackMaxWaitMs: config.ackMaxWaitMs,
      });

      handles.push(handle);
    }

    // Return composite handle
    return {
      async shutdown() {
        await Promise.all(handles.map((h) => h.shutdown()));
      },
      async awaitExit() {
        return (await Promise.all(handles.map((handle) => handle.awaitExit()))).flat();
      },
      isRunning() {
        return handles.some((h) => h.isRunning());
      },
      metrics(): TopologyMetrics {
        const allMetrics = handles.map((h) => h.metrics());
        return {
          itemsProcessed: allMetrics.reduce((s, m) => s + m.itemsProcessed, 0),
          itemsPerSecond: allMetrics.reduce((s, m) => s + m.itemsPerSecond, 0),
          bufferStats: allMetrics.flatMap((m) => m.bufferStats),
          dedupeSize: allMetrics.reduce((s, m) => s + m.dedupeSize, 0),
          activeWindows: allMetrics.reduce((s, m) => s + m.activeWindows, 0),
          joinBufferSize: allMetrics.reduce((s, m) => s + m.joinBufferSize, 0),
        };
      },
    };
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Build a BuiltTopology for a single stage.
 *
 * - If the stage reads from a repartition topic, splices in a new source node.
 * - If the stage writes to a repartition topic, splices in a sink that publishes with key.
 * - If the stage is the original (no shuffle), returns the original topology.
 */
function buildStageTopology(params: {
  stage: ReturnType<typeof planStages>["stages"][number];
  stageSource?: Streamable<unknown> & Acknowledgeable<unknown>;
  stageSink?: KeyedSinkable<unknown>;
  originalTopology: BuiltTopology;
}): BuiltTopology {
  const { stage, stageSource, stageSink, originalTopology } = params;

  // If this is stage 0 with original source and writing to repartition
  if (stage.source === "original" && stageSink) {
    // Build a topology: original source → pre-shuffle nodes → keyed publish
    const keyFn = stage.keyFn;
    const sinkable = {
      publish: (value: unknown) => {
        const key = keyFn ? keyFn(value) : undefined;
        return stageSink.publish(value, key ? { key } : undefined);
      },
      codec: JsonCodec,
    };

    // Find the original source node from the compiled topology
    const sourceNode = findSourceNode(originalTopology.compiled.nodes);
    if (!sourceNode) throw new Error("No source node found in topology");

    // Rebuild from original source to the keyBy, then sink to repartition
    const topo = StreamTopology.source(sourceNode.source as any);

    // Apply pre-shuffle transforms by walking stage.nodes
    return buildFromNodes(topo, stage.nodes, sinkable);
  }

  // If this stage reads from repartition and is the final stage
  if (stageSource && stage.sink === "terminal") {
    // Build topology: repartition source → post-shuffle nodes → original sink(s)
    const topo = StreamTopology.source(stageSource);
    return buildFromNodes(topo, stage.nodes, undefined);
  }

  // If this stage reads from repartition and writes to repartition (middle stage)
  if (stageSource && stageSink) {
    const keyFn = stage.keyFn;
    const sinkable = {
      publish: (value: unknown) => {
        const key = keyFn ? keyFn(value) : undefined;
        return stageSink.publish(value, key ? { key } : undefined);
      },
      codec: JsonCodec,
    };
    const topo = StreamTopology.source(stageSource);
    return buildFromNodes(topo, stage.nodes, sinkable);
  }

  // Fallback: return original topology
  return originalTopology;
}

/** Find the source node in a list of topology nodes. */
function findSourceNode(nodes: readonly any[]): { source: unknown } | undefined {
  for (const node of nodes) {
    if (node.type === "source") return node;
  }
  // Also walk parent chains
  for (const node of nodes) {
    let current = node;
    while (current) {
      if (current.type === "source") return current;
      current = "parent" in current ? current.parent : undefined;
    }
  }
  return undefined;
}

/**
 * Build a BuiltTopology by replaying node types onto a StreamTopology.
 * Handles the common linear topology case.
 */
function buildFromNodes(
  base: StreamTopology<unknown>,
  nodes: readonly any[],
  sink?: { publish: (value: unknown) => Eff<void, any>; codec: any },
): BuiltTopology {
  let topo: any = base;

  for (const node of nodes) {
    switch (node.type) {
      case "map":
        topo = topo.map(node.fn);
        break;
      case "filter":
        topo = topo.filter(node.fn);
        break;
      case "mapAsync":
        topo = topo.mapAsync(node.concurrency, node.fn);
        break;
      case "keyBy":
        topo = topo.keyBy(node.keyFn);
        break;
      case "window":
        if (node.windowType.type === "tumbling") topo = topo.tumbling(node.windowType.windowMs);
        else if (node.windowType.type === "sliding")
          topo = topo.sliding({
            windowMs: node.windowType.windowMs,
            slideMs: node.windowType.slideMs,
          });
        else if (node.windowType.type === "session") topo = topo.session(node.windowType.gapMs);
        break;
      case "aggregate":
        topo = topo.aggregate(node.spec);
        break;
      case "process":
        topo = topo.process(node.spec);
        break;
      case "dedupe":
        topo = topo.dedupe(node.keyFn);
        break;
      case "sink":
        return topo.to(node.sink);
      case "source":
      case "shuffle":
        // Skip — already handled
        break;
    }
  }

  if (sink) {
    return topo.to(sink);
  }

  return topo.build();
}
