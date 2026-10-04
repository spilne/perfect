// ---------------------------------------------------------------------------
// DistributedRunner — multi-stage topology execution with shuffle
//
// Splits a topology at shuffle boundaries into stages, connects stages
// through repartition channels provided by ShuffleTransport, then runs
// each stage via TopologyRunner.
//
// If the topology has no shuffle nodes, delegates directly to TopologyRunner.
//
// The ShuffleTransport interface lives in @spilne/perfect-core/connect (kafka
// implements it, topology consumes it); DistributedTopologyConfig is local.
// ---------------------------------------------------------------------------

import { BuiltTopology } from "./stream-topology.js";
import { TopologyRunner } from "./topology-runner.js";
import { planStages, type TopologyStage } from "./stage-planner.js";
import type {
  SinkNode,
  TopologyConfig,
  TopologyHandle,
  TopologyMetrics,
  TopologyNode,
} from "./types.js";
import type {
  Streamable,
  Acknowledgeable,
  KeyedSinkable,
  ShuffleTransport,
  ChannelName,
} from "@spilne/perfect-core/connect";
import {
  JsonCodec,
  ConsumerGroup,
  TopologyId as makeTopologyId,
} from "@spilne/perfect-core/connect";

/**
 * Config for DistributedRunner: everything TopologyRunner takes, plus the
 * transport that carries records between stages.
 */
export interface DistributedTopologyConfig extends TopologyConfig {
  shuffleTransport: ShuffleTransport<unknown, unknown>;
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
 *   group: ConsumerGroup("counter"),
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
      return TopologyRunner.run(topology, config);
    }

    // Stages are planned along one chain of steps, and a join has two
    // inputs, so a join can't be split into stages yet.
    if (hasJoin(topology.compiled)) {
      throw new TypeError(
        "DistributedRunner can't run a join in a topology with shuffle() yet; " +
          "run it with TopologyRunner instead",
      );
    }

    // Create repartition channels
    const channels = new Map<
      ChannelName,
      {
        source: Streamable<unknown, unknown> & Acknowledgeable<unknown, unknown>;
        sink: KeyedSinkable<unknown, unknown>;
      }
    >();

    for (const topicName of plan.repartitionTopics) {
      const channel = await config.shuffleTransport.getOrCreateRepartitionChannel({
        name: topicName,
        group: config.group,
        codec: JsonCodec,
      });
      channels.set(topicName, channel);
    }

    // Only the value goes through a repartition channel, not the event time
    // an eventTime() step set. Windows after the shuffle would quietly fall
    // back to reading the time from the value, so ask for the step there.
    let eventTimeUpstream = false;
    for (const stage of plan.stages) {
      const setsTime = stage.nodes.some((node) => node.type === "eventTime");
      const usesTime = stage.nodes.some((node) => node.type === "window");
      if (eventTimeUpstream && usesTime && !setsTime) {
        throw new TypeError(
          "eventTime() doesn't carry across shuffle(); call it again after shuffle(), " +
            "before the window",
        );
      }
      eventTimeUpstream ||= setsTime;
    }

    // The key each repartition channel is written with. The stage reading
    // that channel needs it again: keyed steps there (windows, process,
    // dedupe) group by the key that was set before the shuffle.
    const channelKeys = new Map<ChannelName, (value: unknown) => string>();
    for (const stage of plan.stages) {
      if (stage.sink !== "terminal" && stage.keyFn) {
        channelKeys.set(stage.sink.repartitionTopic, stage.keyFn);
      }
    }

    // Start every stage. If one fails to start, stop the ones already
    // running instead of leaving them behind.
    const handles: TopologyHandle[] = [];
    try {
      for (const stage of plan.stages) {
        const stageTopology = buildStageTopology({
          stage,
          channels,
          channelKeys,
        });
        handles.push(
          await TopologyRunner.run(stageTopology, {
            ...config,
            // Each stage is its own consumer group — derived, so rebrand.
            group: ConsumerGroup(`${config.group}-${stage.id}`),
            topologyId: config.topologyId ?? makeTopologyId(config.group),
            stageId: stage.id,
          }),
        );
      }
    } catch (error) {
      await Promise.allSettled(handles.map((handle) => handle.shutdown()));
      throw error;
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
          lateRecords: allMetrics.reduce((s, m) => s + m.lateRecords, 0),
        };
      },
    };
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Channel = {
  source: Streamable<unknown, unknown> & Acknowledgeable<unknown, unknown>;
  sink: KeyedSinkable<unknown, unknown>;
};

/**
 * Build the topology one stage runs.
 *
 * A stage's nodes are copied and linked onto the stage's own start: the
 * original source for the first stage, otherwise the repartition channel it
 * reads, followed by the key from before the shuffle. Copying the nodes (not
 * replaying them through the builder) keeps every kind of step working.
 * A stage that writes to a repartition channel ends in a sink that publishes
 * each value with its key.
 */
function buildStageTopology(params: {
  stage: TopologyStage;
  channels: ReadonlyMap<ChannelName, Channel>;
  channelKeys: ReadonlyMap<ChannelName, (value: unknown) => string>;
}): BuiltTopology {
  const { stage, channels, channelKeys } = params;
  let last: TopologyNode;
  if (stage.source === "original") {
    const source = stage.nodes.find((node) => node.type === "source");
    if (!source) throw new Error("No source node found in topology");
    last = source;
  } else {
    const topic = stage.source.repartitionTopic;
    last = { type: "source", source: channels.get(topic)!.source };
    const keyFn = channelKeys.get(topic);
    if (keyFn) last = { type: "keyBy", parent: last, keyFn };
  }

  for (const node of stage.nodes) {
    if (node.type === "source" || node.type === "shuffle" || node.type === "join") continue;
    if (node.type === "sink") continue;
    last = { ...node, parent: last };
  }

  const sinks: SinkNode<unknown>[] = [];
  if (stage.sink === "terminal") {
    for (const sink of stage.sinkNodes) sinks.push({ ...sink, parent: last });
  } else {
    const channelSink = channels.get(stage.sink.repartitionTopic)!.sink;
    const keyFn = stage.keyFn;
    sinks.push({
      type: "sink",
      parent: last,
      sink: {
        codec: JsonCodec,
        publish: (value: unknown) =>
          channelSink.publish(value, keyFn ? { key: keyFn(value) } : undefined),
      },
    });
  }
  return new BuiltTopology({ nodes: [last], sinks });
}

function hasJoin(compiled: BuiltTopology["compiled"]): boolean {
  const seen = new Set<TopologyNode>();
  const visit = (node: TopologyNode): boolean => {
    if (seen.has(node)) return false;
    seen.add(node);
    if (node.type === "join") return true;
    return "parent" in node && visit(node.parent);
  };
  return [...compiled.sinks, ...compiled.nodes].some(visit);
}
