// ---------------------------------------------------------------------------
// TopologyAnalyzer — static analysis of topology DAGs
//
// Detects potential issues like missing shuffles before stateful operators.
// ---------------------------------------------------------------------------

import type { TopologyNode, CompiledTopology } from "./types.js";

export interface TopologyWarning {
  type: "missing-shuffle";
  message: string;
  /** The node that may need a shuffle before it. */
  node: TopologyNode;
}

/** Stateful node types that require same-key-same-partition for correctness. */
const STATEFUL_TYPES = new Set(["aggregate", "process", "dedupe", "join"]);

const advice =
  "In distributed mode, events for the same key may arrive at different instances. Add .shuffle() after keyBy for correct multi-instance behavior.";

/**
 * Analyze a topology for potential issues.
 *
 * Currently detects:
 * - **missing-shuffle**: A stateful operator (aggregate, process, dedupe,
 *   join) follows a keyBy without a shuffle. In distributed mode, this means
 *   events for the same key could arrive at different instances. A join
 *   needs a shuffle on both of its inputs.
 */
export function analyze(compiled: CompiledTopology): TopologyWarning[] {
  const warnings: TopologyWarning[] = [];
  const visited = new Set<TopologyNode>();

  function walk(node: TopologyNode) {
    if (visited.has(node)) return;
    visited.add(node);

    if (node.type === "join") {
      // Each side reaches the join through its own chain of steps.
      for (const [side, input] of [
        ["left", node.left],
        ["right", node.right],
      ] as const) {
        if (keyedWithoutShuffle(input)) {
          warnings.push({
            type: "missing-shuffle",
            message: `the ${side} side of "join" follows keyBy without .shuffle(). ${advice}`,
            node,
          });
        }
      }
      walk(node.left);
      walk(node.right);
      return;
    }

    if (STATEFUL_TYPES.has(node.type) && "parent" in node && keyedWithoutShuffle(node.parent)) {
      warnings.push({
        type: "missing-shuffle",
        message: `"${node.type}" follows keyBy without .shuffle(). ${advice}`,
        node,
      });
    }
    if ("parent" in node) walk(node.parent);
  }

  for (const sink of compiled.sinks) walk(sink);
  for (const node of compiled.nodes) walk(node);
  return warnings;
}

/**
 * Whether the records coming out of `node` were keyed with keyBy and not
 * shuffled since. The search stops at a join: if one of its sides misses a
 * shuffle, the join gets the warning, and the steps after it would only
 * repeat it.
 */
function keyedWithoutShuffle(node: TopologyNode): boolean {
  if (node.type === "shuffle" || node.type === "join") return false;
  if (node.type === "keyBy") return true;
  return "parent" in node ? keyedWithoutShuffle(node.parent) : false;
}
