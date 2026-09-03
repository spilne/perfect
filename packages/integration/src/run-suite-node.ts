// ---------------------------------------------------------------------------
// Node.js binding for the Kafka adapter conformance suite.
//
// Bundled by the Bun test with `target: "node"` and executed under the
// package-local Node runtime, because two of the three drivers cannot run on
// Bun at all: @platformatic/kafka's consumer is unsupported there, and any
// librdkafka-based driver is a NAN addon compiled against V8's C++ ABI, which
// JavaScriptCore cannot load.
//
// Runs the identical `adapterSuite` the Bun binding runs and reports results
// on stdout as a single PERFECT_SUITE_RESULT= line, so the parent test can
// re-project them as individual assertions.
//
//   node run-suite-node.mjs <broker> <adapter>
// ---------------------------------------------------------------------------

import { ADAPTER_PROFILES, runSuite, type AdapterCtx, type CaseResult } from "./adapter-suite";
import { createKafkajsClient, createKafkajsTopic } from "@spilne/perfect-kafka-kafkajs";
import { createPlatformaticClient } from "@spilne/perfect-kafka-platformatic";
import type { KafkaClient, TopicName } from "@spilne/perfect-kafka";

const CLIENTS: Record<string, (broker: string) => KafkaClient> = {
  kafkajs: (broker) => createKafkajsClient(broker),
  platformatic: (broker) =>
    createPlatformaticClient({
      bootstrapBrokers: [broker],
      consumerOptions: { groupProtocol: "consumer" },
    }),
};

const broker = process.argv[2];
const adapterName = process.argv[3];
if (!broker || !adapterName) throw new Error("usage: run-suite-node <broker> <adapter>");

const makeClient = CLIENTS[adapterName];
const profile = ADAPTER_PROFILES[adapterName];
if (!makeClient || !profile) {
  throw new Error(`unknown adapter '${adapterName}' (have: ${Object.keys(CLIENTS).join(", ")})`);
}

const ctx: AdapterCtx = {
  broker,
  makeClient,
  // KafkaJS bootstraps topics for every adapter — see AdapterCtx.createTopic.
  createTopic: (topic: TopicName, partitions?: number) =>
    createKafkajsTopic(broker, topic, partitions),
  capabilities: profile.capabilities,
};

const results: CaseResult[] = await runSuite(ctx);
console.log(`PERFECT_SUITE_RESULT=${JSON.stringify(results)}`);
process.exit(0);
