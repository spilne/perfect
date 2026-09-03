// ---------------------------------------------------------------------------
// Kafka adapter conformance — one suite, every adapter.
//
// The cases live in `src/adapter-suite.ts` and are runtime-agnostic. This file
// is only the Bun binding, and it runs them two ways because the drivers do
// not share a runtime:
//
//   kafkajs       in-process     — loads and runs under Bun
//   platformatic  subprocess     — consumer unsupported on Bun, so the same
//                                  suite is bundled for Node and executed
//                                  under the package-local Node runtime, with
//                                  its per-case results projected back into
//                                  individual `it()`s
//
// Cases needing an optional port member an adapter does not implement are
// skipped by capability rather than failed (see ADAPTER_PROFILES).
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, setDefaultTimeout } from "bun:test";
import { rm } from "node:fs/promises";
import { join } from "node:path";

setDefaultTimeout(300_000);

import { TopicName } from "@spilne/perfect-kafka";
import { createKafkajsClient, createKafkajsTopic } from "@spilne/perfect-kafka-kafkajs";
import { withApacheKafka, withKafka } from "../src/infra";
import {
  ADAPTER_PROFILES,
  adapterSuite,
  applicable,
  type AdapterCtx,
  type CaseResult,
} from "../src/adapter-suite";

const SUBPROCESS_TIMEOUT = 240_000;

// ---------------------------------------------------------------------------
// Binding 1 — driver runs inside Bun, cases run in-process
// ---------------------------------------------------------------------------

function inProcessSuite(adapter: string, makeCtx: () => AdapterCtx) {
  const profile = ADAPTER_PROFILES[adapter]!;
  describe(`${adapter} — in-process (Bun)`, () => {
    for (const testCase of adapterSuite) {
      const skip = !applicable(testCase, profile.capabilities);
      it.skipIf(skip)(testCase.name, async () => {
        await testCase.run(makeCtx());
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Binding 2 — driver cannot load in Bun, so run the suite under Node and
// project the reported results back as individual assertions
// ---------------------------------------------------------------------------

function subprocessSuite(adapter: string, getBroker: () => string) {
  const profile = ADAPTER_PROFILES[adapter]!;
  describe(`${adapter} — subprocess (Node)`, () => {
    const results = new Map<string, CaseResult>();
    let bootFailure: Error | undefined;

    beforeAll(async () => {
      const bundleDirectory = join(import.meta.dir, `../.suite-${adapter}-${crypto.randomUUID()}`);
      const bundle = join(bundleDirectory, "suite.mjs");
      try {
        // Bundled by the `bun build` CLI rather than the Bun.build API:
        // inside `bun test` the API's resolver does not see the workspace
        // symlinks and fails with `Could not resolve: "@spilne/perfect-kafka"`,
        // while the same build from a normal Bun process succeeds.
        const build = Bun.spawn({
          cmd: [
            "bun",
            "build",
            join(import.meta.dir, "../src/run-suite-node.ts"),
            "--target=node",
            `--outfile=${bundle}`,
            // Left unbundled so Node resolves them from packages/integration.
            "--external=@platformatic/kafka",
            "--external=kafkajs",
          ],
          stdout: "pipe",
          stderr: "pipe",
        });
        const [buildExit, buildErr] = await Promise.all([
          build.exited,
          new Response(build.stderr).text(),
        ]);
        if (buildExit !== 0) throw new Error(buildErr || `bun build exited ${buildExit}`);

        const node = join(import.meta.dir, "../node_modules/.bin/node");
        const child = Bun.spawn({
          cmd: [node, bundle, getBroker(), adapter],
          stdout: "pipe",
          stderr: "pipe",
        });
        const timeout = setTimeout(() => child.kill(9), SUBPROCESS_TIMEOUT);
        const [exitCode, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]).finally(() => clearTimeout(timeout));

        const marker = stdout.split("\n").find((line) => line.startsWith("PERFECT_SUITE_RESULT="));
        if (!marker) {
          throw new Error(
            `suite runner produced no results (exit ${exitCode})\n${stderr || stdout}`,
          );
        }
        for (const result of JSON.parse(marker.slice("PERFECT_SUITE_RESULT=".length))) {
          results.set(result.name, result);
        }
      } catch (error) {
        // Bun.build throws an AggregateError whose `message` is just
        // "Bundle failed" — the actionable detail is in `.errors`.
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        bootFailure = new Error(`suite bootstrap failed: ${detail}`);
      } finally {
        await rm(bundleDirectory, { recursive: true, force: true });
      }
    }, SUBPROCESS_TIMEOUT);

    for (const testCase of adapterSuite) {
      const skip = !applicable(testCase, profile.capabilities);
      it.skipIf(skip)(testCase.name, () => {
        if (bootFailure) throw bootFailure;
        const result = results.get(testCase.name);
        expect(result, `runner reported no result for "${testCase.name}"`).toBeDefined();
        if (result!.status === "fail") throw new Error(result!.reason);
        expect(result!.status).toBe("pass");
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

const kafkajsCtx = (getBroker: () => string): AdapterCtx => ({
  broker: getBroker(),
  makeClient: (broker: string) => createKafkajsClient(broker),
  createTopic: (topic: TopicName, partitions?: number) =>
    createKafkajsTopic(getBroker(), topic, partitions),
  capabilities: ADAPTER_PROFILES.kafkajs!.capabilities,
});

// Redpanda — fast (no JVM), and enough for KafkaJS.
withKafka("Kafka adapter conformance (Redpanda)", (ctx) => {
  inProcessSuite("kafkajs", () => kafkajsCtx(() => ctx.broker));
});

// @platformatic/kafka negotiates a Metadata API version Redpanda does not
// advertise (PLT_KFK_UNSUPPORTED_API), so its lane needs real Apache Kafka.
// The JVM image is slow to start: opt in with KAFKA_FULL=1.
if (process.env.KAFKA_FULL === "1") {
  withApacheKafka("Kafka adapter conformance (Apache Kafka)", (ctx) => {
    // Same suite, same broker — the control for the platformatic run below.
    inProcessSuite("kafkajs", () => kafkajsCtx(() => ctx.broker));
    subprocessSuite("platformatic", () => ctx.broker);
  });
}
