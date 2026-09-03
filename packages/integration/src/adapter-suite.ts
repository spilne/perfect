// ---------------------------------------------------------------------------
// Kafka adapter conformance suite — runtime- and framework-agnostic.
//
// Every case exercises the @spilne/perfect-kafka port (KafkaClient / KafkaConsumer /
// KafkaProducer / KafkaAdmin) through KafkaTopic, so a passing run means the
// adapter is substitutable in application code, not merely that its own
// methods resolve.
//
// Deliberately free of `bun:test`: the drivers do not share a runtime.
// KafkaJS runs under Bun, @platformatic/kafka needs Node, and any
// librdkafka-based driver is Node-only (NAN addons cannot load on Bun's
// JavaScriptCore). A shared suite must therefore be plain functions plus
// node:assert, with the runner supplied per binding — see
// `test/kafka-conformance.test.ts` (Bun, in-process) and
// `src/run-suite-node.ts` (Node, subprocess).
//
// Optional port members (`run`, `stream`, `seek`, `onPartitionsAssigned`,
// `createTopics`, `fetchTopicPartitionCount`) are gated by `requires` so an
// adapter that omits one is reported as skipped rather than failed.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { die, run, sync, type Throws } from "@spilne/perfect-core";
import {
  autoCommitBatchWithin,
  ChannelName,
  ConsumerGroup,
  JsonCodec,
  type Codec,
} from "@spilne/perfect-core/connect";
import {
  KafkaTopic,
  KafkaShuffleTransport,
  commitBatchWithin,
  TopicName,
  GroupId,
  KafkaOffset,
  type KafkaError,
  type KafkaClient,
} from "@spilne/perfect-kafka";

// ---------------------------------------------------------------------------
// Capabilities — optional members of the port
// ---------------------------------------------------------------------------

export interface AdapterCapabilities {
  /** `consumer.run({ eachBatch })` — required by `batchEmit: true`. */
  readonly eachBatch: boolean;
  /** `consumer.seek()` — required by `{ type: "specific" }` offsets. */
  readonly seek: boolean;
  /** `onPartitionsAssigned` / `onPartitionsRevoked`. */
  readonly partitionLifecycle: boolean;
  /** `admin.createTopics()` — required by KafkaShuffleTransport. */
  readonly createTopics: boolean;
  /** `admin.fetchTopicPartitionCount()`. */
  readonly partitionCount: boolean;
  /** `admin.fetchTopicOffsetsByTimestamp()` — `{ type: "timestamp" }` offsets. */
  readonly timestampOffsets: boolean;
}

export const ALL_CAPABILITIES: AdapterCapabilities = {
  eachBatch: true,
  seek: true,
  partitionLifecycle: true,
  createTopics: true,
  partitionCount: true,
  timestampOffsets: true,
};

export interface AdapterCtx {
  readonly broker: string;
  readonly makeClient: (broker: string) => KafkaClient;
  /**
   * Topic bootstrap. Supplied by the binding rather than taken from the
   * adapter under test: a broken `createTopics` would otherwise fail every
   * case opaquely instead of the one that covers it.
   */
  readonly createTopic: (topic: TopicName, partitions?: number) => Promise<void>;
  readonly capabilities: AdapterCapabilities;
}

export interface SuiteCase {
  readonly name: string;
  readonly requires?: ReadonlyArray<keyof AdapterCapabilities>;
  readonly run: (ctx: AdapterCtx) => Promise<void>;
}

// ---------------------------------------------------------------------------
// Helpers — no bun:test, no infra.ts (both import bun:test)
// ---------------------------------------------------------------------------

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function uniqueName(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().slice(0, 8)}`;
}

export async function eventually(
  assertion: () => void | Promise<void>,
  opts?: { timeoutMs?: number; intervalMs?: number },
): Promise<void> {
  const { timeoutMs = 20_000, intervalMs = 250 } = opts ?? {};
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (error) {
      lastError = error;
      await sleep(intervalMs);
    }
  }
  throw lastError;
}

/** Fresh topic + KafkaTopic bound to it, torn down after `body`. */
async function withTopic<T, R>(
  ctx: AdapterCtx,
  opts: { prefix: string; partitions?: number; batchEmit?: boolean },
  body: (kt: KafkaTopic<T>, topic: TopicName, client: KafkaClient) => Promise<R>,
): Promise<R> {
  const topic = TopicName(uniqueName(opts.prefix));
  await ctx.createTopic(topic, opts.partitions);
  const client = ctx.makeClient(ctx.broker);
  const kt = new KafkaTopic<T>({
    kafka: client,
    topic,
    groupId: GroupId(uniqueName("g")),
    batchEmit: opts.batchEmit,
  });
  try {
    return await body(kt, topic, client);
  } finally {
    await kt.disconnect().catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------

export const adapterSuite: readonly SuiteCase[] = [
  // -- producer / consumer basics ------------------------------------------
  {
    name: "publishes and consumes a single message",
    run: (ctx) =>
      withTopic<{ orderId: string }, void>(ctx, { prefix: "single" }, async (kt) => {
        await kt.publish({ orderId: "o-1" });
        const items = await run(
          kt
            .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(uniqueName("g")) })
            .take(1)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(items, [{ orderId: "o-1" }]);
      }),
  },
  {
    name: "publishBatch sends multiple messages",
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "batch" }, async (kt) => {
        await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }, { value: { v: 3 } }]);
        const items = await run(
          kt
            .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(uniqueName("g")) })
            .take(3)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(
          items.map((i) => i.v).sort((a, b) => a - b),
          [1, 2, 3],
        );
      }),
  },
  {
    name: "keyed messages preserve partition ordering",
    run: (ctx) =>
      withTopic<{ seq: number }, void>(ctx, { prefix: "keyed", partitions: 3 }, async (kt) => {
        await kt.publish({ seq: 1 }, { key: "u1" });
        await kt.publish({ seq: 2 }, { key: "u1" });
        await kt.publish({ seq: 3 }, { key: "u1" });
        const items = await run(
          kt
            .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(uniqueName("g")) })
            .take(3)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(
          items.map((i) => i.seq),
          [1, 2, 3],
        );
      }),
  },
  {
    name: "separate consumer groups each receive every message",
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "groups" }, async (kt) => {
        await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }]);
        const readAs = (group: string) =>
          run(
            kt
              .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(group) })
              .take(2)
              .toArray()
              .orDie(),
          );
        const first = await readAs(uniqueName("ga"));
        const second = await readAs(uniqueName("gb"));
        assert.deepEqual(first.map((i) => i.v).sort(), [1, 2]);
        assert.deepEqual(second.map((i) => i.v).sort(), [1, 2]);
      }),
  },

  // -- replay / offsets -----------------------------------------------------
  {
    name: "rewinds a committed group to the earliest offset",
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "replay" }, async (kt) => {
        const group = GroupId(uniqueName("replay-group"));
        await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }, { value: { v: 3 } }]);

        const subscription = kt.subscribeAckWithHandle({
          group,
          offset: { type: "earliest" },
          autoCommit: false,
        });
        await run(
          subscription.stream
            .take(3)
            .through(
              commitBatchWithin<{ v: number }>({
                maxBatchSize: 3,
                maxWaitMs: 1000,
                consumer: subscription.consumer,
                topic: subscription.topic,
              }),
            )
            .drain()
            .catchTag("KafkaCommitError", (error) => die(error))
            .orDie(),
        );
        await subscription.close();

        const replayed = await run(
          kt
            .subscribeFrom({ offset: { type: "earliest" }, group })
            .take(3)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(replayed, [{ v: 1 }, { v: 2 }, { v: 3 }]);
      }),
  },
  {
    name: "subscribeFrom a specific offset skips earlier records",
    requires: ["seek"],
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "specific" }, async (kt) => {
        await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }, { value: { v: 3 } }]);
        const items = await run(
          kt
            .subscribeFrom({
              offset: { type: "specific", value: "1" },
              group: ConsumerGroup(uniqueName("g")),
            })
            .take(2)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(
          items.map((i) => i.v),
          [2, 3],
        );
      }),
  },
  {
    name: "subscribeFrom a timestamp replays records published after it",
    requires: ["timestampOffsets"],
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "ts" }, async (kt) => {
        const before = Date.now() - 60_000;
        await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }]);
        const items = await run(
          kt
            .subscribeFrom({
              offset: { type: "timestamp", value: before },
              group: ConsumerGroup(uniqueName("g")),
            })
            .take(2)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(
          items.map((i) => i.v).sort((a, b) => a - b),
          [1, 2],
        );
      }),
  },

  // -- batch emission -------------------------------------------------------
  {
    name: "batchEmit surfaces driver fetch batches as Stream chunks",
    requires: ["eachBatch"],
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "batch-emit", batchEmit: true }, async (kt) => {
        await kt.publishBatch(
          Array.from({ length: 6 }, (_, index) => ({ value: { v: index + 1 } })),
        );
        const chunkSizes: number[] = [];
        const values = await run(
          kt
            .subscribeFrom({
              offset: { type: "earliest" },
              group: ConsumerGroup(uniqueName("batch-consumer")),
            })
            .mapChunks((chunk) => {
              chunkSizes.push(chunk.length);
              return chunk;
            })
            .take(6)
            .toArray()
            .orDie(),
        );
        assert.deepEqual(
          values.map((value) => value.v),
          [1, 2, 3, 4, 5, 6],
        );
        assert.ok(chunkSizes.reduce((total, size) => total + size, 0) >= 6);
      }),
  },

  // -- partition lifecycle --------------------------------------------------
  {
    name: "blocks delivery until assignment restore and awaits revocation on close",
    requires: ["partitionLifecycle"],
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "lifecycle" }, async (kt) => {
        await kt.publish({ v: 1 });
        const events: string[] = [];
        const subscription = kt.subscribeAckWithHandle({
          fromBeginning: true,
          autoCommit: false,
        });
        subscription.setPartitionLifecycle({
          assigned: async () => {
            events.push("restore:start");
            await sleep(25);
            events.push("restore:end");
          },
          revoking: async () => {
            events.push("revoke:start");
            await sleep(25);
            events.push("revoke:end");
          },
        });

        const envelopes = await run(subscription.stream.take(1).toArray().orDie());
        events.push("delivery");
        assert.deepEqual(
          envelopes.map((envelope) => envelope.value),
          [{ v: 1 }],
        );
        assert.deepEqual(events.slice(0, 3), ["restore:start", "restore:end", "delivery"]);

        await subscription.close();
        assert.deepEqual(events.slice(-2), ["revoke:start", "revoke:end"]);
      }),
  },

  // -- ack + commit flows ---------------------------------------------------
  {
    name: "subscribeAck + autoCommitBatchWithin commits the full watermark",
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "ack" }, async (kt) => {
        const group = GroupId(uniqueName("g"));
        for (let i = 0; i < 10; i++) await kt.publish({ v: i });

        const values: { v: number }[] = await run(
          kt
            .subscribeAck({ group, fromBeginning: true, commitIntervalMs: 200 })
            .through(autoCommitBatchWithin<{ v: number }, Throws<KafkaError>>(5, 300))
            .take(10)
            .toArray()
            .catchTag("AckError", (error) => die(error))
            .orDie(),
        );
        assert.deepEqual(
          values.map((value) => value.v).sort((a, b) => a - b),
          [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        );

        // The shutdown flush is fire-and-forget — poll the committed offset.
        await eventually(async () => {
          const committed = await kt.getCommittedOffset({ group });
          assert.equal(committed, "10");
        });
      }),
  },
  {
    name: "subscribeAck + commitBatchWithin writes batched offsets",
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "commit" }, async (kt, topic, client) => {
        for (let i = 0; i < 10; i++) await kt.publish({ v: i });

        const commitGroup = GroupId(uniqueName("committer"));
        const subscription = kt.subscribeAckWithHandle({
          group: commitGroup,
          offset: { type: "earliest" },
          autoCommit: false,
        });
        const seen: number[] = [];

        await run(
          subscription.stream
            .take(10)
            .evalMap((envelope) =>
              sync(() => {
                seen.push(envelope.value.v);
                return envelope;
              }),
            )
            .through(
              commitBatchWithin<{ v: number }>({
                maxBatchSize: 4,
                maxWaitMs: 1_000,
                consumer: subscription.consumer,
                topic: subscription.topic,
              }),
            )
            .drain()
            .catchTag("KafkaCommitError", (error) => die(error))
            .orDie(),
        );
        assert.deepEqual(
          seen.sort((a, b) => a - b),
          [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        );

        await eventually(async () => {
          const admin = client.admin();
          await admin.connect();
          const offsets = await admin.fetchOffsets({ groupId: commitGroup, topics: [topic] });
          await admin.disconnect();
          const p0 = offsets[0]?.partitions.find((p) => p.partition === 0);
          assert.equal(p0?.offset, KafkaOffset("10"));
        });

        await subscription.close();
      }),
  },

  // -- admin ----------------------------------------------------------------
  {
    name: "admin.fetchTopicOffsetsByTimestamp reports per-partition offsets",
    requires: ["timestampOffsets"],
    run: (ctx) =>
      withTopic<{ v: number }, void>(
        ctx,
        { prefix: "adm-ts", partitions: 2 },
        async (kt, topic, client) => {
          await kt.publish({ v: 1 }, { key: "a" });
          const admin = client.admin();
          await admin.connect();
          try {
            const offsets = await admin.fetchTopicOffsetsByTimestamp(topic, Date.now() - 60_000);
            assert.ok(Array.isArray(offsets), "expected an array of partition offsets");
            assert.ok(offsets.length >= 1, "expected at least one partition");
            for (const entry of offsets) {
              assert.equal(typeof entry.partition, "number");
              assert.equal(typeof entry.offset, "string");
            }
          } finally {
            await admin.disconnect();
          }
        },
      ),
  },
  {
    name: "fetchPartitions reports the configured partition count",
    requires: ["partitionCount"],
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "parts", partitions: 3 }, async (kt) => {
        const count = await kt.fetchPartitions();
        assert.equal(count, 3);
      }),
  },

  // -- shuffle transport ----------------------------------------------------
  {
    name: "KafkaShuffleTransport routes keys to sticky partitions",
    requires: ["createTopics"],
    run: async (ctx) => {
      type Ev = { user: string; n: number };
      const client = ctx.makeClient(ctx.broker);
      const transport = new KafkaShuffleTransport({ kafka: client, partitions: 3 });
      try {
        const { source, sink } = await transport.getOrCreateRepartitionChannel<Ev>({
          name: ChannelName(uniqueName("shuffle")),
          group: ConsumerGroup(uniqueName("sg")),
          codec: JsonCodec as Codec<Ev>,
        });

        const events: Ev[] = [
          { user: "alice", n: 1 },
          { user: "bob", n: 1 },
          { user: "alice", n: 2 },
          { user: "bob", n: 2 },
        ];
        for (const event of events) await sink.publish(event, { key: event.user });

        const envelopes = await run(
          (source as KafkaTopic<Ev>)
            .subscribeAck({ fromBeginning: true })
            .take(4)
            .toArray()
            .orDie(),
        );

        const received = envelopes
          .map((envelope) => envelope.value)
          .sort((a, b) => a.user.localeCompare(b.user) || a.n - b.n);
        assert.deepEqual(received, [
          { user: "alice", n: 1 },
          { user: "alice", n: 2 },
          { user: "bob", n: 1 },
          { user: "bob", n: 2 },
        ]);

        const partitionsByUser = new Map<string, Set<unknown>>();
        for (const envelope of envelopes) {
          const set = partitionsByUser.get(envelope.value.user) ?? new Set();
          set.add(envelope.metadata.partition);
          partitionsByUser.set(envelope.value.user, set);
        }
        for (const [, partitions] of partitionsByUser) {
          assert.equal(partitions.size, 1);
        }
      } finally {
        await transport.disconnect().catch(() => {});
      }
    },
  },

  // -- lifecycle ------------------------------------------------------------
  {
    name: "disconnect is idempotent",
    run: (ctx) =>
      withTopic<{ v: number }, void>(ctx, { prefix: "disc" }, async (kt) => {
        await kt.publish({ v: 1 });
        await kt.disconnect();
        await kt.disconnect();
      }),
  },
];

// ---------------------------------------------------------------------------
// Generic runner — used by the Node binding; the Bun binding maps cases to
// `it()` instead so failures surface per test.
// ---------------------------------------------------------------------------

export interface CaseResult {
  readonly name: string;
  readonly status: "pass" | "fail" | "skip";
  readonly durationMs: number;
  readonly reason?: string;
}

export function applicable(testCase: SuiteCase, capabilities: AdapterCapabilities): boolean {
  return (testCase.requires ?? []).every((capability) => capabilities[capability]);
}

export async function runSuite(ctx: AdapterCtx): Promise<CaseResult[]> {
  const results: CaseResult[] = [];
  for (const testCase of adapterSuite) {
    if (!applicable(testCase, ctx.capabilities)) {
      const missing = (testCase.requires ?? []).filter((c) => !ctx.capabilities[c]);
      results.push({
        name: testCase.name,
        status: "skip",
        durationMs: 0,
        reason: `unsupported: ${missing.join(", ")}`,
      });
      continue;
    }
    const started = Date.now();
    try {
      await testCase.run(ctx);
      results.push({ name: testCase.name, status: "pass", durationMs: Date.now() - started });
    } catch (error) {
      results.push({
        name: testCase.name,
        status: "fail",
        durationMs: Date.now() - started,
        reason: error instanceof Error ? (error.stack ?? error.message) : String(error),
      });
    }
  }
  return results;
}

// ---------------------------------------------------------------------------
// Adapter registry — metadata only, no driver imports.
//
// Lives here so the Bun binding and the Node binding agree on capabilities
// without either importing the other's drivers: `@spilne/perfect-kafka-platformatic`
// cannot be imported from a Bun process at all, so the Bun test can never
// reach `run-suite-node.ts`.
// ---------------------------------------------------------------------------

export interface AdapterProfile {
  readonly capabilities: AdapterCapabilities;
  /** Whether the driver can be imported and run inside a Bun process. */
  readonly bunCompatible: boolean;
  /** Why not, when it isn't — surfaced in the test name. */
  readonly bunNote?: string;
}

export const ADAPTER_PROFILES: Record<string, AdapterProfile> = {
  kafkajs: {
    capabilities: ALL_CAPABILITIES,
    bunCompatible: true,
  },
  platformatic: {
    // Stream-mode driver: no `run()`, hence no eachBatch and no `batchEmit`.
    capabilities: { ...ALL_CAPABILITIES, eachBatch: false },
    bunCompatible: false,
    bunNote: "@platformatic/kafka's consumer is unsupported on Bun",
  },
};
