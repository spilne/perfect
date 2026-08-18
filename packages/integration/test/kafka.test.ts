// ---------------------------------------------------------------------------
// Real-broker integration tests for @perfect/kafka — Redpanda via
// testcontainers. Suites skip cleanly when Docker is unavailable
// (see src/infra.ts).
// ---------------------------------------------------------------------------

import { describe, it, expect, setDefaultTimeout } from "bun:test";

// Kafka operations (consumer group join, rebalancing, commits) need generous timeouts
setDefaultTimeout(300_000);

import { die, run, fromPromise } from "@perfect/core";
import {
  autoCommitBatchWithin,
  ChannelName,
  ConsumerGroup,
  JsonCodec,
  type Codec,
} from "@perfect/core/connect";
import {
  KafkaTopic,
  KafkaShuffleTransport,
  commitBatchWithin,
  TopicName,
  GroupId,
  KafkaOffset,
  type KafkaClient,
} from "@perfect/kafka";
import { withKafka, withApacheKafka, eventually, uniqueName } from "../src/infra";
import { createKafkajsClient, createTopic } from "../src/adapters/kafkajs-adapter";
import { createPlatformaticClient } from "../src/adapters/stream-adapter";

// ---------------------------------------------------------------------------
// Shared test suite — same tests, different adapter
// ---------------------------------------------------------------------------

function adapterTests(
  name: string,
  getBroker: () => string,
  makeClient: (broker: string) => KafkaClient,
) {
  describe(name, () => {
    it("publishes and consumes a single message", async () => {
      const broker = getBroker();
      const topic = TopicName(uniqueName("single"));
      await createTopic(broker, topic);

      const kt = new KafkaTopic<{ orderId: string }>({
        kafka: makeClient(broker),
        topic,
        groupId: GroupId(uniqueName("g")),
      });

      await kt.publish({ orderId: "o-1" });

      const items = await run(
        kt
          .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(uniqueName("g")) })
          .take(1)
          .toArray(),
      );

      expect(items).toEqual([{ orderId: "o-1" }]);
      await kt.disconnect();
    });

    it("publishBatch sends multiple messages", async () => {
      const broker = getBroker();
      const topic = TopicName(uniqueName("batch"));
      await createTopic(broker, topic);

      const kt = new KafkaTopic<{ v: number }>({
        kafka: makeClient(broker),
        topic,
        groupId: GroupId(uniqueName("g")),
      });

      await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }, { value: { v: 3 } }]);

      const items = await run(
        kt
          .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(uniqueName("g")) })
          .take(3)
          .toArray(),
      );

      expect(items.map((i) => i.v).sort()).toEqual([1, 2, 3]);
      await kt.disconnect();
    });

    it("keyed messages preserve partition ordering", async () => {
      const broker = getBroker();
      const topic = TopicName(uniqueName("keyed"));
      await createTopic(broker, topic, 3);

      const kt = new KafkaTopic<{ seq: number }>({
        kafka: makeClient(broker),
        topic,
        groupId: GroupId(uniqueName("g")),
      });

      await kt.publish({ seq: 1 }, { key: "u1" });
      await kt.publish({ seq: 2 }, { key: "u1" });
      await kt.publish({ seq: 3 }, { key: "u1" });

      const items = await run(
        kt
          .subscribeFrom({ offset: { type: "earliest" }, group: ConsumerGroup(uniqueName("g")) })
          .take(3)
          .toArray(),
      );

      expect(items.map((i) => i.seq)).toEqual([1, 2, 3]);
      await kt.disconnect();
    });
  });
}

// ---------------------------------------------------------------------------
// All tests under one Redpanda container
// ---------------------------------------------------------------------------

withKafka("Kafka integration (Redpanda)", (ctx) => {
  const getBroker = () => ctx.broker;

  adapterTests("kafkajs adapter (callback-based)", getBroker, createKafkajsClient);

  describe("replay controls", () => {
    it("rewinds a committed consumer group to the earliest offset", async () => {
      const client = createKafkajsClient(ctx.broker);
      const topic = TopicName(uniqueName("replay"));
      await createTopic(ctx.broker, topic);
      const group = GroupId(uniqueName("replay-group"));
      const kt = new KafkaTopic<{ v: number }>({ kafka: client, topic, groupId: group });
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
          .catchTag("KafkaCommitError", (error) => die(error)),
      );
      await subscription.close();

      const replayed = await run(
        kt
          .subscribeFrom({ offset: { type: "earliest" }, group })
          .take(3)
          .toArray(),
      );

      expect(replayed).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
      await kt.disconnect();
    });
  });

  describe("batch emission", () => {
    it("consumes callback-driver fetch batches as Stream chunks", async () => {
      const client = createKafkajsClient(ctx.broker);
      const topic = TopicName(uniqueName("batch-emit"));
      await createTopic(ctx.broker, topic);
      const kt = new KafkaTopic<{ v: number }>({
        kafka: client,
        topic,
        groupId: GroupId(uniqueName("batch-group")),
        batchEmit: true,
      });
      await kt.publishBatch(Array.from({ length: 6 }, (_, value) => ({ value: { v: value + 1 } })));
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
          .toArray(),
      );

      expect(values.map((value) => value.v)).toEqual([1, 2, 3, 4, 5, 6]);
      expect(chunkSizes.reduce((total, size) => total + size, 0)).toBeGreaterThanOrEqual(6);
      await kt.disconnect();
    });
  });

  // -- subscribeAck + autoCommitBatchWithin: the production ack flow --
  describe("subscribeAck + autoCommitBatchWithin — batched ack flow", () => {
    it("processes all messages and commits acked offsets for the group", async () => {
      const client = createKafkajsClient(ctx.broker);
      const topic = TopicName(uniqueName("ack"));
      await createTopic(ctx.broker, topic);
      const group = GroupId(uniqueName("g"));

      const kt = new KafkaTopic<{ v: number }>({ kafka: client, topic, groupId: group });
      for (let i = 0; i < 10; i++) await kt.publish({ v: i });

      const seen: number[] = [];

      // take() sits AFTER the commit pipe so all 10 envelopes are acked
      // before the source is finalized — the shutdown flush then commits
      // the full watermark.
      const values: { v: number }[] = await run(
        kt
          .subscribeAck({ group, fromBeginning: true, commitIntervalMs: 200 })
          .through(autoCommitBatchWithin<{ v: number }>(5, 300))
          .take(10)
          .toArray()
          .catchTag("AckError", (error) => die(error)),
      );

      for (const v of values) seen.push(v.v);
      expect(seen.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

      // The shutdown flush is fire-and-forget — poll the committed offset.
      await eventually(
        async () => {
          const committed = await kt.getCommittedOffset({ group });
          expect(committed).toBe("10");
        },
        { timeoutMs: 20_000, intervalMs: 500 },
      );

      await kt.disconnect();
    });
  });

  // -- subscribeAck + commitBatchWithin: offset-writing commit pipe --
  describe("subscribeAck + commitBatchWithin — batched offset commits", () => {
    it("commits batched offsets through a real consumer", async () => {
      const client = createKafkajsClient(ctx.broker);
      const topic = TopicName(uniqueName("commit"));
      await createTopic(ctx.broker, topic);

      const kt = new KafkaTopic<{ v: number }>({
        kafka: client,
        topic,
        groupId: GroupId(uniqueName("read")),
      });
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
          .evalMap((env) =>
            fromPromise(
              async () => {
                seen.push(env.value.v);
                return env;
              },
              (e) => e,
            ),
          )
          .through(
            commitBatchWithin({
              maxBatchSize: 4,
              maxWaitMs: 1_000,
              consumer: subscription.consumer,
              topic: subscription.topic,
            }),
          )
          .drain()
          .catchTag("KafkaCommitError", (error) => die(error)),
      );

      expect(seen.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

      await eventually(
        async () => {
          const admin = client.admin();
          await admin.connect();
          const offsets = await admin.fetchOffsets({ groupId: commitGroup, topics: [topic] });
          await admin.disconnect();
          const p0 = offsets[0]?.partitions.find((p) => p.partition === 0);
          expect(p0?.offset).toBe(KafkaOffset("10"));
        },
        { timeoutMs: 20_000, intervalMs: 500 },
      );

      await subscription.close();
      await kt.disconnect();
    });
  });

  // -- KafkaShuffleTransport: repartition channel over a real broker --
  describe("KafkaShuffleTransport — repartition channel", () => {
    it("routes keyed values through a repartition channel with sticky partitions", async () => {
      const client = createKafkajsClient(ctx.broker);
      const transport = new KafkaShuffleTransport({ kafka: client, partitions: 3 });

      type Ev = { user: string; n: number };
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
      for (const e of events) await sink.publish(e, { key: e.user });

      // Acknowledgeable's contract has no replay knob — downcast to reach
      // KafkaTopic's fromBeginning option (see README: interface friction).
      const envelopes = await run(
        (source as KafkaTopic<Ev>).subscribeAck({ fromBeginning: true }).take(4).toArray(),
      );

      const received = envelopes
        .map((e) => e.value)
        .sort((a, b) => a.user.localeCompare(b.user) || a.n - b.n);
      expect(received).toEqual([
        { user: "alice", n: 1 },
        { user: "alice", n: 2 },
        { user: "bob", n: 1 },
        { user: "bob", n: 2 },
      ]);

      // Same key must always land on the same partition.
      const partitionsByUser = new Map<string, Set<unknown>>();
      for (const env of envelopes) {
        const set = partitionsByUser.get(env.value.user) ?? new Set();
        set.add(env.metadata.partition);
        partitionsByUser.set(env.value.user, set);
      }
      for (const [, partitions] of partitionsByUser) {
        expect(partitions.size).toBe(1);
      }

      await transport.disconnect();
    });
  });
});

// ---------------------------------------------------------------------------
// @platformatic/kafka — needs real Apache Kafka (Redpanda has API gaps).
// Uses KafkaContainer from @testcontainers/kafka with the Confluent image.
// Slow to start (~60s JVM): skipped by default, enable with KAFKA_FULL=1.
// ---------------------------------------------------------------------------

const runFullKafka = process.env.KAFKA_FULL === "1";

if (runFullKafka) {
  withApacheKafka("@platformatic/kafka adapter (Apache Kafka)", (ctx) => {
    adapterTests("stream-based consume", () => ctx.broker, createPlatformaticClient);
  });
}
