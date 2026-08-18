// Unit tests for KafkaTopic — uses a fake KafkaClient that invokes our
// consumer callback with canned batches, so we can assert ordering and
// dispatch without booting a real broker.
//
// Ported from promin's kafka-topic.test.ts. promin's batchEmit-flag tests
// became eachMessage-dispatch tests: perfect's v1 port is per-message only
// (batchEmit / chunk-emit deferred — see kafka-topic.ts header).

import { describe, it, expect } from "bun:test";
import { fromPromise, run } from "@perfect/core";
import { KafkaTopic } from "../src/kafka-topic";
import type {
  KafkaAdmin,
  KafkaClient,
  KafkaConsumer,
  KafkaMessage,
  KafkaOffsetCommit,
  KafkaProducer,
} from "../src/kafka-types";
import { TopicName, GroupId, PartitionId, KafkaOffset } from "../src/brands";

/**
 * Fake client that simulates a kafkajs-style consumer. The consumer's
 * `run()` receives the user's callbacks; the fake picks whichever is set
 * and invokes it with canned data, mirroring how kafkajs dispatches based
 * on whether `eachMessage` or `eachBatch` was provided.
 *
 * `batches` is the data to replay (outer index = partition). `dispatchedVia`
 * records which callback the code under test asked for; `committed` captures
 * every commitOffsets call.
 */
function makeFakeKafka(opts: {
  topic: TopicName;
  batches: ReadonlyArray<ReadonlyArray<{ value: unknown; offset: string; key?: string }>>;
}): {
  client: KafkaClient;
  consumer: KafkaConsumer;
  dispatchedVia: { current: "eachMessage" | "eachBatch" | null };
  committed: KafkaOffsetCommit[][];
  subscriptions: Array<{ topic: TopicName; fromBeginning?: boolean }>;
  sought: Array<{ topic: TopicName; partition: PartitionId; offset: KafkaOffset }>;
} {
  const dispatchedVia: { current: "eachMessage" | "eachBatch" | null } = { current: null };
  const committed: KafkaOffsetCommit[][] = [];
  const subscriptions: Array<{ topic: TopicName; fromBeginning?: boolean }> = [];
  const sought: Array<{ topic: TopicName; partition: PartitionId; offset: KafkaOffset }> = [];

  const consumer: KafkaConsumer = {
    async connect() {},
    async disconnect() {},
    async subscribe(params) {
      subscriptions.push(params);
    },
    seek(params) {
      sought.push(params);
    },
    async commitOffsets(offsets) {
      committed.push(offsets);
    },
    async run(params) {
      // kafkajs-style dispatch: use whichever callback is set. If both,
      // kafkajs would throw — we assert the code under test passes ONE,
      // not both.
      const hasMessage = typeof params.eachMessage === "function";
      const hasBatch = typeof params.eachBatch === "function";
      if (hasMessage && hasBatch) {
        throw new Error(
          "KafkaTopic passed both eachMessage and eachBatch — kafkajs would reject this as a ConfigurationError",
        );
      }
      if (hasBatch) {
        dispatchedVia.current = "eachBatch";
      } else if (hasMessage) {
        dispatchedVia.current = "eachMessage";
        for (let partition = 0; partition < opts.batches.length; partition++) {
          const batch = opts.batches[partition]!;
          for (const m of batch) {
            const msg: KafkaMessage = {
              topic: opts.topic,
              partition: PartitionId(partition),
              message: {
                key: m.key ?? null,
                value: JSON.stringify(m.value),
                offset: KafkaOffset(m.offset),
                timestamp: "0",
              },
            };
            await params.eachMessage!(msg);
          }
        }
      }
      // Block forever so the Stream keeps running until take() resolves.
      await new Promise(() => {});
    },
  };

  const producer: KafkaProducer = {
    async connect() {},
    async disconnect() {},
    async send() {},
  };
  const admin: KafkaAdmin = {
    async connect() {},
    async disconnect() {},
    async fetchOffsets() {
      return [];
    },
    async fetchTopicOffsetsByTimestamp() {
      return [];
    },
    async fetchTopicPartitionCount() {
      return opts.batches.length;
    },
  };

  const client: KafkaClient = {
    producer: () => producer,
    consumer: () => consumer,
    admin: () => admin,
  };

  return { client, consumer, dispatchedVia, committed, subscriptions, sought };
}

describe("KafkaTopic — subscribe", () => {
  it("delivers messages in order via eachMessage", async () => {
    const { client, dispatchedVia } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [
        [
          { value: { n: 1 }, offset: "0" },
          { value: { n: 2 }, offset: "1" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    const values = await run(topic.subscribe().take(2).toArray());
    expect(values.map((v) => v.n)).toEqual([1, 2]);
    expect(dispatchedVia.current).toBe("eachMessage");
  });

  it("delivers messages from multiple partitions", async () => {
    const { client } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [
        [
          { value: { n: 10 }, offset: "0" },
          { value: { n: 20 }, offset: "1" },
        ],
        [
          { value: { n: 30 }, offset: "0" },
          { value: { n: 40 }, offset: "1" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    const values = await run(topic.subscribe().take(4).toArray());
    expect(values.map((v) => v.n)).toEqual([10, 20, 30, 40]);
  });

  it("never passes BOTH eachBatch and eachMessage to the driver", async () => {
    // Regression guard: kafkajs throws a ConfigurationError when both are
    // set, so our code must commit to exactly one per run() call. The fake
    // client's run() throws if both are passed — so this test fails loudly
    // if we ever regress.
    const { client, dispatchedVia } = makeFakeKafka({
      topic: TopicName("t"),
      batches: [[{ value: { n: 1 }, offset: "0" }]],
    });
    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("t"),
      groupId: GroupId("g"),
    });
    const values = await run(topic.subscribe().take(1).toArray());
    expect(values).toEqual([{ n: 1 }]);
    expect(dispatchedVia.current).toBe("eachMessage");
  });
});

describe("KafkaTopic — subscribeAck", () => {
  it("exposes the subscription consumer as a commit handle", async () => {
    const { client, consumer, subscriptions } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [
        [
          { value: { n: 1 }, offset: "0" },
          { value: { n: 2 }, offset: "1" },
        ],
      ],
    });
    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    const subscription = topic.subscribeAckWithHandle({
      autoCommit: false,
      offset: { type: "earliest" },
    });
    expect(subscription.consumer).toBe(consumer);
    expect(await run(subscription.stream.take(1).toArray())).toHaveLength(1);
    expect(subscriptions[0]?.fromBeginning).toBe(true);
    await subscription.close();
  });

  it("applies specific replay offsets to every partition", async () => {
    const { client, sought } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [[{ value: { n: 1 }, offset: "7" }], [{ value: { n: 2 }, offset: "7" }]],
    });
    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    await run(
      topic
        .subscribeAck({ offset: { type: "specific", value: "7" } })
        .take(1)
        .toArray(),
    );

    expect(sought).toEqual([
      { topic: TopicName("orders"), partition: PartitionId(0), offset: KafkaOffset("7") },
      { topic: TopicName("orders"), partition: PartitionId(1), offset: KafkaOffset("7") },
    ]);
  });

  it("delivers envelopes in order with offset metadata", async () => {
    const { client, dispatchedVia } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [
        [
          { value: { n: 1 }, offset: "0" },
          { value: { n: 2 }, offset: "1" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    const envelopes = await run(topic.subscribeAck().take(2).toArray());
    expect(envelopes.map((e) => e.value.n)).toEqual([1, 2]);
    expect(envelopes[0]!.metadata.offset).toBe("0");
    expect(envelopes[1]!.metadata.offset).toBe("1");
    expect(envelopes[0]!.metadata.topic).toBe("orders");
    expect(envelopes[0]!.metadata.partition).toBe(0);
    expect(dispatchedVia.current).toBe("eachMessage");
  });

  it("commits acked offsets on shutdown flush", async () => {
    const { client, committed } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [
        [
          { value: { n: 1 }, offset: "0" },
          { value: { n: 2 }, offset: "1" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    // Ack while streaming (like a real handler would) — the shutdown flush
    // then commits the completed offsets.
    const values = await run(
      topic
        .subscribeAck()
        .evalMap((env) =>
          fromPromise(
            async () => {
              await env.ack();
              return env.value;
            },
            (e) => e,
          ),
        )
        .take(2)
        .toArray(),
    );
    expect(values.map((v) => v.n)).toEqual([1, 2]);

    // The stream's cleanup flush is fire-and-forget — give it a tick.
    await new Promise((r) => setTimeout(r, 20));

    expect(committed).toEqual([
      [{ topic: TopicName("orders"), partition: PartitionId(0), offset: KafkaOffset("2") }],
    ]);
  });

  it("does not commit unacked offsets", async () => {
    const { client, committed } = makeFakeKafka({
      topic: TopicName("orders"),
      batches: [[{ value: { n: 1 }, offset: "0" }]],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("orders"),
      groupId: GroupId("g"),
    });

    await run(topic.subscribeAck().take(1).toArray());
    await new Promise((r) => setTimeout(r, 20));

    expect(committed).toEqual([]);
  });
});

describe("KafkaTopic — stream-mode driver", () => {
  it("consumes via consumer.stream() when the driver exposes it", async () => {
    const messages: KafkaMessage[] = [1, 2, 3].map((n, i) => ({
      topic: TopicName("t"),
      partition: PartitionId(0),
      message: {
        key: null,
        value: JSON.stringify({ n }),
        offset: KafkaOffset(String(i)),
        timestamp: "0",
      },
    }));

    const consumer: KafkaConsumer = {
      async connect() {},
      async disconnect() {},
      async subscribe() {},
      async commitOffsets() {},
      stream() {
        return (async function* () {
          yield* messages;
          // Stay open like a live consumer.
          await new Promise(() => {});
        })();
      },
    };
    const client: KafkaClient = {
      producer: () => ({ async connect() {}, async disconnect() {}, async send() {} }),
      consumer: () => consumer,
      admin: () => ({
        async connect() {},
        async disconnect() {},
        async fetchOffsets() {
          return [];
        },
        async fetchTopicOffsetsByTimestamp() {
          return [];
        },
      }),
    };

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: TopicName("t"),
      groupId: GroupId("g"),
    });
    const values = await run(topic.subscribe().take(3).toArray());
    expect(values.map((v) => v.n)).toEqual([1, 2, 3]);
  });
});
