import { fromPromise, run } from "@perfect/core";
import { KafkaTopic } from "./src/kafka-topic";
import type { KafkaClient, KafkaConsumer, KafkaMessage } from "./src/kafka-types";

const committed: unknown[] = [];
const consumer: KafkaConsumer = {
  async connect() {},
  async disconnect() {
    console.log("disconnect");
  },
  async subscribe() {},
  async commitOffsets(o) {
    console.log("commitOffsets", o);
    committed.push(o);
  },
  async run(params) {
    for (let i = 0; i < 2; i++) {
      const msg: KafkaMessage = {
        topic: "orders",
        partition: 0,
        message: {
          key: null,
          value: JSON.stringify({ n: i + 1 }),
          offset: String(i),
          timestamp: "0",
        },
      };
      await params.eachMessage!(msg);
    }
    await new Promise(() => {});
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
const topic = new KafkaTopic<{ n: number }>({ kafka: client, topic: "orders", groupId: "g" });
const values = await run(
  topic
    .subscribeAck()
    .evalMap((env) =>
      fromPromise(
        async () => {
          console.log("ack", env.metadata.offset);
          await env.ack();
          return env.value;
        },
        (e) => e,
      ),
    )
    .take(2)
    .toArray(),
);
console.log("values", values);
await new Promise((r) => setTimeout(r, 50));
console.log("committed", committed);
