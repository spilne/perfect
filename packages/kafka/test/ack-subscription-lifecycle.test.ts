import { expect, test } from "bun:test";
import { AckSubscriptionLifecycle } from "../src/ack-subscription-lifecycle";
import { TopicName, PartitionId } from "../src/brands";
import type { KafkaConsumer, KafkaOffsetCommit } from "../src/kafka-types";

test("failed commits retain their batch and concurrent flushes share one request", async () => {
  const requests: KafkaOffsetCommit[][] = [];
  let rejectCommit: (cause: unknown) => void = () => {};
  const consumer: KafkaConsumer = {
    async connect() {},
    async disconnect() {},
    async subscribe() {},
    commitOffsets(offsets) {
      requests.push(offsets);
      return requests.length === 1
        ? new Promise<void>((_, reject) => {
            rejectCommit = reject;
          })
        : Promise.resolve();
    },
  };
  const lifecycle = new AckSubscriptionLifecycle({
    consumer,
    topic: TopicName("orders"),
    autoCommit: true,
  });
  lifecycle.tracker.observe(PartitionId(0), 10);
  lifecycle.tracker.complete(PartitionId(0), 10);
  const first = lifecycle.flushCommits();
  expect(lifecycle.flushCommits()).toBe(first);
  rejectCommit(new Error("broker unavailable"));
  await expect(first).rejects.toThrow("broker unavailable");
  await lifecycle.flushCommits();
  expect(requests).toHaveLength(2);
  expect(requests[1]).toBe(requests[0]);
  expect(requests[1]?.[0]?.offset).toBe("11");
  await lifecycle.close();
});

test("closing is idempotent and disconnects even when the final commit fails", async () => {
  const events: string[] = [];
  const consumer: KafkaConsumer = {
    async connect() {},
    async subscribe() {},
    async disconnect() {
      events.push("disconnect");
    },
    async commitOffsets() {
      events.push("commit");
      throw new Error("commit failed");
    },
    onPartitionsAssigned() {
      return () => {
        events.push("remove assigned");
      };
    },
    onPartitionsRevoked() {
      return () => {
        events.push("remove revoked");
      };
    },
  };
  const lifecycle = new AckSubscriptionLifecycle({
    consumer,
    topic: TopicName("orders"),
    autoCommit: true,
  });
  lifecycle.tracker.observe(PartitionId(0), 0);
  lifecycle.tracker.complete(PartitionId(0), 0);
  const closed = lifecycle.close();
  expect(lifecycle.close()).toBe(closed);
  expect(lifecycle.stopped).toBe(true);
  await expect(closed).rejects.toThrow("commit failed");
  expect(events).toEqual(["commit", "remove assigned", "remove revoked", "disconnect"]);
});
