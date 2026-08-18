// Unit tests for commitBatchWithin — a fake consumer captures commitOffsets
// calls so we can assert batching, contiguity (gap holding), and unwrapping
// without a broker.

import { describe, it, expect } from "bun:test";
import { die, run, succeed, sync } from "@perfect/core";
import { Stream } from "@perfect/core/stream";
import type { Envelope } from "@perfect/core/connect";
import { commitBatchWithin } from "../src/commit-batch-within";
import type { KafkaConsumer, KafkaOffsetCommit } from "../src/kafka-types";
import { TopicName, PartitionId, KafkaOffset } from "../src/brands";

/** Expected-commit helper — brands the identifier fields. */
function commit(partition: number, offset: string): KafkaOffsetCommit {
  return { topic: TopicName("t"), partition: PartitionId(partition), offset: KafkaOffset(offset) };
}

function makeFakeConsumer(opts?: { failCommits?: number }): {
  consumer: KafkaConsumer;
  committed: KafkaOffsetCommit[][];
} {
  const committed: KafkaOffsetCommit[][] = [];
  let failures = opts?.failCommits ?? 0;

  const consumer: KafkaConsumer = {
    async connect() {},
    async disconnect() {},
    async subscribe() {},
    async commitOffsets(offsets) {
      if (failures > 0) {
        failures--;
        throw new Error("commit failed");
      }
      committed.push(offsets);
    },
  };

  return { consumer, committed };
}

function envelope<T>(value: T, partition: number, offset: number): Envelope<T> {
  return {
    value,
    ack: () => succeed(undefined),
    nack: () => succeed(undefined),
    metadata: { topic: "t", partition, offset: String(offset) },
  };
}

describe("commitBatchWithin", () => {
  it("commits per full batch and unwraps envelopes to values", async () => {
    const { consumer, committed } = makeFakeConsumer();
    const envelopes = [0, 1, 2, 3].map((i) => envelope(`v${i}`, 0, i));

    const values = await run(
      Stream.fromIterable(envelopes)
        .through(
          commitBatchWithin({
            maxBatchSize: 2,
            maxWaitMs: 60_000,
            consumer,
            topic: TopicName("t"),
          }),
        )
        .toArray()
        .catchTag("KafkaCommitError", (error) => die(error)),
    );

    expect(values).toEqual(["v0", "v1", "v2", "v3"]);
    expect(committed).toEqual([[commit(0, "2")], [commit(0, "4")]]);
  });

  it("flushes a partial batch when the stream ends", async () => {
    const { consumer, committed } = makeFakeConsumer();
    const envelopes = [0, 1, 2].map((i) => envelope(i, 0, i));

    const values = await run(
      Stream.fromIterable(envelopes)
        .through(
          commitBatchWithin({
            maxBatchSize: 100,
            maxWaitMs: 60_000,
            consumer,
            topic: TopicName("t"),
          }),
        )
        .toArray()
        .catchTag("KafkaCommitError", (error) => die(error)),
    );

    expect(values).toEqual([0, 1, 2]);
    expect(committed).toEqual([[commit(0, "3")]]);
  });

  it("holds commits behind an offset gap (contiguity)", async () => {
    const { consumer, committed } = makeFakeConsumer();
    // Offset 2 is missing — only 0..1 are contiguous, 3 must be held.
    const envelopes = [0, 1, 3].map((i) => envelope(i, 0, i));

    const values = await run(
      Stream.fromIterable(envelopes)
        .through(
          commitBatchWithin({
            maxBatchSize: 3,
            maxWaitMs: 60_000,
            consumer,
            topic: TopicName("t"),
          }),
        )
        .toArray()
        .catchTag("KafkaCommitError", (error) => die(error)),
    );

    expect(values).toEqual([0, 1, 3]);
    expect(committed).toEqual([[commit(0, "2")]]);
  });

  it("commits per partition", async () => {
    const { consumer, committed } = makeFakeConsumer();
    const envelopes = [envelope("a", 0, 0), envelope("b", 1, 0), envelope("c", 0, 1)];

    await run(
      Stream.fromIterable(envelopes)
        .through(
          commitBatchWithin({
            maxBatchSize: 3,
            maxWaitMs: 60_000,
            consumer,
            topic: TopicName("t"),
          }),
        )
        .toArray()
        .catchTag("KafkaCommitError", (error) => die(error)),
    );

    expect(committed).toHaveLength(1);
    expect(committed[0]).toEqual(expect.arrayContaining([commit(0, "2"), commit(1, "1")]));
  });

  it("commits by time when the batch is not full", async () => {
    const { consumer, committed } = makeFakeConsumer();

    let emitEnv: (env: Envelope<string>) => void = () => {};
    let close: () => void = () => {};

    const collecting = run(
      Stream.async<Envelope<string>, never>((emit, doClose) => {
        emitEnv = emit;
        close = doClose;
        return sync(() => {});
      })
        .through(
          commitBatchWithin({ maxBatchSize: 100, maxWaitMs: 50, consumer, topic: TopicName("t") }),
        )
        .toArray()
        .catchTag("KafkaCommitError", (error) => die(error)),
    );

    // Let the stream start so the register callback captured emit/close.
    await new Promise((r) => setTimeout(r, 0));
    emitEnv(envelope("only", 0, 0));
    // Wait past maxWaitMs so groupWithin's timer flushes the partial batch.
    await new Promise((r) => setTimeout(r, 120));
    expect(committed).toEqual([[commit(0, "1")]]);

    close();
    const values = await collecting;
    expect(values).toEqual(["only"]);
  });

  it("a failed commit surfaces as typed KafkaCommitError", async () => {
    const { consumer, committed } = makeFakeConsumer({ failCommits: 1 });
    const envelopes = [0, 1, 2, 3].map((i) => envelope(i, 0, i));

    const message = await run(
      Stream.fromIterable(envelopes)
        .through(
          commitBatchWithin({
            maxBatchSize: 2,
            maxWaitMs: 60_000,
            consumer,
            topic: TopicName("t"),
          }),
        )
        .toArray()
        .map(() => "unexpected")
        .catchTag("KafkaCommitError", (error) => {
          expect(error.offsets).toEqual([commit(0, "2")]);
          return succeed((error.cause as Error).message);
        }),
    );

    expect(message).toBe("commit failed");
    expect(committed).toEqual([]);
  });
});
