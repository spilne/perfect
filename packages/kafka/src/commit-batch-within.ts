// ---------------------------------------------------------------------------
// commitBatchWithin — fs2-kafka-style batched offset commit pipe
//
// Usage:
//   topic.subscribeAck()
//     .parEvalMap(25, (env) => fromPromise(() => process(env.value), (e) => e).map(() => env))
//     .through(commitBatchWithin({ maxBatchSize: 500, maxWaitMs: 15_000, consumer, topic }))
//     .drain();
//
// Batches ack'd envelopes by count or time, commits offsets as a group.
// Uses OffsetTracker internally for parallel-safe contiguous commits.
//
// Ported from promin's commit-batch-within.ts. The setInterval-based timer
// became Stream.groupWithin — same batch-by-count-or-time semantics, but
// fiber-native (matches core's autoCommitBatchWithin, the queue-agnostic
// variant that lives in @perfect/core/connect). promin's `flushing`
// re-entrancy guard is gone because groupWithin + evalMap serializes
// batches: each batch's commit completes before the next batch is
// processed, so two commitOffsets calls can never be in flight at once.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@perfect/core";
import { fromPromise } from "@perfect/core";
import { Stream } from "@perfect/core/stream";
import type { Pipe } from "@perfect/core/stream";
import { OffsetTracker } from "@perfect/core/connect";
import type { Envelope } from "@perfect/core/connect";
import type { KafkaConsumer, KafkaOffsetCommit } from "./kafka-types";
import { type TopicName, PartitionId, KafkaOffset } from "./brands";
import { KafkaCommitError, type KafkaError } from "./kafka-error";

export interface CommitBatchWithinConfig {
  /** Commit after this many messages. */
  maxBatchSize: number;
  /** Commit after this many ms, even if batch isn't full. */
  maxWaitMs: number;
  /** Kafka consumer to commit offsets on. */
  consumer: KafkaConsumer;
  /** Topic name (for offset commits). */
  topic: TopicName;
}

/**
 * Pipe that batches envelope acks and commits offsets to Kafka.
 *
 * Collects processed envelopes, tracks contiguous offsets via OffsetTracker,
 * and flushes commits when either `maxBatchSize` or `maxWaitMs` is reached.
 *
 * The pipe unwraps `Envelope<T>` → `T`, so downstream sees plain values.
 */
export function commitBatchWithin<T, AckS = Throws<KafkaError>>(
  config: CommitBatchWithinConfig,
): Pipe<Envelope<T, AckS>, T, Throws<KafkaCommitError>> {
  return <S>(stream: Stream<Envelope<T, AckS>, S>) => {
    const tracker = new OffsetTracker();

    return stream
      .groupWithin(config.maxBatchSize, config.maxWaitMs)
      .evalMap((chunk) => {
        let attemptedOffsets: KafkaOffsetCommit[] = [];
        return fromPromise(
          async () => {
            for (const env of chunk) {
              // Envelope metadata is deliberately Record<string, unknown> —
              // rebrand at this boundary.
              const partition = PartitionId((env.metadata.partition as number) ?? 0);
              const offset = Number(env.metadata.offset ?? 0);

              // Seed the frontier from the lowest offset seen
              // (reordering-safe) so commits start at the right place on a
              // non-zero resume / rewind.
              tracker.observe(partition, offset);
              tracker.complete(partition, offset);
            }

            const committable = tracker.committable();
            if (committable.size > 0) {
              attemptedOffsets = [...committable.entries()].map(([partition, offset]) => ({
                topic: config.topic,
                partition,
                offset: KafkaOffset(offset.toString()),
              }));

              await config.consumer.commitOffsets(attemptedOffsets);
            }

            return chunk;
          },
          (cause) =>
            new KafkaCommitError({
              cause,
              topic: config.topic,
              offsets: attemptedOffsets,
            }),
        ) as Eff<typeof chunk, Throws<KafkaCommitError>>;
      })
      .flatMap((chunk) => Stream.fromChunk(chunk).map((env) => env.value));
  };
}
