// autoCommitBatchWithin — batch envelope acks by count or time window.
//
// Queue-agnostic: operates purely on Envelope.ack(); zero backend knowledge.
// (The Kafka-offset-writing variant, commitBatchWithin, lives in
// @perfect/kafka.)
//
// Ported from promin's kafka/commit-batch-within.ts. The setInterval-based
// timer became Stream.groupWithin — same batch-by-count-or-time semantics,
// but fiber-native and driven by the Clock service (TestClock-controllable).
//
//   topic.subscribeAck()
//     .parEvalMap(25, (env) => process(env.value).map(() => env))
//     .through(autoCommitBatchWithin(500, 15_000))
//     .drain()

import type { Eff } from "../eff";
import { fromPromise } from "../constructors";
import { Stream } from "../stream";
import type { Envelope } from "./contracts";

export function autoCommitBatchWithin<T>(
  maxBatchSize: number,
  maxWaitMs: number,
): (stream: Stream<Envelope<T>, any>) => Stream<T, any> {
  return (stream) =>
    stream
      .groupWithin(maxBatchSize, maxWaitMs)
      .evalMap(
        (chunk) =>
          fromPromise(
            async () => {
              for (const env of chunk) await env.ack();
              return chunk;
            },
            (e) => e,
          ) as Eff<typeof chunk, any>,
      )
      .flatMap((chunk) => Stream.fromChunk(chunk).map((env) => env.value));
}
