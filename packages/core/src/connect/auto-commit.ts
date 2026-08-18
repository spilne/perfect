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

import { TaggedError } from "../tagged-error";
import type { Eff, Throws } from "../eff";
import { fromPromise } from "../constructors";
import { Stream } from "../stream";
import type { Envelope } from "./contracts";

export class AckError extends TaggedError("AckError")<{
  readonly cause: unknown;
}>() {}

export function autoCommitBatchWithin<T>(
  maxBatchSize: number,
  maxWaitMs: number,
): <S>(stream: Stream<Envelope<T>, S>) => Stream<T, S | Throws<AckError>> {
  return <S>(stream: Stream<Envelope<T>, S>) =>
    stream
      .groupWithin(maxBatchSize, maxWaitMs)
      .evalMap(
        (chunk) =>
          fromPromise(
            async () => {
              for (const env of chunk) await env.ack();
              return chunk;
            },
            (cause) => new AckError({ cause }),
          ) as Eff<typeof chunk, Throws<AckError>>,
      )
      .flatMap((chunk) => Stream.fromChunk(chunk).map((env) => env.value));
}
