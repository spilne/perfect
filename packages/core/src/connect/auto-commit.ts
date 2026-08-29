// autoCommitBatchWithin — batch envelope acks by count or time window.
//
// Queue-agnostic: operates purely on Envelope.ack(); zero backend knowledge.
// (The Kafka-offset-writing variant, commitBatchWithin, lives in
// @spilne/perfect-kafka.)
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
import { succeed } from "../constructors";
import { TaggedError } from "../tagged-error";
import { Stream } from "../stream";
import type { Pipe } from "../stream";
import type { Envelope } from "./contracts";

export class AckError extends TaggedError("AckError")<{
  readonly cause: unknown;
}>() {}

export function autoCommitBatchWithin<T, AckS = never>(
  maxBatchSize: number,
  maxWaitMs: number,
): Pipe<Envelope<T, AckS>, T, AckS> {
  return <S>(stream: Stream<Envelope<T, AckS>, S>) =>
    stream
      .groupWithin(maxBatchSize, maxWaitMs)
      .evalMap((chunk) =>
        Array.from(chunk)
          .reduce<Eff<void, AckS>>(
            (effect, envelope) => effect.flatMap(() => envelope.ack()),
            succeed(undefined),
          )
          .map(() => chunk),
      )
      .flatMap((chunk) => Stream.fromChunk(chunk).map((env) => env.value));
}
