import { TaggedError } from "@spilne/perfect-core";
import type { TopicName } from "./brands.js";
import type { KafkaOffsetCommit } from "./kafka-types.js";

export class KafkaCommitError extends TaggedError("KafkaCommitError")<{
  readonly cause: unknown;
  readonly topic: TopicName;
  readonly offsets: readonly KafkaOffsetCommit[];
}>() {}

export class KafkaError extends TaggedError("KafkaError")<{
  readonly operation: string;
  readonly topic: TopicName;
  readonly cause: unknown;
}>() {}

export function toKafkaError(operation: string, topic: TopicName, cause: unknown): KafkaError {
  return cause instanceof KafkaError ? cause : new KafkaError({ operation, topic, cause });
}
