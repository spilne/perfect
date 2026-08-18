import { TaggedError } from "@perfect/core";
import type { TopicName } from "./brands";
import type { KafkaOffsetCommit } from "./kafka-types";

export class KafkaCommitError extends TaggedError("KafkaCommitError")<{
  readonly cause: unknown;
  readonly topic: TopicName;
  readonly offsets: readonly KafkaOffsetCommit[];
}>() {}
