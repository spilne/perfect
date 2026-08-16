// ---------------------------------------------------------------------------
// Branded Kafka identifiers — topic names, group ids, partitions, offsets
// are all confusable primitives (string/string/number/string) that flow
// through the same call sites. Branding makes a swapped argument a compile
// error while staying zero-cost at runtime.
//
// GroupId and PartitionId deliberately REUSE the connect-layer brands
// (ConsumerGroup / Partition) rather than defining Kafka-flavored twins:
// KafkaTopic implements the connect contracts, so its identifiers must be
// assignable both ways without casts.
// ---------------------------------------------------------------------------

import { type Brand, nominal } from "@perfect/core";
import { ConsumerGroup, Partition } from "@perfect/core/connect";

// Constructor signatures are pinned explicitly (runtime-free cast) so the
// exported types don't reference core's unexported BRAND symbol (TS4023).

/** A Kafka topic name. */
export type TopicName = Brand<string, "TopicName">;
export const TopicName = nominal<TopicName>() as (value: string) => TopicName;

/** A Kafka consumer-group id — alias of connect's ConsumerGroup brand. */
export type GroupId = ConsumerGroup;
export const GroupId = ConsumerGroup;

/** A Kafka partition id — alias of connect's Partition brand (integer ≥ 0). */
export type PartitionId = Partition;
export const PartitionId = Partition;

/** A Kafka offset. Kafka offsets are stringly at the driver boundary. */
export type KafkaOffset = Brand<string, "KafkaOffset">;
export const KafkaOffset = nominal<KafkaOffset>() as (value: string) => KafkaOffset;
