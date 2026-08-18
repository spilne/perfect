// Queue-agnostic endpoint contracts — the "any queue" layer.
//
// Every messaging backend (Kafka, Redis Streams, in-memory, …) implements
// these opt-in capability interfaces; consumers like StreamTopology require
// only what they use (e.g. `Streamable & Acknowledgeable`). Ported from
// promin's typeclasses/streamable.ts with StreamPipeline → Stream.
//
// Two queue shapes:
//   log-shaped    (Kafka, Redis Streams, Kinesis): numeric offset per
//                 partition, replayable — implements Partitionable /
//                 Replayable / Checkpointable on top of the base three.
//   broker-shaped (RabbitMQ, SQS, NATS core): opaque per-message ack token —
//                 just Streamable + Sinkable + Acknowledgeable.

import { type Brand, nominal, refined } from "../brand";
import type { Eff } from "../eff";
import type { Stream } from "../stream";
import type { Codec } from "./codec";

// ── Branded identifiers — shared across all backends ───────────────
//
// These are the queue-agnostic confusable identifiers: every backend has
// consumer groups and (log-shaped ones) partitions, and the distributed
// layer names repartition channels. Backends reuse these brands (kafka's
// GroupId IS ConsumerGroup, its PartitionId IS Partition) rather than
// defining lookalikes — cross-package call sites stay assignable.

// Constructor signatures are pinned explicitly (the cast is runtime-free):
// exporting the inferred `(value: Unbrand<A>) => A` type would reference
// brand.ts's unexported BRAND symbol in declaration emit (TS4023).

/** A consumer-group identifier — backend-agnostic. */
export type ConsumerGroup = Brand<string, "ConsumerGroup">;
export const ConsumerGroup = nominal<ConsumerGroup>() as (value: string) => ConsumerGroup;

/** A partition identifier (log-shaped backends). Integer ≥ 0. */
export type Partition = Brand<number, "Partition">;
export const Partition = refined<Partition>(
  (n) => Number.isInteger(n) && n >= 0,
  (n) => `Partition must be a non-negative integer, got ${n}`,
) as (value: number) => Partition;

/** A repartition-channel name (shuffle transport). */
export type ChannelName = Brand<string, "ChannelName">;
export const ChannelName = nominal<ChannelName>() as (value: string) => ChannelName;

export type TopologyId = Brand<string, "TopologyId">;
export const TopologyId = nominal<TopologyId>() as (value: string) => TopologyId;

export type StageId = Brand<string, "StageId">;
export const StageId = nominal<StageId>() as (value: string) => StageId;

export type TopologyInstanceId = Brand<string, "TopologyInstanceId">;
export const TopologyInstanceId = nominal<TopologyInstanceId>() as (
  value: string,
) => TopologyInstanceId;

export type SourceRecordId = Brand<string, "SourceRecordId">;
export const SourceRecordId = nominal<SourceRecordId>() as (value: string) => SourceRecordId;

export type StateCheckpointId = Brand<string, "StateCheckpointId">;
export const StateCheckpointId = nominal<StateCheckpointId>() as (
  value: string,
) => StateCheckpointId;

export type LeaseEpoch = Brand<number, "LeaseEpoch">;
export const LeaseEpoch = refined<LeaseEpoch>(
  (value) => Number.isSafeInteger(value) && value > 0,
  (value) => `LeaseEpoch must be a positive safe integer, got ${value}`,
) as (value: number) => LeaseEpoch;

// ── Streamable<T> — "I can produce a stream of T" ──────────────────

export interface Streamable<T, S = never> {
  subscribe(params?: { group?: ConsumerGroup }): Stream<T, S>;
  codec: Codec<T>;
}

export function isStreamable<T, S = never>(value: unknown): value is Streamable<T, S> {
  return (
    value !== null &&
    typeof value === "object" &&
    "subscribe" in value &&
    typeof (value as any).subscribe === "function" &&
    "codec" in value
  );
}

// ── Sinkable<T> — "I can consume values of T" ──────────────────────

export interface Sinkable<T, S = never> {
  publish(value: T): Eff<void, S>;
  codec: Codec<T>;
}

export interface TransactionalSinkable<T, S = never, Transaction = unknown> extends Sinkable<T, S> {
  readonly transactionDomain: object;
  publishInTransaction(transaction: Transaction, value: T): Promise<void>;
}

export function isSinkable<T, S = never>(value: unknown): value is Sinkable<T, S> {
  return (
    value !== null &&
    typeof value === "object" &&
    "publish" in value &&
    typeof (value as any).publish === "function" &&
    "codec" in value
  );
}

export function isTransactionalSinkable<T, S = never, Transaction = unknown>(
  value: unknown,
): value is TransactionalSinkable<T, S, Transaction> {
  return (
    isSinkable(value) &&
    "transactionDomain" in value &&
    typeof (value as any).transactionDomain === "object" &&
    (value as any).transactionDomain !== null &&
    "publishInTransaction" in value &&
    typeof (value as any).publishInTransaction === "function"
  );
}

// ── KeyedSinkable<T> — "I can route by key" ────────────────────────

export interface KeyedSinkable<T, S = never> extends Sinkable<T, S> {
  publish(value: T, params?: { key: string }): Eff<void, S>;
}

export function isKeyedSinkable<T, S = never>(value: unknown): value is KeyedSinkable<T, S> {
  return isSinkable(value);
}

// ── Partitionable<T> — "I have partitions" ─────────────────────────

export interface Partitionable<T, S = never> extends Streamable<T, S> {
  /** Partition COUNT — a cardinality, not an identifier, so plain number. */
  partitions: number;
  subscribe(params?: { group?: ConsumerGroup; partitions?: Partition[] }): Stream<T, S>;
}

export function isPartitionable<T, S = never>(value: unknown): value is Partitionable<T, S> {
  return (
    isStreamable(value) && "partitions" in value && typeof (value as any).partitions === "number"
  );
}

// ── Replayable<T> — "I can seek to a point in time" ────────────────

// `specific.value` stays a PLAIN string on purpose: it is backend-opaque.
// A Kafka offset is a stringly integer, a Redis Streams id is "1526919-0",
// a Kinesis sequence number is something else again — branding it here
// would impose one backend's semantics on all of them. Backends may brand
// their own offset representation internally (kafka's KafkaOffset does).
export type Offset =
  | { type: "earliest" }
  | { type: "latest" }
  | { type: "timestamp"; value: number }
  | { type: "specific"; value: string };

export interface Replayable<T, S = never> extends Streamable<T, S> {
  subscribeFrom(params: { offset: Offset; group?: ConsumerGroup }): Stream<T, S>;
}

export function isReplayable<T, S = never>(value: unknown): value is Replayable<T, S> {
  return (
    isStreamable(value) &&
    "subscribeFrom" in value &&
    typeof (value as any).subscribeFrom === "function"
  );
}

// ── Acknowledgeable<T> — "I support manual ack/nack" ───────────────

export interface Envelope<T, S = never> {
  readonly value: T;
  ack(): Eff<void, S>;
  nack(): Eff<void, S>;
  readonly metadata: Record<string, unknown>;
}

export interface TransactionalEnvelope<T, S = never, Transaction = unknown> extends Envelope<T, S> {
  readonly transactionDomain: object;
  ackInTransaction(transaction: Transaction): Promise<void>;
}

export function isTransactionalEnvelope<T, S = never, Transaction = unknown>(
  value: Envelope<T, S>,
): value is TransactionalEnvelope<T, S, Transaction> {
  return (
    "transactionDomain" in value &&
    typeof (value as any).transactionDomain === "object" &&
    (value as any).transactionDomain !== null &&
    "ackInTransaction" in value &&
    typeof (value as any).ackInTransaction === "function"
  );
}

export interface AcknowledgeOptions {
  readonly group?: ConsumerGroup;
  readonly offset?: Offset;
}

export interface Acknowledgeable<T, S = never> extends Streamable<T, S> {
  subscribeAck(params?: AcknowledgeOptions): Stream<Envelope<T, S>, S>;
}

export interface PartitionAssignment {
  readonly partitions: readonly Partition[];
  readonly generation?: number;
}

export interface PartitionLifecycle {
  assigned(assignment: PartitionAssignment): Promise<void>;
  revoking(assignment: PartitionAssignment): Promise<void>;
}

export interface ManagedAcknowledgementSubscription<T, S = never> {
  readonly stream: Stream<Envelope<T, S>, S>;
  setPartitionLifecycle(lifecycle: PartitionLifecycle): void;
  close(): Promise<void>;
}

export interface ManagedAcknowledgeable<T, S = never> extends Acknowledgeable<T, S> {
  subscribeAckManaged(params?: AcknowledgeOptions): ManagedAcknowledgementSubscription<T, S>;
}

export function isAcknowledgeable<T, S = never>(value: unknown): value is Acknowledgeable<T, S> {
  return (
    isStreamable(value) &&
    "subscribeAck" in value &&
    typeof (value as any).subscribeAck === "function"
  );
}

export function isManagedAcknowledgeable<T, S = never>(
  value: unknown,
): value is ManagedAcknowledgeable<T, S> {
  return (
    isAcknowledgeable(value) &&
    "subscribeAckManaged" in value &&
    typeof (value as any).subscribeAckManaged === "function"
  );
}

// ── Checkpointable<T> — "I can save/restore consumption position" ──

export interface Checkpointable<T, S = never> extends Streamable<T, S> {
  // `offset` is plain here for the same backend-opacity reason as
  // Offset.specific.value above.
  commitOffset(params: { group: ConsumerGroup; offset: string }): Promise<void>;
  getCommittedOffset(params: { group: ConsumerGroup }): Promise<string | null>;
}

export function isCheckpointable<T, S = never>(value: unknown): value is Checkpointable<T, S> {
  return (
    isStreamable(value) &&
    "commitOffset" in value &&
    typeof (value as any).commitOffset === "function" &&
    "getCommittedOffset" in value &&
    typeof (value as any).getCommittedOffset === "function"
  );
}

// ── LeaderElection — "only one coordinator runs at a time" ─────────
//
// Coordination capability for distributed backends (Postgres advisory
// locks, Redis locks, etcd leases, …). Ported from promin's workflow
// leader-election interface. This remains a Promise-based driver boundary;
// the Eff layer wraps it at call sites.

export interface LeaderElection {
  /** Try to become the leader. Returns true if this instance is now the leader. */
  tryAcquire(): Promise<boolean>;
  /** Release leadership. */
  release(): Promise<void>;
}

// ── ShuffleTransport — repartition channels for distributed runs ───
//
// Lives here (not in @perfect/kafka or @perfect/topology) to break the
// dependency cycle: kafka IMPLEMENTS it, topology CONSUMES it.

export interface ShuffleTransport<T = unknown, S = never> {
  getOrCreateRepartitionChannel(params: {
    name: ChannelName;
    group: ConsumerGroup;
    codec: Codec<T>;
  }): Promise<{
    source: Streamable<T, S> & Acknowledgeable<T, S>;
    sink: KeyedSinkable<T, S>;
  }>;
}
