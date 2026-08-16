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

// ── Streamable<T> — "I can produce a stream of T" ──────────────────

export interface Streamable<T> {
  subscribe(params?: { group?: ConsumerGroup }): Stream<T, never>;
  codec: Codec<T>;
}

export function isStreamable<T>(value: unknown): value is Streamable<T> {
  return (
    value !== null &&
    typeof value === "object" &&
    "subscribe" in value &&
    typeof (value as any).subscribe === "function" &&
    "codec" in value
  );
}

// ── Sinkable<T> — "I can consume values of T" ──────────────────────

export interface Sinkable<T> {
  publish(value: T): Promise<void>;
  codec: Codec<T>;
}

export function isSinkable<T>(value: unknown): value is Sinkable<T> {
  return (
    value !== null &&
    typeof value === "object" &&
    "publish" in value &&
    typeof (value as any).publish === "function" &&
    "codec" in value
  );
}

// ── KeyedSinkable<T> — "I can route by key" ────────────────────────

export interface KeyedSinkable<T> extends Sinkable<T> {
  publish(value: T, params?: { key: string }): Promise<void>;
}

export function isKeyedSinkable<T>(value: unknown): value is KeyedSinkable<T> {
  return isSinkable(value);
}

// ── Partitionable<T> — "I have partitions" ─────────────────────────

export interface Partitionable<T> extends Streamable<T> {
  /** Partition COUNT — a cardinality, not an identifier, so plain number. */
  partitions: number;
  subscribe(params?: { group?: ConsumerGroup; partitions?: Partition[] }): Stream<T, never>;
}

export function isPartitionable<T>(value: unknown): value is Partitionable<T> {
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

export interface Replayable<T> extends Streamable<T> {
  subscribeFrom(params: { offset: Offset; group?: ConsumerGroup }): Stream<T, never>;
}

export function isReplayable<T>(value: unknown): value is Replayable<T> {
  return (
    isStreamable(value) &&
    "subscribeFrom" in value &&
    typeof (value as any).subscribeFrom === "function"
  );
}

// ── Acknowledgeable<T> — "I support manual ack/nack" ───────────────

export interface Envelope<T> {
  readonly value: T;
  ack(): Promise<void>;
  nack(): Promise<void>;
  readonly metadata: Record<string, unknown>;
}

export interface Acknowledgeable<T> extends Streamable<T> {
  subscribeAck(params?: { group?: ConsumerGroup }): Stream<Envelope<T>, never>;
}

export function isAcknowledgeable<T>(value: unknown): value is Acknowledgeable<T> {
  return (
    isStreamable(value) &&
    "subscribeAck" in value &&
    typeof (value as any).subscribeAck === "function"
  );
}

// ── Checkpointable<T> — "I can save/restore consumption position" ──

export interface Checkpointable<T> extends Streamable<T> {
  // `offset` is plain here for the same backend-opacity reason as
  // Offset.specific.value above.
  commitOffset(params: { group: ConsumerGroup; offset: string }): Promise<void>;
  getCommittedOffset(params: { group: ConsumerGroup }): Promise<string | null>;
}

export function isCheckpointable<T>(value: unknown): value is Checkpointable<T> {
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
// leader-election interface. Promise-based like Envelope.ack — this is a
// driver-facing boundary; the Eff layer wraps at call sites.

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

export interface ShuffleTransport<T = unknown> {
  getOrCreateRepartitionChannel(params: {
    name: ChannelName;
    group: ConsumerGroup;
    codec: Codec<T>;
  }): Promise<{
    source: Streamable<T> & Acknowledgeable<T>;
    sink: KeyedSinkable<T>;
  }>;
}
