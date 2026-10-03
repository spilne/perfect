// AssignmentTracker — which partitions a consumer holds, and who to tell.
//
// Kafka client adapters use this to implement onPartitionsAssigned /
// onPartitionsRevoked. (The kafkajs and platformatic adapters each had their
// own copy of this logic.)
//
// Listener calls run one change at a time: the listeners for one change
// finish before the listeners for the next one start. Messages wait for the
// changes queued before them, so a handler never sees a message from a
// partition before its "assigned" listeners have run.
//
// A listener that throws reports its error once, to the next caller of
// ensureAssigned() or settle(). After that the tracker keeps working. (Before,
// one failing listener left the chain rejected, and every later message
// failed for the life of the consumer.)

import { PartitionId, TopicName } from "./brands.js";
import type { KafkaPartitionAssignment } from "./kafka-types.js";

export type AssignmentListener = (assignment: KafkaPartitionAssignment) => void | Promise<void>;

export class AssignmentTracker {
  private readonly assignedListeners = new Set<AssignmentListener>();
  private readonly revokedListeners = new Set<AssignmentListener>();
  // topic → partitions we hold
  private readonly held = new Map<string, Set<number>>();
  // The listener calls queued so far, one after another. Never rejects.
  private queue: Promise<void> = Promise.resolve();
  // How many changes are queued and not finished yet.
  private queued = 0;
  // A listener error nobody has been told about yet.
  private failure: { readonly cause: unknown } | null = null;

  onAssigned(listener: AssignmentListener): () => void {
    this.assignedListeners.add(listener);
    return () => this.assignedListeners.delete(listener);
  }

  onRevoked(listener: AssignmentListener): () => void {
    this.revokedListeners.add(listener);
    return () => this.revokedListeners.delete(listener);
  }

  /** The group gave this consumer a new set of partitions. */
  assignAll(
    assignments: Iterable<readonly [string, readonly number[]]>,
    generation?: number,
  ): void {
    this.held.clear();
    for (const [topic, partitions] of assignments) {
      this.held.set(topic, new Set(partitions));
      this.notify(this.assignedListeners, topic, partitions, generation);
    }
  }

  /** A rebalance started, or the consumer is closing: we lose everything. */
  revokeAll(): void {
    for (const [topic, partitions] of this.held) {
      this.notify(this.revokedListeners, topic, [...partitions]);
    }
    this.held.clear();
  }

  /**
   * A message arrived from this partition. If we didn't know we held it,
   * tell the "assigned" listeners first. Returns a promise to await before
   * handing the message on, or undefined when there is nothing to wait for
   * (the common case, so a message costs no extra await).
   */
  ensureAssigned(topic: string, partition: number): Promise<void> | undefined {
    let partitions = this.held.get(topic);
    if (partitions === undefined) {
      partitions = new Set();
      this.held.set(topic, partitions);
    }
    if (!partitions.has(partition)) {
      partitions.add(partition);
      this.notify(this.assignedListeners, topic, [partition]);
    }
    if (this.queued === 0 && this.failure === null) return undefined;
    return this.settle();
  }

  /** Wait for every queued listener call. Throws a listener error, once. */
  async settle(): Promise<void> {
    await this.queue;
    const failure = this.failure;
    this.failure = null;
    if (failure !== null) throw failure.cause;
  }

  private notify(
    listeners: ReadonlySet<AssignmentListener>,
    topic: string,
    partitions: readonly number[],
    generation?: number,
  ): void {
    if (partitions.length === 0 || listeners.size === 0) return;
    const assignment: KafkaPartitionAssignment = {
      topic: TopicName(topic),
      partitions: partitions.map(PartitionId),
      generation,
    };
    const callAll = async (): Promise<void> => {
      try {
        await Promise.all([...listeners].map((listener) => listener(assignment)));
      } catch (cause) {
        this.failure ??= { cause };
      } finally {
        this.queued--;
      }
    };
    this.queued++;
    this.queue = this.queue.then(callAll);
  }
}
