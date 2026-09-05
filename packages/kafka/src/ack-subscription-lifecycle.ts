import { OffsetTracker } from "@spilne/perfect-core/connect";
import type {
  Partition,
  PartitionAssignment,
  PartitionLifecycle,
} from "@spilne/perfect-core/connect";
import { KafkaOffset, type TopicName } from "./brands";
import type { KafkaConsumer, KafkaOffsetCommit } from "./kafka-types";

interface SubscriptionOptions {
  readonly consumer: KafkaConsumer;
  readonly topic: TopicName;
  readonly autoCommit: boolean;
}

export class AckSubscriptionLifecycle {
  readonly tracker = new OffsetTracker();
  stopped = false;
  private commitTimer: ReturnType<typeof setInterval> | undefined;
  private flushPromise: Promise<void> | null = null;
  private closePromise: Promise<void> | null = null;
  private pendingCommit: KafkaOffsetCommit[] | null = null;
  private lifecycle: PartitionLifecycle | undefined;
  private readonly activePartitions = new Set<Partition>();
  private activeGeneration: number | undefined;
  private readonly removeAssigned: (() => void) | undefined;
  private readonly removeRevoked: (() => void) | undefined;

  constructor(private readonly options: SubscriptionOptions) {
    this.removeAssigned = options.consumer.onPartitionsAssigned?.(async (assignment) => {
      for (const partition of assignment.partitions) this.activePartitions.add(partition);
      this.activeGeneration = assignment.generation;
      await this.lifecycle?.assigned({
        partitions: assignment.partitions,
        generation: assignment.generation,
      });
    });
    this.removeRevoked = options.consumer.onPartitionsRevoked?.((assignment) =>
      this.revoke(assignment),
    );
  }

  setPartitionLifecycle(lifecycle: PartitionLifecycle): void {
    this.lifecycle = lifecycle;
  }

  startCommitTimer(params: { intervalMs: number; onFailure: (cause: unknown) => void }): void {
    if (!this.options.autoCommit) return;
    this.commitTimer = setInterval(() => {
      void this.flushCommits().catch(params.onFailure);
    }, params.intervalMs);
  }

  flushCommits(): Promise<void> {
    if (!this.options.autoCommit) return Promise.resolve();
    if (this.flushPromise) return this.flushPromise;
    this.flushPromise = (async () => {
      // committable() advances the tracker. Retain that batch until the broker
      // accepts it so a failed flush retries the same offsets.
      if (this.pendingCommit === null) {
        const committable = this.tracker.committable();
        if (committable.size === 0) return;
        this.pendingCommit = [...committable.entries()].map(([partition, nextOffset]) => ({
          topic: this.options.topic,
          partition,
          offset: KafkaOffset(nextOffset.toString()),
        }));
      }
      await this.options.consumer.commitOffsets(this.pendingCommit);
      this.pendingCommit = null;
    })().finally(() => {
      this.flushPromise = null;
    });
    return this.flushPromise;
  }

  private async revoke(assignment: PartitionAssignment): Promise<void> {
    await this.lifecycle?.revoking({
      partitions: assignment.partitions,
      generation: assignment.generation,
    });
    await this.flushCommits();
    for (const partition of assignment.partitions) this.activePartitions.delete(partition);
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.stopped = true;
    if (this.commitTimer) clearInterval(this.commitTimer);
    this.closePromise = (async () => {
      let failure: unknown;
      try {
        if (this.activePartitions.size > 0) {
          await this.revoke({
            partitions: [...this.activePartitions],
            generation: this.activeGeneration,
          });
        } else {
          await this.flushCommits();
        }
      } catch (cause) {
        failure = cause;
      }
      this.removeAssigned?.();
      this.removeRevoked?.();
      try {
        await this.options.consumer.disconnect();
      } catch (cause) {
        if (failure === undefined) failure = cause;
      }
      if (failure !== undefined) throw failure;
    })();
    return this.closePromise;
  }
}
