import {
  type Eff,
  type ExitT,
  type Fiber,
  type Throws,
  Cause,
  Exit,
  fromPromise,
  runFiber,
  succeed,
  sync,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  CheckpointName,
  InMemoryPartitionedState,
  Partition,
  SourceRecordId,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
  isManagedAcknowledgeable,
  isTransactionalEnvelope,
  isTransactionalPartitionedStateBackend,
  isTransactionalSinkable,
  type Acknowledgeable,
  type Envelope,
  type ManagedAcknowledgementSubscription,
  type PartitionedStateBackend,
  type PartitionStateCommit,
  type PartitionStateSnapshot,
  type Sinkable,
  type StateMutation,
  type StatePartitionLease,
  type StatePartitionScope,
  type Streamable,
  type TransactionalPartitionedStateBackend,
  type TransactionalSinkable,
} from "@spilne/perfect-core/connect";
import type { StateBackend } from "./state-backend.js";
import { BuiltTopology } from "./stream-topology.js";
import { WindowManager } from "./window-manager.js";
import { JoinBuffer } from "./join-buffer.js";
import { PartitionLifecycle, type PartitionContext } from "./partition-lifecycle.js";
import type {
  TopologyConfig,
  TopologyHandle,
  TopologyMetrics,
  TopologyNode,
} from "./types.js";

export class TopologyRunner {
  static async run(topology: BuiltTopology, config: TopologyConfig): Promise<TopologyHandle> {
    return new TopologyRunnerInstance(topology, config).start();
  }
}

class InsertionOrderSet {
  private readonly set = new Set<string>();
  private readonly order: string[] = [];

  constructor(private readonly maxSize: number) {}

  has(key: string): boolean {
    return this.set.has(key);
  }

  add(key: string): string | undefined {
    if (this.set.has(key)) return undefined;
    this.set.add(key);
    this.order.push(key);
    if (this.set.size <= this.maxSize) return undefined;
    const oldest = this.order.shift()!;
    this.set.delete(oldest);
    return oldest;
  }

  get size(): number {
    return this.set.size;
  }
}

class RateLimiter {
  private tokens: number;
  private lastRefill = Date.now();

  constructor(private readonly maxPerSecond: number) {
    this.tokens = maxPerSecond;
  }

  async acquire(): Promise<void> {
    while (true) {
      const now = Date.now();
      const elapsed = (now - this.lastRefill) / 1000;
      this.tokens = Math.min(this.maxPerSecond, this.tokens + elapsed * this.maxPerSecond);
      this.lastRefill = now;
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      const waitMs = Math.ceil(((1 - this.tokens) / this.maxPerSecond) * 1000);
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
}

interface RecordCompletion {
  pending: number;
  readonly context: PartitionContext;
  readonly mutations: Map<string, StateMutation<unknown>>;
  readonly envelope?: Envelope<unknown, unknown>;
  readonly sourceId?: SourceRecordId;
  readonly sourceOffset?: string;
  readonly knownDuplicate: boolean;
  /** This record put its sourceId in the context's inflightSources. */
  readonly tracksSource: boolean;
  readonly outputs: { readonly sink: Sinkable<unknown, unknown>; readonly value: unknown }[];
}

interface TopologyRecord {
  readonly value: unknown;
  readonly partition: Partition;
  readonly completion: RecordCompletion;
  readonly skip: boolean;
  /** Set by an eventTime() step; otherwise read from the value when needed. */
  readonly eventTime?: number;
  /**
   * A marker sent after the last record of a source that has ended: window
   * steps emit what they still have open. It is skipped by every other step.
   */
  readonly endOfInput?: boolean;
}

/** A window step's working state for one partition. */
class WindowOperatorState {
  /** Newest event time seen in the partition; the watermark follows it. */
  newestEventTime = -Infinity;

  constructor(readonly manager: WindowManager<any, any, any>) {}
}

class TopologyRunnerInstance {
  private readonly topologyId: TopologyId;
  private readonly stageId: StageId;
  private readonly instanceId: TopologyInstanceId;
  private readonly leaseMs: number;
  private readonly stateBackend: PartitionedStateBackend<unknown>;
  private readonly legacyStateBackend?: StateBackend<string, unknown>;
  private readonly partitionLifecycle: PartitionLifecycle;
  private readonly operatorIds = new Map<TopologyNode, string>();
  private readonly operatorCounts = new Map<string, number>();
  private readonly managedSubscriptions: ManagedAcknowledgementSubscription<unknown, unknown>[] =
    [];

  private running = true;
  private stopping = false;
  private checkpointInterval: ReturnType<typeof setInterval> | null = null;
  private leaseInterval: ReturnType<typeof setInterval> | null = null;
  private fibers: Fiber<void>[] = [];
  private drainPromise: Promise<readonly ExitT<unknown, void>[]> = Promise.resolve([]);
  private checkpointInFlight: Promise<void> | null = null;
  private checkpointFailure: unknown;
  private shutdownPromise: Promise<void> | null = null;
  private checkpointSequence = 0;

  private itemsProcessed = 0;
  // How many sources the topology reads, and how many have ended.
  private sources = 0;
  private endedSources = 0;
  private lateRecords = 0;
  private readonly metricsStartTime = Date.now();
  private readonly rateLimiter: RateLimiter | null;

  constructor(
    private readonly topology: BuiltTopology,
    private readonly config: TopologyConfig,
  ) {
    validateTopologyConfig(config);
    this.topologyId = config.topologyId ?? TopologyId(config.group);
    this.stageId = config.stageId ?? StageId("stage-0");
    this.instanceId = config.instanceId ?? TopologyInstanceId(crypto.randomUUID());
    this.leaseMs = config.partitionLeaseMs ?? 30_000;
    this.legacyStateBackend = config.stateBackend;
    this.stateBackend =
      config.partitionedStateBackend ??
      (config.stateBackend
        ? new LegacyPartitionedStateBackend(config.stateBackend, config.processedRetentionMs)
        : new InMemoryPartitionedState({ processedRetentionMs: config.processedRetentionMs }));
    if (
      config.deliveryGuarantee === "exactly-once" &&
      !isTransactionalPartitionedStateBackend(this.stateBackend)
    ) {
      throw new TypeError("exactly-once delivery requires a transactional partitionedStateBackend");
    }
    this.partitionLifecycle = new PartitionLifecycle({
      topologyId: this.topologyId,
      stageId: this.stageId,
      instanceId: this.instanceId,
      leaseMs: this.leaseMs,
      stateBackend: this.stateBackend,
      nextCheckpointId: () =>
        StateCheckpointId(`${this.instanceId}:revoke:${++this.checkpointSequence}`),
    });
    this.rateLimiter = config.maxItemsPerSecond ? new RateLimiter(config.maxItemsPerSecond) : null;
  }

  async start(): Promise<TopologyHandle> {
    if (this.legacyStateBackend) {
      await this.legacyStateBackend.restore({
        name: CheckpointName(`topology:${this.config.group}`),
      });
    }

    const drains: Eff<void, unknown>[] = [];
    if (this.topology.compiled.sinks.length > 0) {
      for (const sink of this.topology.compiled.sinks) {
        let pipeline = this.compile(sink.parent);
        const sinkTarget = sink.sink as Sinkable<unknown, unknown>;
        this.validateSink(sinkTarget);
        if (this.config.maxBufferSize) pipeline = pipeline.buffer(this.config.maxBufferSize);

        drains.push(pipeline.evalMap((record) => this.deliverRecord(record, sinkTarget)).drain());
      }
    } else {
      const terminal = this.topology.compiled.nodes[this.topology.compiled.nodes.length - 1]!;
      let pipeline = this.compile(terminal);
      if (this.config.maxBufferSize) pipeline = pipeline.buffer(this.config.maxBufferSize);
      drains.push(pipeline.evalMap((record) => this.deliverRecord(record)).drain());
    }

    this.fibers = drains.map((drain) => runFiber((drain as Eff<void, Throws<unknown>>).orDie()));
    const exits = this.fibers.map((fiber, index) =>
      fiber.await().then((exit) => {
        if (!this.stopping && exit._tag === "Failure" && !Cause.isInterruptedOnly(exit.cause)) {
          this.running = false;
          for (let i = 0; i < this.fibers.length; i++) {
            if (i !== index) this.fibers[i]!.interrupt();
          }
        }
        return exit;
      }),
    );
    this.drainPromise = Promise.all(exits).then((completed) => {
      this.running = false;
      return completed;
    });

    this.leaseInterval = setInterval(
      () => void this.partitionLifecycle.renew().catch((error) => this.failBackground(error)),
      Math.max(1, Math.floor(this.leaseMs / 3)),
    );
    if (this.config.checkpointIntervalMs) {
      this.checkpointInterval = setInterval(
        () => this.startCheckpoint(),
        this.config.checkpointIntervalMs,
      );
    }

    return {
      shutdown: () => this.shutdown(),
      awaitExit: () => this.awaitExit(),
      isRunning: () => this.running,
      metrics: () => this.getMetrics(),
    };
  }

  private compile(node: TopologyNode): Stream<TopologyRecord, any> {
    switch (node.type) {
      case "source":
        return this.compileSource(node);
      case "map":
        return this.compile(node.parent).map((record) =>
          record.skip ? record : this.withValue(record, node.fn(record.value)),
        );
      case "filter":
        return this.compile(node.parent).map((record) =>
          record.skip || node.fn(record.value) ? record : this.skipped(record),
        );
      case "mapAsync":
        return this.compile(node.parent).parEvalMap(node.concurrency, (record) =>
          record.skip
            ? succeed(record)
            : fromPromise(
                () => node.fn(record.value),
                (error) => error,
              ).map((value) => this.withValue(record, value)),
        );
      case "eventTime":
        return this.compile(node.parent).map((record) => {
          if (record.skip) return record;
          const eventTime = node.fn(record.value);
          if (!Number.isFinite(eventTime)) {
            throw new TypeError(
              `eventTime() must return milliseconds as a finite number, got ${String(eventTime)}`,
            );
          }
          return { ...record, eventTime };
        });
      case "keyBy":
      case "shuffle":
      case "window":
        return this.compile(node.parent);
      case "aggregate":
        return this.compileAggregate(node);
      case "process":
        return this.compileProcess(node);
      case "dedupe":
        return this.compileDedupe(node);
      case "join":
        return this.compileJoin(node);
      case "sink":
        return this.compile(node.parent);
    }
  }

  private compileSource(node: { source: unknown }): Stream<TopologyRecord, any> {
    const source = node.source as Streamable<unknown, any> & Acknowledgeable<unknown, any>;
    let envelopes: Stream<Envelope<unknown, unknown>, any>;

    if (isManagedAcknowledgeable(source)) {
      const subscription = source.subscribeAckManaged({ group: this.config.group });
      // The source is closed only after processing has ended (finished,
      // failed or interrupted). Partitions it gives up then have nothing in
      // flight that can still finish, so their revocation must not wait.
      let closing = false;
      subscription.setPartitionLifecycle({
        assigned: async ({ partitions }) => {
          await Promise.all(
            partitions.map((partition) => this.partitionLifecycle.activate(partition)),
          );
        },
        revoking: async ({ partitions }) => {
          await Promise.all(
            partitions.map((partition) =>
              this.partitionLifecycle.revoke(partition, { waitForInflight: !closing }),
            ),
          );
        },
      });
      this.managedSubscriptions.push(
        subscription as ManagedAcknowledgementSubscription<unknown, unknown>,
      );
      envelopes = subscription.stream.onFinalize(
        fromPromise(
          () => {
            closing = true;
            return subscription.close();
          },
          (error) => error,
        ),
      );
    } else {
      envelopes = source.subscribeAck({ group: this.config.group });
    }

    const managed = isManagedAcknowledgeable(source);
    this.sources += 1;
    return envelopes
      .evalMap((envelope) =>
        fromPromise(
          () => this.prepareEnvelope(envelope, managed),
          (error) => error,
        ),
      )
      .collect((record) => record)
      .concat(Stream.suspend(() => Stream.fromArray(this.endOfInputMarkers())));
  }

  /**
   * Called when a source has no more records. Once every source of the
   * topology has ended, send one end marker per partition so window steps
   * emit what they still have open. (A source stopped by shutdown or a
   * failure never gets here: it is interrupted, not ended.)
   */
  private endOfInputMarkers(): TopologyRecord[] {
    this.endedSources += 1;
    if (this.endedSources < this.sources) return [];
    return [...this.partitionLifecycle.contexts].map(([partition, context]) => {
      context.inflight += 1;
      return {
        value: undefined,
        partition,
        skip: true,
        endOfInput: true,
        completion: {
          pending: 1,
          context,
          mutations: new Map(),
          knownDuplicate: false,
          tracksSource: false,
          outputs: [],
        },
      };
    });
  }

  private compileProcess(
    node: Extract<TopologyNode, { type: "process" }>,
  ): Stream<TopologyRecord, any> {
    const keyFn = this.findKeyBy(node.parent).keyFn;
    const operatorId = this.operatorId(node, "process");
    const legacyIndex = Number(operatorId.split(":")[1]);

    return this.compile(node.parent).map((record) => {
      if (record.skip) return record;
      const context = record.completion.context;
      const key = keyFn(record.value);
      const stateKey = `${operatorId}:key:${encodeURIComponent(key)}`;
      let current = context.values.get(stateKey);
      if (current === undefined) {
        const legacy = context.values.get(`process-map:${legacyIndex}`);
        if (Array.isArray(legacy)) {
          current = (legacy as [string, unknown][]).find(([candidate]) => candidate === key)?.[1];
        }
      }
      const result = node.spec.process(current ?? node.spec.init(), record.value);
      this.putMutation(record, stateKey, result.state);
      return result.emit === undefined ? this.skipped(record) : this.withValue(record, result.emit);
    });
  }

  private compileAggregate(
    node: Extract<TopologyNode, { type: "aggregate" }>,
  ): Stream<TopologyRecord, any> {
    const { window, keyFn } = this.findWindowAndKey(node.parent);
    const lateness = window.allowedLatenessMs ?? 0;
    const operatorId = this.operatorId(node, "window");

    // Each key's windows are saved under their own state entry, so a record
    // only rewrites the keys whose windows changed.
    const windowsEntry = (key: string) => `${operatorId}:windows:${encodeURIComponent(key)}`;
    const windowsPrefix = `${operatorId}:windows:`;
    // The newest event time the partition has seen, saved so that a restart
    // doesn't reopen windows that were already emitted.
    const newestEntry = `${operatorId}:newest-event-time`;
    // Partitions restored from the old single-entry format. Their first
    // record writes every key in the new format, removes the old entry and
    // sets a marker. The marker matters because the old entry can sit where
    // a commit can't delete it (the store's root, for partition 0); after
    // the marker it is ignored, so flushed windows can't come back from it.
    const migrating = new WeakSet<PartitionContext>();
    const migratedMarker = `${operatorId}:windows-migrated`;

    const windowsOf = (context: PartitionContext): WindowOperatorState => {
      let state = context.operatorCaches.get(operatorId) as WindowOperatorState | undefined;
      if (state) return state;
      state = new WindowOperatorState(new WindowManager(window.windowType, node.spec));
      const legacy = context.values.get(operatorId);
      if (Array.isArray(legacy) && context.values.get(migratedMarker) !== true) {
        state.manager.restore(legacy as any);
        migrating.add(context);
      }
      for (const [entryKey, saved] of context.values) {
        if (entryKey.startsWith(windowsPrefix) && Array.isArray(saved)) {
          state.manager.restore(saved as any);
        }
      }
      const newest = context.values.get(newestEntry);
      if (typeof newest === "number") state.newestEventTime = newest;
      context.operatorCaches.set(operatorId, state);
      return state;
    };

    const saveChangedWindows = (record: TopologyRecord, manager: WindowManager<any, any, any>) => {
      const context = record.completion.context;
      const changed = new Set(manager.takeChangedKeys());
      if (migrating.delete(context)) {
        for (const openKey of manager.keys()) changed.add(openKey);
        this.deleteMutation(record, operatorId);
        this.putMutation(record, migratedMarker, true);
      }
      for (const key of changed) {
        const windows = manager.snapshotKey(key);
        if (windows.length > 0) this.putMutation(record, windowsEntry(key), windows);
        else this.deleteMutation(record, windowsEntry(key));
      }
    };

    return this.compile(node.parent).flatMap((record) => {
      if (record.endOfInput) {
        // No more input: every open window is complete.
        const { manager } = windowsOf(record.completion.context);
        const outputs = manager.flushAll();
        saveChangedWindows(record, manager);
        return Stream.fromArray(this.emitBeforeEnd(record, outputs));
      }
      if (record.skip) return Stream.fromArray([record]);

      const state = windowsOf(record.completion.context);
      const time = this.timeOf(record);
      if (state.manager.isLate(time, state.newestEventTime - lateness)) {
        this.lateRecords += 1;
        return Stream.fromArray([this.skipped(record)]);
      }

      state.manager.add(keyFn(record.value), record.value, time);
      if (time > state.newestEventTime) {
        state.newestEventTime = time;
        this.putMutation(record, newestEntry, time);
      }
      // Close the windows of every key, not just this record's, that ended
      // before the watermark.
      const outputs = state.manager.close(state.newestEventTime - lateness);
      saveChangedWindows(record, state.manager);
      return Stream.fromArray(this.branch(record, outputs));
    });
  }

  private compileDedupe(
    node: Extract<TopologyNode, { type: "dedupe" }>,
  ): Stream<TopologyRecord, any> {
    const operatorId = this.operatorId(node, "dedupe");
    const maxSize = this.config.maxDedupeSize ?? 100_000;

    return this.compile(node.parent).map((record) => {
      if (record.skip) return record;
      const context = record.completion.context;
      let seen = context.operatorCaches.get(operatorId) as InsertionOrderSet | undefined;
      if (!seen) {
        seen = new InsertionOrderSet(maxSize);
        const prefix = `${operatorId}:item:`;
        for (const key of context.values.keys()) {
          if (key.startsWith(prefix)) seen.add(decodeURIComponent(key.slice(prefix.length)));
        }
        context.operatorCaches.set(operatorId, seen);
      }

      const key = node.keyFn(record.value);
      if (seen.has(key)) return this.skipped(record);
      const evicted = seen.add(key);
      this.putMutation(record, `${operatorId}:item:${encodeURIComponent(key)}`, true);
      if (evicted !== undefined) {
        this.deleteMutation(record, `${operatorId}:item:${encodeURIComponent(evicted)}`);
      }
      return record;
    });
  }

  private compileJoin(node: Extract<TopologyNode, { type: "join" }>): Stream<TopologyRecord, any> {
    const operatorId = this.operatorId(node, "join");
    // Each key's buffered items are saved under their own state entry.
    const keyPrefix = `${operatorId}:key:`;
    const keyEntry = (key: string) => `${keyPrefix}${encodeURIComponent(key)}`;
    const migrating = new WeakSet<PartitionContext>();
    const leftKeyFn = this.findKeyBy(node.left).keyFn;
    const rightKeyFn = this.findKeyBy(node.right).keyFn;

    type Tagged = { record: TopologyRecord; side: "left" | "right"; key: string; ts: number };
    const left = this.compile(node.left).map((record): Tagged => ({
      record,
      side: "left",
      key: leftKeyFn(record.value),
      ts: this.timeOf(record),
    }));
    const right = this.compile(node.right).map((record): Tagged => ({
      record,
      side: "right",
      key: rightKeyFn(record.value),
      ts: this.timeOf(record),
    }));

    return left.merge(right).flatMap((tagged) => {
      const record = tagged.record;
      if (record.skip) return Stream.fromArray([record]);
      const context = record.completion.context;
      let buffer = context.operatorCaches.get(operatorId) as
        | JoinBuffer<unknown, unknown>
        | undefined;
      if (!buffer) {
        buffer = new JoinBuffer(node.config.windowMs);
        for (const [entryKey, saved] of context.values) {
          if (entryKey.startsWith(keyPrefix)) {
            buffer.restoreKey(decodeURIComponent(entryKey.slice(keyPrefix.length)), saved as any);
          }
        }
        // State saved before keys had their own entries: load it, and the
        // first record below saves every key in the new format.
        const legacy = context.values.get(operatorId);
        if (legacy !== undefined) {
          buffer.restore(legacy as any);
          migrating.add(context);
        }
        context.operatorCaches.set(operatorId, buffer);
      }
      const outputs =
        tagged.side === "left"
          ? buffer.addLeft(tagged.key, record.value, tagged.ts)
          : buffer.addRight(tagged.key, record.value, tagged.ts);

      // Save only the keys that changed. (Before, every record saved the
      // whole buffer under one entry, so the cost grew with the buffer.)
      const changed = new Set(buffer.takeChangedKeys());
      if (migrating.delete(context)) {
        for (const key of buffer.keys()) changed.add(key);
        this.deleteMutation(record, operatorId);
      }
      for (const key of changed) {
        const saved = buffer.snapshotKey(key);
        if (saved) this.putMutation(record, keyEntry(key), saved);
        else this.deleteMutation(record, keyEntry(key));
      }
      return Stream.fromArray(this.branch(record, outputs as unknown[]));
    });
  }

  /**
   * Turn a source envelope into a record, or undefined to drop it.
   *
   * A managed source tells us which partitions we own. A record can still
   * arrive for a partition it just took away (it was already fetched). Such a
   * record is dropped without an ack, so the new owner processes it. Taking
   * the partition's lease back here would lock the new owner out.
   */
  private async prepareEnvelope(
    envelope: Envelope<unknown, unknown>,
    managed: boolean,
  ): Promise<TopologyRecord | undefined> {
    const rawPartition = envelope.metadata.partition;
    const partition = Partition(
      typeof rawPartition === "number" && Number.isInteger(rawPartition) ? rawPartition : 0,
    );
    const owned = managed
      ? this.partitionLifecycle.owned(partition)
      : this.partitionLifecycle.activate(partition);
    if (owned === undefined) return undefined;
    const context = await owned;
    const sourceOffset =
      envelope.metadata.offset === undefined ? undefined : String(envelope.metadata.offset);
    const sourceId =
      sourceOffset === undefined
        ? undefined
        : SourceRecordId(
            `${String(envelope.metadata.topic ?? this.topologyId)}:${partition}:${sourceOffset}`,
          );
    const duplicate =
      sourceId !== undefined &&
      (context.inflightSources.has(sourceId) ||
        (await this.stateBackend.isProcessed({ lease: context.lease, sourceId })));
    // Another copy of this record is skipped while this one is in flight. It
    // is still acked: records commit in order, so by the time the copy
    // commits, this one has, and the backend reports the copy as a duplicate.
    const tracksSource = sourceId !== undefined && !duplicate;
    if (tracksSource) context.inflightSources.add(sourceId);

    context.inflight += 1;
    return {
      value: envelope.value,
      partition,
      skip: duplicate,
      completion: {
        pending: 1,
        context,
        mutations: new Map(),
        envelope,
        sourceId,
        sourceOffset,
        knownDuplicate: duplicate,
        tracksSource,
        outputs: [],
      },
    };
  }

  private deliverRecord(
    record: TopologyRecord,
    sink?: Sinkable<unknown, unknown>,
  ): Eff<void, unknown> {
    const waitForRate =
      this.rateLimiter && !record.skip && sink
        ? fromPromise(
            () => this.rateLimiter!.acquire(),
            (error) => error,
          )
        : succeed(undefined);

    if (this.config.deliveryGuarantee === "exactly-once") {
      if (!record.skip && sink) {
        record.completion.outputs.push({ sink, value: record.value });
      }
      // maxItemsPerSecond limits outputs here too (it used to be ignored).
      return waitForRate.flatMap(() => this.finishRecord(record, true));
    }

    const publish =
      record.skip || !sink
        ? succeed(undefined)
        : waitForRate.flatMap(() => sink.publish(record.value));
    // A finalizer, not tapErrorCause: the in-flight count must drop even when
    // the publish is interrupted.
    return publish
      .onExit((exit) =>
        exit._tag === "Success"
          ? succeed(undefined)
          : sync(() => {
              if (record.completion.pending > 0) {
                record.completion.pending = 0;
                this.recordDone(record.completion);
              }
            }),
      )
      .flatMap(() => this.finishRecord(record, false));
  }

  private finishRecord(record: TopologyRecord, exactlyOnce: boolean): Eff<void, unknown> {
    if (record.completion.pending > 1) {
      record.completion.pending -= 1;
      return succeed(undefined);
    }
    record.completion.pending = 0;
    const completion = record.completion;
    // An end marker that had nothing open to flush has nothing to save.
    if (record.endOfInput && completion.mutations.size === 0 && completion.outputs.length === 0) {
      this.recordDone(completion);
      return succeed(undefined);
    }
    const checkpointId = StateCheckpointId(`${this.instanceId}:${++this.checkpointSequence}`);

    const commit = exactlyOnce
      ? fromPromise(
          () => this.commitExactlyOnce(completion, checkpointId),
          (error) => error,
        )
      : fromPromise(
          async () => {
            const result = await this.stateBackend.commit({
              lease: completion.context.lease,
              mutations: [...completion.mutations.values()],
              sourceId: completion.sourceId,
              sourceOffset: completion.sourceOffset,
              checkpointId,
            });
            if (result === "fenced") throw new Error("partition state lease was fenced");
          },
          (error) => error,
        ).flatMap(() => completion.envelope?.ack() ?? succeed(undefined));

    return commit
      .map(() => {
        if (completion.sourceOffset !== undefined) {
          completion.context.sourceOffset = completion.sourceOffset;
        }
        // End markers are not source records.
        if (completion.envelope) this.itemsProcessed += 1;
      })
      .ensuring(sync(() => this.recordDone(completion)));
  }

  /** The record is finished (committed, or given up), so it is no longer in flight. */
  private recordDone(completion: RecordCompletion): void {
    completion.context.inflight -= 1;
    if (completion.tracksSource) completion.context.inflightSources.delete(completion.sourceId!);
  }

  private async commitExactlyOnce(
    completion: RecordCompletion,
    checkpointId: StateCheckpointId,
  ): Promise<void> {
    const backend = this.stateBackend as TransactionalPartitionedStateBackend<unknown, unknown>;
    // An end marker has no source record: its flushed windows are saved and
    // published in one transaction, with nothing to ack.
    const envelope = completion.envelope;
    if (envelope !== undefined) {
      if (!isTransactionalEnvelope(envelope)) {
        throw new TypeError("exactly-once delivery requires transactional source envelopes");
      }
      if (envelope.transactionDomain !== backend.transactionDomain) {
        throw new TypeError("source and state backend do not share a transaction domain");
      }
    }

    await backend.transaction(async (transaction) => {
      const result = await backend.commitInTransaction(transaction, {
        lease: completion.context.lease,
        mutations: [...completion.mutations.values()],
        sourceId: completion.sourceId,
        sourceOffset: completion.sourceOffset,
        checkpointId,
      });
      if (result === "fenced") throw new Error("partition state lease was fenced");
      if (result === "duplicate" && !completion.knownDuplicate) {
        throw new Error("source record was concurrently committed by another transaction");
      }
      if (result === "committed") {
        for (const output of completion.outputs) {
          const sink = output.sink as TransactionalSinkable<unknown, unknown, unknown>;
          await sink.publishInTransaction(transaction, output.value);
        }
      }
      if (envelope !== undefined) await envelope.ackInTransaction(transaction);
    });
  }

  private validateSink(sink: Sinkable<unknown, unknown>): void {
    if (this.config.deliveryGuarantee !== "exactly-once") return;
    if (!isTransactionalSinkable(sink)) {
      throw new TypeError("exactly-once delivery requires transactional sinks");
    }
    const backend = this.stateBackend as TransactionalPartitionedStateBackend<unknown, unknown>;
    if (sink.transactionDomain !== backend.transactionDomain) {
      throw new TypeError("sink and state backend do not share a transaction domain");
    }
  }

  private checkpointAllState(): Promise<void> {
    return (async () => {
      for (const context of this.partitionLifecycle.contexts.values()) {
        const result = await this.stateBackend.commit({
          lease: context.lease,
          mutations: [],
          sourceOffset: context.sourceOffset,
          checkpointId: StateCheckpointId(
            `${this.instanceId}:checkpoint:${++this.checkpointSequence}`,
          ),
        });
        if (result === "fenced") throw new Error("partition state lease was fenced");
      }
      if (this.legacyStateBackend) {
        await this.legacyStateBackend.checkpoint({
          name: CheckpointName(`topology:${this.config.group}`),
        });
      }
    })();
  }

  private startCheckpoint(): void {
    if (this.checkpointInFlight || this.stopping) return;
    this.checkpointInFlight = this.checkpointAllState()
      .catch((error) => this.failBackground(error))
      .finally(() => {
        this.checkpointInFlight = null;
      });
  }

  private failBackground(error: unknown): void {
    if (this.checkpointFailure !== undefined) return;
    this.checkpointFailure = error;
    this.running = false;
    for (const fiber of this.fibers) fiber.interrupt();
  }

  private async awaitExit(): Promise<readonly ExitT<unknown, void>[]> {
    const exits = await this.drainPromise;
    return this.checkpointFailure === undefined
      ? exits
      : [...exits, Exit.die(this.checkpointFailure)];
  }

  private shutdown(): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shutdownPromise = (async () => {
      this.stopping = true;
      this.running = false;
      if (this.checkpointInterval) clearInterval(this.checkpointInterval);
      if (this.leaseInterval) clearInterval(this.leaseInterval);
      this.checkpointInterval = null;
      this.leaseInterval = null;
      for (const fiber of this.fibers) fiber.interrupt();
      await this.drainPromise;
      await this.checkpointInFlight;
      await this.checkpointAllState();
      // Processing has stopped, so records still counted as in flight will
      // never finish; don't wait for them.
      for (const partition of this.partitionLifecycle.contexts.keys())
        await this.partitionLifecycle.revoke(partition, { waitForInflight: false });
    })();
    return this.shutdownPromise;
  }

  private *operatorCaches(): Iterable<unknown> {
    for (const context of this.partitionLifecycle.contexts.values())
      yield* context.operatorCaches.values();
  }

  private getMetrics(): TopologyMetrics {
    const elapsed = (Date.now() - this.metricsStartTime) / 1000;
    return {
      itemsProcessed: this.itemsProcessed,
      itemsPerSecond: elapsed > 0 ? this.itemsProcessed / elapsed : 0,
      bufferStats: [],
      dedupeSize: sum(this.operatorCaches(), (cache) =>
        cache instanceof InsertionOrderSet ? cache.size : 0,
      ),
      activeWindows: sum(this.operatorCaches(), (cache) =>
        cache instanceof WindowOperatorState ? cache.manager.size : 0,
      ),
      joinBufferSize: sum(this.operatorCaches(), (cache) => {
        if (!(cache instanceof JoinBuffer)) return 0;
        const stats = cache.stats();
        return stats.leftItems + stats.rightItems;
      }),
      lateRecords: this.lateRecords,
    };
  }

  private operatorId(node: TopologyNode, type: string): string {
    const existing = this.operatorIds.get(node);
    if (existing) return existing;
    const index = this.operatorCounts.get(type) ?? 0;
    this.operatorCounts.set(type, index + 1);
    const id = `${type}:${index}`;
    this.operatorIds.set(node, id);
    return id;
  }

  private putMutation(record: TopologyRecord, key: string, value: unknown): void {
    record.completion.context.values.set(key, value);
    record.completion.mutations.set(key, { type: "put", key, value });
  }

  private deleteMutation(record: TopologyRecord, key: string): void {
    record.completion.context.values.delete(key);
    record.completion.mutations.set(key, { type: "delete", key });
  }

  private withValue(record: TopologyRecord, value: unknown): TopologyRecord {
    return { ...record, value, skip: false };
  }

  private skipped(record: TopologyRecord): TopologyRecord {
    return { ...record, skip: true };
  }

  /** The flushed results as records, then the end marker itself, which goes on. */
  private emitBeforeEnd(marker: TopologyRecord, values: readonly unknown[]): TopologyRecord[] {
    marker.completion.pending += values.length;
    const results = values.map((value) => ({ ...marker, value, skip: false, endOfInput: false }));
    return [...results, marker];
  }

  private branch(record: TopologyRecord, values: readonly unknown[]): TopologyRecord[] {
    if (values.length === 0) return [this.skipped(record)];
    record.completion.pending += values.length - 1;
    return values.map((value) => this.withValue(record, value));
  }

  private findWindowAndKey(node: TopologyNode): {
    window: Extract<TopologyNode, { type: "window" }>;
    keyFn: (value: unknown) => string;
  } {
    let window: Extract<TopologyNode, { type: "window" }> | undefined;
    let keyFn: ((value: unknown) => string) | undefined;
    let current: TopologyNode | undefined = node;
    while (current) {
      if (current.type === "window" && !window) window = current;
      if (current.type === "keyBy" && !keyFn) {
        keyFn = current.keyFn as (value: unknown) => string;
      }
      if (window && keyFn) break;
      current = "parent" in current ? (current.parent as TopologyNode) : undefined;
    }
    if (!window) throw new Error("aggregate requires a window");
    if (!keyFn) throw new Error("windowed aggregate requires keyBy");
    return { window, keyFn };
  }

  private findKeyBy(node: TopologyNode): { keyFn: (value: unknown) => string } {
    let current: TopologyNode | undefined = node;
    while (current) {
      if (current.type === "keyBy") {
        return { keyFn: current.keyFn as (value: unknown) => string };
      }
      current = "parent" in current ? (current.parent as TopologyNode) : undefined;
    }
    throw new Error("stateful operator requires keyBy");
  }

  /** The record's event time: from an eventTime() step, else from its value. */
  private timeOf(record: TopologyRecord): number {
    return record.eventTime ?? this.extractTimestamp(record.value);
  }

  private extractTimestamp(value: unknown): number {
    if (value && typeof value === "object") {
      const candidate = value as Record<string, unknown>;
      if (typeof candidate.ts === "number") return candidate.ts;
      if (typeof candidate.timestamp === "number") return candidate.timestamp;
      if (typeof candidate.eventTime === "number") return candidate.eventTime;
      if (typeof candidate.createdAt === "string") return new Date(candidate.createdAt).getTime();
    }
    return Date.now();
  }
}

function sum<T>(items: Iterable<T>, count: (item: T) => number): number {
  let total = 0;
  for (const item of items) total += count(item);
  return total;
}

function validateTopologyConfig(config: TopologyConfig): void {
  const positiveIntegers: Array<[string, number | undefined]> = [
    ["partitionLeaseMs", config.partitionLeaseMs],
    ["checkpointIntervalMs", config.checkpointIntervalMs],
    ["maxBufferSize", config.maxBufferSize],
    ["maxDedupeSize", config.maxDedupeSize],
    ["processedRetentionMs", config.processedRetentionMs],
    ["ackBatchSize", config.ackBatchSize],
    ["ackMaxWaitMs", config.ackMaxWaitMs],
  ];
  for (const [name, value] of positiveIntegers) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1)) {
      throw new RangeError(`${name} must be a positive safe integer, got ${value}`);
    }
  }
  if (
    config.maxItemsPerSecond !== undefined &&
    (!Number.isFinite(config.maxItemsPerSecond) || config.maxItemsPerSecond <= 0)
  ) {
    throw new RangeError(
      `maxItemsPerSecond must be a positive finite number, got ${config.maxItemsPerSecond}`,
    );
  }
}

class LegacyPartitionedStateBackend implements PartitionedStateBackend<unknown> {
  private readonly leases = new InMemoryPartitionedState<unknown>();
  // "@seen:" keys this process knows about, with the time each was written,
  // roughly oldest first. Used to delete them once they are older than the
  // retention. Keys already in the store are added when a partition loads
  // (in store order, so one of those may be deleted up to one retention
  // period late). Only kept when a retention is set.
  private readonly seenAt = new Map<string, number>();

  constructor(
    private readonly backend: StateBackend<string, unknown>,
    private readonly processedRetentionMs?: number,
  ) {}

  // A "@seen:" value is the time it was written (older versions wrote
  // `true`). It counts as seen unless it is older than the retention.
  private stillSeen(value: unknown): boolean {
    if (value === true) return true;
    if (typeof value !== "number") return false;
    return (
      this.processedRetentionMs === undefined || value >= Date.now() - this.processedRetentionMs
    );
  }

  acquire(params: {
    scope: StatePartitionScope;
    ownerId: TopologyInstanceId;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    return this.leases.acquire(params);
  }

  renew(params: {
    lease: StatePartitionLease;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    return this.leases.renew(params);
  }

  async load(lease: StatePartitionLease): Promise<PartitionStateSnapshot<unknown> | undefined> {
    if (!(await this.leases.load(lease))) return undefined;
    const prefix = this.prefix(lease.scope);
    const values = new Map<string, unknown>();
    for (const [key, value] of await this.backend.entries()) {
      if (key.startsWith(prefix)) {
        const relative = key.slice(prefix.length);
        if (relative.startsWith("@seen:")) {
          // Keys from older versions hold `true`: count their age from now.
          if (this.processedRetentionMs !== undefined) {
            this.seenAt.set(key, typeof value === "number" ? value : Date.now());
          }
        } else if (!relative.startsWith("@")) values.set(relative, value);
      } else if (lease.scope.partition === 0 && !key.startsWith("@partition/"))
        values.set(key, value);
    }
    const checkpoint = await this.backend.get(`${prefix}@checkpoint`);
    return {
      values,
      sourceOffset: (await this.backend.get(`${prefix}@offset`)) as string | undefined,
      ...(checkpoint === undefined ? {} : { checkpointId: StateCheckpointId(String(checkpoint)) }),
    };
  }

  async isProcessed(params: {
    lease: StatePartitionLease;
    sourceId: SourceRecordId;
  }): Promise<boolean> {
    if (!(await this.leases.load(params.lease))) return false;
    return this.stillSeen(
      await this.backend.get(`${this.prefix(params.lease.scope)}@seen:${params.sourceId}`),
    );
  }

  async commit(commit: PartitionStateCommit<unknown>) {
    if (!(await this.leases.load(commit.lease))) return "fenced" as const;
    if (
      commit.sourceId &&
      this.stillSeen(
        await this.backend.get(`${this.prefix(commit.lease.scope)}@seen:${commit.sourceId}`),
      )
    ) {
      return "duplicate" as const;
    }
    const prefix = this.prefix(commit.lease.scope);
    // Only the last mutation of each key matters, and different keys don't
    // depend on each other, so they are written in parallel instead of one
    // round trip at a time.
    const lastByKey = new Map<string, (typeof commit.mutations)[number]>();
    for (const mutation of commit.mutations) lastByKey.set(mutation.key, mutation);
    const writes: Promise<unknown>[] = [...lastByKey.values()].map((mutation) =>
      mutation.type === "put"
        ? this.backend.put(`${prefix}${mutation.key}`, mutation.value)
        : this.backend.delete(`${prefix}${mutation.key}`),
    );
    if (commit.sourceId) {
      const seenKey = `${prefix}@seen:${commit.sourceId}`;
      const now = Date.now();
      writes.push(this.backend.put(seenKey, now));
      if (this.processedRetentionMs !== undefined) {
        this.seenAt.delete(seenKey);
        this.seenAt.set(seenKey, now);
      }
    }
    writes.push(...this.forgetOldSeenKeys());
    if (commit.sourceOffset) writes.push(this.backend.put(`${prefix}@offset`, commit.sourceOffset));
    if (commit.checkpointId) {
      writes.push(this.backend.put(`${prefix}@checkpoint`, commit.checkpointId));
    }
    await Promise.all(writes);
    await this.leases.commit(commit);
    return "committed" as const;
  }

  release(lease: StatePartitionLease): Promise<boolean> {
    return this.leases.release(lease);
  }

  // Delete "@seen:" keys older than the retention, oldest first.
  private forgetOldSeenKeys(): Promise<unknown>[] {
    if (this.processedRetentionMs === undefined) return [];
    const cutoff = Date.now() - this.processedRetentionMs;
    const deletes: Promise<unknown>[] = [];
    for (const [key, writtenAt] of this.seenAt) {
      if (writtenAt >= cutoff) break;
      this.seenAt.delete(key);
      deletes.push(this.backend.delete(key));
    }
    return deletes;
  }

  private prefix(scope: StatePartitionScope): string {
    return `@partition/${encodeURIComponent(scope.topologyId)}/${encodeURIComponent(scope.stageId)}/${scope.partition}/`;
  }
}
