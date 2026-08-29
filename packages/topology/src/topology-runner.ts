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
import type { StateBackend } from "./state-backend";
import { BuiltTopology } from "./stream-topology";
import { WindowManager } from "./window-manager";
import { JoinBuffer } from "./join-buffer";
import type {
  TopologyConfig,
  TopologyHandle,
  TopologyMetrics,
  TopologyNode,
  WindowType,
} from "./types";

export class TopologyRunner {
  static async run(topology: BuiltTopology, config: TopologyConfig): Promise<TopologyHandle> {
    return new TopologyRunnerInstance(topology, config).start();
  }
}

class LruSet {
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

interface PartitionContext {
  lease: StatePartitionLease;
  readonly values: Map<string, unknown>;
  inflight: number;
  sourceOffset?: string;
}

interface RecordCompletion {
  pending: number;
  readonly context: PartitionContext;
  readonly mutations: Map<string, StateMutation<unknown>>;
  readonly envelope?: Envelope<unknown, unknown>;
  readonly sourceId?: SourceRecordId;
  readonly sourceOffset?: string;
  readonly knownDuplicate: boolean;
  readonly outputs: { readonly sink: Sinkable<unknown, unknown>; readonly value: unknown }[];
}

interface TopologyRecord {
  readonly value: unknown;
  readonly partition: Partition;
  readonly completion: RecordCompletion;
  readonly skip: boolean;
}

class TopologyRunnerInstance {
  private readonly topologyId: TopologyId;
  private readonly stageId: StageId;
  private readonly instanceId: TopologyInstanceId;
  private readonly leaseMs: number;
  private readonly stateBackend: PartitionedStateBackend<unknown>;
  private readonly legacyStateBackend?: StateBackend<string, unknown>;
  private readonly partitions = new Map<Partition, PartitionContext>();
  private readonly partitionActivations = new Map<Partition, Promise<PartitionContext>>();
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

  private readonly windowManagers: Map<Partition, WindowManager<unknown, unknown, unknown>>[] = [];
  private readonly joinBuffers: Map<Partition, JoinBuffer<unknown, unknown>>[] = [];
  private readonly dedupSets: Map<Partition, LruSet>[] = [];

  private itemsProcessed = 0;
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
        ? new LegacyPartitionedStateBackend(config.stateBackend)
        : new InMemoryPartitionedState());
    if (
      config.deliveryGuarantee === "exactly-once" &&
      !isTransactionalPartitionedStateBackend(this.stateBackend)
    ) {
      throw new TypeError("exactly-once delivery requires a transactional partitionedStateBackend");
    }
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
      () => void this.renewLeases().catch((error) => this.failBackground(error)),
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
      subscription.setPartitionLifecycle({
        assigned: async ({ partitions }) => {
          await Promise.all(partitions.map((partition) => this.activatePartition(partition)));
        },
        revoking: async ({ partitions }) => {
          await Promise.all(partitions.map((partition) => this.revokePartition(partition)));
        },
      });
      this.managedSubscriptions.push(
        subscription as ManagedAcknowledgementSubscription<unknown, unknown>,
      );
      envelopes = subscription.stream.onFinalize(
        fromPromise(
          () => subscription.close(),
          (error) => error,
        ),
      );
    } else {
      envelopes = source.subscribeAck({ group: this.config.group });
    }

    return envelopes.evalMap((envelope) =>
      fromPromise(
        () => this.prepareEnvelope(envelope),
        (error) => error,
      ),
    );
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
    const { windowType, keyFn } = this.findWindowAndKey(node.parent);
    const operatorId = this.operatorId(node, "window");
    const managers = new Map<Partition, WindowManager<unknown, unknown, unknown>>();
    this.windowManagers.push(managers);

    return this.compile(node.parent).flatMap((record) => {
      if (record.skip) return Stream.fromArray([record]);
      const context = record.completion.context;
      let manager = managers.get(record.partition);
      if (!manager) {
        manager = new WindowManager(windowType, node.spec);
        const saved = context.values.get(operatorId);
        if (Array.isArray(saved)) manager.restore(saved as any);
        managers.set(record.partition, manager);
      }

      const key = keyFn(record.value);
      const now = this.extractTimestamp(record.value);
      const outputs = [...manager.add(key, record.value, now), ...manager.flush(key, now)];
      this.putMutation(record, operatorId, manager.snapshot());
      return Stream.fromArray(this.branch(record, outputs));
    });
  }

  private compileDedupe(
    node: Extract<TopologyNode, { type: "dedupe" }>,
  ): Stream<TopologyRecord, any> {
    const operatorId = this.operatorId(node, "dedupe");
    const sets = new Map<Partition, LruSet>();
    this.dedupSets.push(sets);
    const maxSize = this.config.maxDedupeSize ?? 100_000;

    return this.compile(node.parent).map((record) => {
      if (record.skip) return record;
      const context = record.completion.context;
      let seen = sets.get(record.partition);
      if (!seen) {
        seen = new LruSet(maxSize);
        const prefix = `${operatorId}:item:`;
        for (const key of context.values.keys()) {
          if (key.startsWith(prefix)) seen.add(decodeURIComponent(key.slice(prefix.length)));
        }
        sets.set(record.partition, seen);
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
    const buffers = new Map<Partition, JoinBuffer<unknown, unknown>>();
    this.joinBuffers.push(buffers);
    const leftKeyFn = this.findKeyBy(node.left).keyFn;
    const rightKeyFn = this.findKeyBy(node.right).keyFn;

    type Tagged = { record: TopologyRecord; side: "left" | "right"; key: string; ts: number };
    const left = this.compile(node.left).map((record): Tagged => ({
      record,
      side: "left",
      key: leftKeyFn(record.value),
      ts: this.extractTimestamp(record.value),
    }));
    const right = this.compile(node.right).map((record): Tagged => ({
      record,
      side: "right",
      key: rightKeyFn(record.value),
      ts: this.extractTimestamp(record.value),
    }));

    return left.merge(right).flatMap((tagged) => {
      const record = tagged.record;
      if (record.skip) return Stream.fromArray([record]);
      const context = record.completion.context;
      let buffer = buffers.get(record.partition);
      if (!buffer) {
        buffer = new JoinBuffer(node.config.windowMs);
        const saved = context.values.get(operatorId);
        if (saved) buffer.restore(saved as any);
        buffers.set(record.partition, buffer);
      }
      const outputs =
        tagged.side === "left"
          ? buffer.addLeft(tagged.key, record.value, tagged.ts)
          : buffer.addRight(tagged.key, record.value, tagged.ts);
      this.putMutation(record, operatorId, buffer.snapshot());
      return Stream.fromArray(this.branch(record, outputs as unknown[]));
    });
  }

  private async prepareEnvelope(envelope: Envelope<unknown, unknown>): Promise<TopologyRecord> {
    const rawPartition = envelope.metadata.partition;
    const partition = Partition(
      typeof rawPartition === "number" && Number.isInteger(rawPartition) ? rawPartition : 0,
    );
    const context = await this.activatePartition(partition);
    const sourceOffset =
      envelope.metadata.offset === undefined ? undefined : String(envelope.metadata.offset);
    const sourceId =
      sourceOffset === undefined
        ? undefined
        : SourceRecordId(
            `${String(envelope.metadata.topic ?? this.topologyId)}:${partition}:${sourceOffset}`,
          );
    const duplicate = sourceId
      ? await this.stateBackend.isProcessed({ lease: context.lease, sourceId })
      : false;

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
        outputs: [],
      },
    };
  }

  private deliverRecord(
    record: TopologyRecord,
    sink?: Sinkable<unknown, unknown>,
  ): Eff<void, unknown> {
    if (this.config.deliveryGuarantee === "exactly-once") {
      if (!record.skip && sink) {
        record.completion.outputs.push({ sink, value: record.value });
      }
      return this.finishRecord(record, true);
    }

    const publish =
      record.skip || !sink
        ? succeed(undefined)
        : (this.rateLimiter
            ? fromPromise(
                () => this.rateLimiter!.acquire(),
                (error) => error,
              )
            : succeed(undefined)
          ).flatMap(() => sink.publish(record.value));
    return publish
      .tapErrorCause(() =>
        sync(() => {
          if (record.completion.pending > 0) {
            record.completion.pending = 0;
            record.completion.context.inflight -= 1;
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
        this.itemsProcessed += 1;
      })
      .ensuring(
        sync(() => {
          completion.context.inflight -= 1;
        }),
      );
  }

  private async commitExactlyOnce(
    completion: RecordCompletion,
    checkpointId: StateCheckpointId,
  ): Promise<void> {
    const backend = this.stateBackend as TransactionalPartitionedStateBackend<unknown, unknown>;
    const envelope = completion.envelope;
    if (!envelope || !isTransactionalEnvelope(envelope)) {
      throw new TypeError("exactly-once delivery requires transactional source envelopes");
    }
    if (envelope.transactionDomain !== backend.transactionDomain) {
      throw new TypeError("source and state backend do not share a transaction domain");
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
      await envelope.ackInTransaction(transaction);
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

  private async activatePartition(partition: Partition): Promise<PartitionContext> {
    const active = this.partitions.get(partition);
    if (active) return active;
    const pending = this.partitionActivations.get(partition);
    if (pending) return pending;
    const activation = this.acquirePartition(partition).finally(() => {
      this.partitionActivations.delete(partition);
    });
    this.partitionActivations.set(partition, activation);
    return activation;
  }

  private async acquirePartition(partition: Partition): Promise<PartitionContext> {
    const scope: StatePartitionScope = {
      topologyId: this.topologyId,
      stageId: this.stageId,
      partition,
    };
    const deadline = Date.now() + this.leaseMs;
    let lease: StatePartitionLease | undefined;
    while (!lease && Date.now() < deadline) {
      lease = await this.stateBackend.acquire({
        scope,
        ownerId: this.instanceId,
        leaseMs: this.leaseMs,
      });
      if (!lease) await new Promise((resolve) => setTimeout(resolve, 25));
    }
    if (!lease) throw new Error(`partition ${partition} is owned by another instance`);
    const snapshot = await this.stateBackend.load(lease);
    if (!snapshot) throw new Error(`partition ${partition} lease was lost during restore`);
    const context: PartitionContext = {
      lease,
      values: new Map(snapshot.values),
      inflight: 0,
      sourceOffset: snapshot.sourceOffset,
    };
    this.partitions.set(partition, context);
    return context;
  }

  private async revokePartition(partition: Partition): Promise<void> {
    const pending = this.partitionActivations.get(partition);
    if (pending) await pending;
    const context = this.partitions.get(partition);
    if (!context) return;
    const deadline = Date.now() + this.leaseMs;
    while (context.inflight > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    if (context.inflight > 0) {
      throw new Error(`partition ${partition} did not drain before lease revocation`);
    }
    const checkpoint = await this.stateBackend.commit({
      lease: context.lease,
      mutations: [],
      sourceOffset: context.sourceOffset,
      checkpointId: StateCheckpointId(`${this.instanceId}:revoke:${++this.checkpointSequence}`),
    });
    if (checkpoint === "fenced") {
      throw new Error(`partition ${partition} was fenced during revocation checkpoint`);
    }
    if (!(await this.stateBackend.release(context.lease))) {
      throw new Error(`partition ${partition} lease was lost during revocation`);
    }
    this.partitions.delete(partition);
  }

  private async renewLeases(): Promise<void> {
    for (const context of this.partitions.values()) {
      const renewed = await this.stateBackend.renew({
        lease: context.lease,
        leaseMs: this.leaseMs,
      });
      if (!renewed) throw new Error(`partition ${context.lease.scope.partition} lease was fenced`);
      context.lease = renewed;
    }
  }

  private checkpointAllState(): Promise<void> {
    return (async () => {
      for (const context of this.partitions.values()) {
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
      for (const partition of this.partitions.keys()) await this.revokePartition(partition);
    })();
    return this.shutdownPromise;
  }

  private getMetrics(): TopologyMetrics {
    const elapsed = (Date.now() - this.metricsStartTime) / 1000;
    return {
      itemsProcessed: this.itemsProcessed,
      itemsPerSecond: elapsed > 0 ? this.itemsProcessed / elapsed : 0,
      bufferStats: [],
      dedupeSize: this.dedupSets.reduce(
        (total, byPartition) =>
          total + [...byPartition.values()].reduce((sum, set) => sum + set.size, 0),
        0,
      ),
      activeWindows: this.windowManagers.reduce(
        (total, byPartition) =>
          total +
          [...byPartition.values()].reduce((sum, manager) => sum + manager.snapshot().length, 0),
        0,
      ),
      joinBufferSize: this.joinBuffers.reduce(
        (total, byPartition) =>
          total +
          [...byPartition.values()].reduce((sum, buffer) => {
            const stats = buffer.stats();
            return sum + stats.leftItems + stats.rightItems;
          }, 0),
        0,
      ),
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

  private branch(record: TopologyRecord, values: readonly unknown[]): TopologyRecord[] {
    if (values.length === 0) return [this.skipped(record)];
    record.completion.pending += values.length - 1;
    return values.map((value) => this.withValue(record, value));
  }

  private findWindowAndKey(node: TopologyNode): {
    windowType: WindowType;
    keyFn: (value: unknown) => string;
  } {
    let windowType: WindowType | undefined;
    let keyFn: ((value: unknown) => string) | undefined;
    let current: TopologyNode | undefined = node;
    while (current) {
      if (current.type === "window" && !windowType) windowType = current.windowType;
      if (current.type === "keyBy" && !keyFn) {
        keyFn = current.keyFn as (value: unknown) => string;
      }
      if (windowType && keyFn) break;
      current = "parent" in current ? (current.parent as TopologyNode) : undefined;
    }
    if (!windowType) throw new Error("aggregate requires a window");
    if (!keyFn) throw new Error("windowed aggregate requires keyBy");
    return { windowType, keyFn };
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

function validateTopologyConfig(config: TopologyConfig): void {
  const positiveIntegers: Array<[string, number | undefined]> = [
    ["partitionLeaseMs", config.partitionLeaseMs],
    ["checkpointIntervalMs", config.checkpointIntervalMs],
    ["maxBufferSize", config.maxBufferSize],
    ["maxDedupeSize", config.maxDedupeSize],
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

  constructor(private readonly backend: StateBackend<string, unknown>) {}

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
        if (!relative.startsWith("@")) values.set(relative, value);
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
    return (
      (await this.backend.get(`${this.prefix(params.lease.scope)}@seen:${params.sourceId}`)) ===
      true
    );
  }

  async commit(commit: PartitionStateCommit<unknown>) {
    if (!(await this.leases.load(commit.lease))) return "fenced" as const;
    if (
      commit.sourceId &&
      (await this.backend.get(`${this.prefix(commit.lease.scope)}@seen:${commit.sourceId}`)) ===
        true
    ) {
      return "duplicate" as const;
    }
    const prefix = this.prefix(commit.lease.scope);
    for (const mutation of commit.mutations) {
      if (mutation.type === "put")
        await this.backend.put(`${prefix}${mutation.key}`, mutation.value);
      else await this.backend.delete(`${prefix}${mutation.key}`);
    }
    if (commit.sourceId) await this.backend.put(`${prefix}@seen:${commit.sourceId}`, true);
    if (commit.sourceOffset) await this.backend.put(`${prefix}@offset`, commit.sourceOffset);
    if (commit.checkpointId) await this.backend.put(`${prefix}@checkpoint`, commit.checkpointId);
    await this.leases.commit(commit);
    return "committed" as const;
  }

  release(lease: StatePartitionLease): Promise<boolean> {
    return this.leases.release(lease);
  }

  private prefix(scope: StatePartitionScope): string {
    return `@partition/${encodeURIComponent(scope.topologyId)}/${encodeURIComponent(scope.stageId)}/${scope.partition}/`;
  }
}
