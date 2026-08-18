// ---------------------------------------------------------------------------
// TopologyRunner — compiles and executes a StreamTopology
//
// Walks the logical plan (topology nodes) and compiles them into a
// perfect Stream that runs within a single process. Kafka consumer groups
// handle partition assignment across instances — the runner processes
// whatever partitions are assigned to it.
//
// State is keyed and checkpointed so it can be restored on crash/rebalance.
// Backpressure is applied via bounded buffers and rate limiting.
//
// Ported from promin's topology-runner with StreamPipeline → Stream:
//   .mapAsync(fn)        → .evalMap(v => fromPromise(() => fn(v), e => e))
//   .parAsyncMap(n, fn)  → .parEvalMap(n, ...) (order-preserving)
//   .interruptOn(signal) → runFiber + fiber.interrupt() on shutdown
// promin's manual map/filter fusion pass (collectPureOps/fuseOpsToStream) is
// dropped — perfect's Stream fuses adjacent pure ops natively (stream/fusion).
// State logic (windows, joins, dedup, process) is unchanged.
// ---------------------------------------------------------------------------

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
} from "@perfect/core";
import { Stream } from "@perfect/core/stream";
import type { Streamable, Acknowledgeable, Sinkable } from "@perfect/core/connect";
import { autoCommitBatchWithin, CheckpointName } from "@perfect/core/connect";
import type { StateBackend } from "./state-backend";
import { InMemoryState } from "./state-backend";
import { BuiltTopology } from "./stream-topology";
import { WindowManager } from "./window-manager";
import { JoinBuffer } from "./join-buffer";
import type {
  TopologyNode,
  TopologyConfig,
  TopologyHandle,
  TopologyMetrics,
  AggregateSpec,
  ProcessSpec,
  WindowType,
} from "./types";

export class TopologyRunner {
  /** Compile and run a topology. Returns a handle for shutdown. */
  static async run(topology: BuiltTopology, config: TopologyConfig): Promise<TopologyHandle> {
    const runner = new TopologyRunnerInstance(topology, config);
    return runner.start();
  }
}

// ---------------------------------------------------------------------------
// LRU set — bounded dedup with eviction
// ---------------------------------------------------------------------------

class LruSet {
  private set = new Set<string>();
  private order: string[] = [];

  constructor(private readonly maxSize: number) {}

  has(key: string): boolean {
    return this.set.has(key);
  }

  add(key: string): void {
    if (this.set.has(key)) return;
    this.set.add(key);
    this.order.push(key);
    while (this.set.size > this.maxSize) {
      const oldest = this.order.shift()!;
      this.set.delete(oldest);
    }
  }

  get size(): number {
    return this.set.size;
  }

  toArray(): string[] {
    return [...this.set];
  }

  restore(keys: string[]): void {
    this.set.clear();
    this.order = [];
    for (const key of keys) {
      this.set.add(key);
      this.order.push(key);
    }
  }
}

// ---------------------------------------------------------------------------
// Rate limiter — token bucket
// ---------------------------------------------------------------------------

class RateLimiter {
  private tokens: number;
  private lastRefill: number;

  constructor(private readonly maxPerSecond: number) {
    this.tokens = maxPerSecond;
    this.lastRefill = Date.now();
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

      // Wait for token to become available
      const waitMs = Math.ceil(((1 - this.tokens) / this.maxPerSecond) * 1000);
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }
}

// ---------------------------------------------------------------------------
// TopologyRunnerInstance
// ---------------------------------------------------------------------------

class TopologyRunnerInstance {
  private running = true;
  private stateBackend: StateBackend<string, unknown>;
  private checkpointInterval: ReturnType<typeof setInterval> | null = null;
  private fibers: Fiber<void>[] = [];
  private pendingRestores: Promise<void>[] = [];
  private drainPromise: Promise<readonly ExitT<unknown, void>[]> = Promise.resolve([]);
  private checkpointInFlight: Promise<void> | null = null;
  private checkpointFailure: unknown;
  private stopping = false;
  private shutdownPromise: Promise<void> | null = null;

  // Stateful operators tracked for checkpointing
  private windowManagers: WindowManager<unknown, unknown, unknown>[] = [];
  private joinBuffers: JoinBuffer<unknown, unknown>[] = [];
  private dedupSets: LruSet[] = [];
  private processStates: Map<string, unknown>[] = [];

  // Metrics
  private itemsProcessed = 0;
  private metricsStartTime = Date.now();
  private rateLimiter: RateLimiter | null = null;

  constructor(
    private readonly topology: BuiltTopology,
    private readonly config: TopologyConfig,
  ) {
    this.stateBackend = config.stateBackend ?? new InMemoryState();

    if (config.maxItemsPerSecond) {
      this.rateLimiter = new RateLimiter(config.maxItemsPerSecond);
    }
  }

  async start(): Promise<TopologyHandle> {
    await this.restoreAllState();

    const drains: Eff<void, unknown>[] = [];

    if (this.topology.compiled.sinks.length > 0) {
      for (const sink of this.topology.compiled.sinks) {
        let pipeline = this.compile(sink.parent);
        const sinkTarget = sink.sink as Sinkable<unknown, unknown>;

        // Apply backpressure buffer
        if (this.config.maxBufferSize) {
          pipeline = pipeline.buffer(this.config.maxBufferSize);
        }

        drains.push(
          pipeline
            .evalMap((value) => {
              const rateLimit = this.rateLimiter
                ? fromPromise(
                    () => this.rateLimiter!.acquire(),
                    (error) => error,
                  )
                : succeed(undefined);

              return rateLimit
                .flatMap(() => sinkTarget.publish(value))
                .map(() => {
                  this.itemsProcessed++;
                });
            })
            .drain(),
        );
      }
    } else {
      const terminal = this.topology.compiled.nodes[this.topology.compiled.nodes.length - 1]!;
      let pipeline = this.compile(terminal);

      if (this.config.maxBufferSize) {
        pipeline = pipeline.buffer(this.config.maxBufferSize);
      }

      const drainEff = pipeline
        .tap(() => {
          this.itemsProcessed++;
        })
        .drain();

      drains.push(drainEff);
    }

    await Promise.all(this.pendingRestores);

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

    if (this.config.checkpointIntervalMs) {
      this.checkpointInterval = setInterval(
        () => this.startCheckpoint(),
        this.config.checkpointIntervalMs,
      );
    }

    const self = this;
    const handle: TopologyHandle = {
      shutdown: () => self.shutdown(),
      awaitExit: () => self.awaitExit(),
      isRunning: () => self.running,
      metrics: () => self.getMetrics(),
    };

    return handle;
  }

  // -------------------------------------------------------------------------
  // Metrics
  // -------------------------------------------------------------------------

  private getMetrics(): TopologyMetrics {
    const elapsed = (Date.now() - this.metricsStartTime) / 1000;
    return {
      itemsProcessed: this.itemsProcessed,
      itemsPerSecond: elapsed > 0 ? this.itemsProcessed / elapsed : 0,
      bufferStats: [], // Could be wired to Stream.buffer internals in future
      dedupeSize: this.dedupSets.reduce((sum, s) => sum + s.size, 0),
      activeWindows: this.windowManagers.reduce((sum, wm) => sum + wm.snapshot().length, 0),
      joinBufferSize: this.joinBuffers.reduce((sum, jb) => {
        const s = jb.stats();
        return sum + s.leftItems + s.rightItems;
      }, 0),
    };
  }

  // -------------------------------------------------------------------------
  // State checkpoint / restore
  // -------------------------------------------------------------------------

  private async checkpointAllState(): Promise<void> {
    // Save window manager state
    for (let i = 0; i < this.windowManagers.length; i++) {
      const snapshot = this.windowManagers[i]!.snapshot();
      await this.stateBackend.put(`window:${i}`, snapshot);
    }

    // Save dedup sets
    for (let i = 0; i < this.dedupSets.length; i++) {
      await this.stateBackend.put(`dedupe:${i}`, this.dedupSets[i]!.toArray());
    }

    // Save join buffer state — left and right buffers
    for (let i = 0; i < this.joinBuffers.length; i++) {
      await this.stateBackend.put(`join:${i}`, this.joinBuffers[i]!.snapshot());
    }

    // Process states are saved inline during processing, but flush here too
    for (let i = 0; i < this.processStates.length; i++) {
      const entries = [...this.processStates[i]!.entries()];
      await this.stateBackend.put(`process-map:${i}`, entries);
    }

    await this.stateBackend.checkpoint({ name: CheckpointName(`topology:${this.config.group}`) });
  }

  private startCheckpoint(): void {
    if (this.checkpointInFlight || this.stopping) return;
    this.checkpointInFlight = this.checkpointAllState()
      .catch((error) => {
        this.checkpointFailure = error;
        this.running = false;
        for (const fiber of this.fibers) fiber.interrupt();
      })
      .finally(() => {
        this.checkpointInFlight = null;
      });
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
      if (this.checkpointInterval) {
        clearInterval(this.checkpointInterval);
        this.checkpointInterval = null;
      }
      for (const fiber of this.fibers) fiber.interrupt();
      await this.drainPromise;
      await this.checkpointInFlight;
      await this.checkpointAllState();
    })();

    return this.shutdownPromise;
  }

  private async restoreAllState(): Promise<void> {
    await this.stateBackend.restore({ name: CheckpointName(`topology:${this.config.group}`) });

    // Note: actual restoration happens in compile* methods when they
    // create their stateful operators — they check the backend for
    // existing state. This method just loads the checkpoint into the
    // backend's live state.
  }

  // -------------------------------------------------------------------------
  // Compilation
  // -------------------------------------------------------------------------

  // Adjacent pure map/filter ops fuse into a single chunk-walk inside
  // perfect's Stream, so no manual fusion pass is needed here.
  private compile(node: TopologyNode): Stream<unknown, any> {
    switch (node.type) {
      case "source":
        return this.compileSource(node);
      case "map":
        return this.compile(node.parent).map(node.fn);
      case "filter":
        return this.compile(node.parent).filter(node.fn as (v: unknown) => boolean);
      case "mapAsync":
        return this.compile(node.parent).parEvalMap(node.concurrency, (value) =>
          fromPromise(
            () => node.fn(value),
            (e) => e,
          ),
        );
      case "keyBy":
        return this.compile(node.parent);
      case "shuffle":
        return this.compile(node.parent);
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

  private compileSource(node: { source: unknown }): Stream<unknown, any> {
    const source = node.source as Streamable<unknown, any> & Acknowledgeable<unknown, any>;
    const batchSize = this.config.ackBatchSize ?? 100;

    if (batchSize <= 0) {
      return source.subscribe();
    }

    return source
      .subscribeAck({ group: this.config.group })
      .through(autoCommitBatchWithin(batchSize, this.config.ackMaxWaitMs ?? 1_000));
  }

  private compileAggregate(node: {
    parent: TopologyNode;
    spec: AggregateSpec<unknown, unknown, unknown>;
  }): Stream<unknown, any> {
    const { windowType, keyFn } = this.findWindowAndKey(node.parent);
    const manager = new WindowManager(windowType, node.spec);
    const idx = this.windowManagers.length;
    this.windowManagers.push(manager);

    this.pendingRestores.push(
      this.stateBackend.get(`window:${idx}`).then((saved) => {
        if (saved && Array.isArray(saved)) manager.restore(saved as any);
      }),
    );

    return this.compile(node.parent).flatMap((value: unknown) => {
      const key = keyFn(value);
      const now = this.extractTimestamp(value);
      const emitted = manager.add(key, value, now);
      const flushed = manager.flush(key, now);

      const all = [...emitted, ...flushed];
      return Stream.fromIterable(all);
    });
  }

  private compileProcess(node: {
    parent: TopologyNode;
    spec: ProcessSpec<unknown, unknown, unknown>;
  }): Stream<unknown, any> {
    const { keyFn } = this.findKeyBy(node.parent);
    const keyStates = new Map<string, unknown>();
    const idx = this.processStates.length;
    this.processStates.push(keyStates);

    this.pendingRestores.push(
      this.stateBackend.get(`process-map:${idx}`).then((saved) => {
        if (saved && Array.isArray(saved)) {
          for (const [k, v] of saved as [string, unknown][]) {
            keyStates.set(k, v);
          }
        }
      }),
    );

    return this.compile(node.parent).filterMap((value: unknown) => {
      const key = keyFn(value);
      const currentState = keyStates.get(key) ?? node.spec.init();
      const result = node.spec.process(currentState, value);
      keyStates.set(key, result.state);
      return result.emit;
    });
  }

  private compileDedupe(node: {
    parent: TopologyNode;
    keyFn: (value: unknown) => string;
  }): Stream<unknown, any> {
    const maxSize = this.config.maxDedupeSize ?? 100_000;
    const seen = new LruSet(maxSize);
    const idx = this.dedupSets.length;
    this.dedupSets.push(seen);

    this.pendingRestores.push(
      this.stateBackend.get(`dedupe:${idx}`).then((saved) => {
        if (saved && Array.isArray(saved)) seen.restore(saved as string[]);
      }),
    );

    return this.compile(node.parent).filter((value: unknown) => {
      const key = node.keyFn(value);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  private compileJoin(node: {
    left: TopologyNode;
    right: TopologyNode;
    config: { windowMs: number };
  }): Stream<unknown, any> {
    const buffer = new JoinBuffer(node.config.windowMs);
    const idx = this.joinBuffers.length;
    this.joinBuffers.push(buffer);

    this.pendingRestores.push(
      this.stateBackend.get(`join:${idx}`).then((saved) => {
        if (saved) buffer.restore(saved as any);
      }),
    );

    const leftKeyFn = this.findKeyBy(node.left).keyFn;
    const rightKeyFn = this.findKeyBy(node.right).keyFn;

    type Tagged = { side: "left" | "right"; key: string; value: unknown; ts: number };

    const leftStream: Stream<Tagged, any> = this.compile(node.left).map((value) => ({
      side: "left" as const,
      key: leftKeyFn(value),
      value,
      ts: this.extractTimestamp(value),
    }));

    const rightStream: Stream<Tagged, any> = this.compile(node.right).map((value) => ({
      side: "right" as const,
      key: rightKeyFn(value),
      value,
      ts: this.extractTimestamp(value),
    }));

    return leftStream.merge(rightStream).flatMap((tagged) => {
      const matches =
        tagged.side === "left"
          ? buffer.addLeft(tagged.key, tagged.value, tagged.ts)
          : buffer.addRight(tagged.key, tagged.value, tagged.ts);

      return Stream.fromIterable(matches as unknown[]);
    });
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private findWindowAndKey(node: TopologyNode): {
    windowType: WindowType;
    keyFn: (value: unknown) => string;
  } {
    let windowType: WindowType | undefined;
    let keyFn: ((value: unknown) => string) | undefined;
    let current: TopologyNode | undefined = node;

    while (current) {
      if (current.type === "window" && !windowType) {
        windowType = current.windowType;
      }
      if (current.type === "keyBy" && !keyFn) {
        keyFn = current.keyFn as (value: unknown) => string;
      }
      if (windowType && keyFn) break;
      current = "parent" in current ? (current as any).parent : undefined;
    }

    if (!windowType) throw new Error("aggregate requires a window (tumbling/sliding/session)");
    if (!keyFn) throw new Error("windowed aggregate requires keyBy");

    return { windowType, keyFn };
  }

  private findKeyBy(node: TopologyNode): { keyFn: (value: unknown) => string } {
    let current: TopologyNode | undefined = node;
    while (current) {
      if (current.type === "keyBy") {
        return { keyFn: current.keyFn as (value: unknown) => string };
      }
      current = "parent" in current ? (current as any).parent : undefined;
    }
    throw new Error("stateful operator requires keyBy");
  }

  private extractTimestamp(value: unknown): number {
    if (value && typeof value === "object") {
      const v = value as Record<string, unknown>;
      if (typeof v.ts === "number") return v.ts;
      if (typeof v.timestamp === "number") return v.timestamp;
      if (typeof v.eventTime === "number") return v.eventTime;
      if (typeof v.createdAt === "string") return new Date(v.createdAt).getTime();
    }
    return Date.now();
  }
}
