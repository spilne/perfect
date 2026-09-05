import type {
  Partition,
  PartitionedStateBackend,
  StatePartitionLease,
  StatePartitionScope,
  TopologyId,
  StageId,
  TopologyInstanceId,
  StateCheckpointId,
} from "@spilne/perfect-core/connect";

export interface PartitionContext {
  lease: StatePartitionLease;
  readonly values: Map<string, unknown>;
  inflight: number;
  sourceOffset?: string;
}

interface PartitionLifecycleOptions {
  readonly topologyId: TopologyId;
  readonly stageId: StageId;
  readonly instanceId: TopologyInstanceId;
  readonly leaseMs: number;
  readonly stateBackend: PartitionedStateBackend<unknown>;
  readonly nextCheckpointId: () => StateCheckpointId;
}

export class PartitionLifecycle {
  readonly contexts = new Map<Partition, PartitionContext>();
  private readonly activations = new Map<Partition, Promise<PartitionContext>>();

  constructor(private readonly options: PartitionLifecycleOptions) {}

  async activate(partition: Partition): Promise<PartitionContext> {
    const active = this.contexts.get(partition);
    if (active) return active;
    const pending = this.activations.get(partition);
    if (pending) return pending;
    const activation = this.acquire(partition).finally(() => {
      this.activations.delete(partition);
    });
    this.activations.set(partition, activation);
    return activation;
  }

  private async acquire(partition: Partition): Promise<PartitionContext> {
    const scope: StatePartitionScope = {
      topologyId: this.options.topologyId,
      stageId: this.options.stageId,
      partition,
    };
    const deadline = Date.now() + this.options.leaseMs;
    let lease: StatePartitionLease | undefined;
    while (!lease && Date.now() < deadline) {
      lease = await this.options.stateBackend.acquire({
        scope,
        ownerId: this.options.instanceId,
        leaseMs: this.options.leaseMs,
      });
      if (!lease) await new Promise((resolve) => setTimeout(resolve, 25));
    }
    if (!lease) throw new Error(`partition ${partition} is owned by another instance`);
    const snapshot = await this.options.stateBackend.load(lease);
    if (!snapshot) throw new Error(`partition ${partition} lease was lost during restore`);
    const context: PartitionContext = {
      lease,
      values: new Map(snapshot.values),
      inflight: 0,
      sourceOffset: snapshot.sourceOffset,
    };
    this.contexts.set(partition, context);
    return context;
  }

  async revoke(partition: Partition): Promise<void> {
    const pending = this.activations.get(partition);
    if (pending) await pending;
    const context = this.contexts.get(partition);
    if (!context) return;
    const deadline = Date.now() + this.options.leaseMs;
    while (context.inflight > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    if (context.inflight > 0) {
      throw new Error(`partition ${partition} did not drain before lease revocation`);
    }
    const checkpoint = await this.options.stateBackend.commit({
      lease: context.lease,
      mutations: [],
      sourceOffset: context.sourceOffset,
      checkpointId: this.options.nextCheckpointId(),
    });
    if (checkpoint === "fenced") {
      throw new Error(`partition ${partition} was fenced during revocation checkpoint`);
    }
    if (!(await this.options.stateBackend.release(context.lease))) {
      throw new Error(`partition ${partition} lease was lost during revocation`);
    }
    this.contexts.delete(partition);
  }

  async renew(): Promise<void> {
    for (const context of this.contexts.values()) {
      const renewed = await this.options.stateBackend.renew({
        lease: context.lease,
        leaseMs: this.options.leaseMs,
      });
      if (!renewed) throw new Error(`partition ${context.lease.scope.partition} lease was fenced`);
      context.lease = renewed;
    }
  }
}
