import {
  LeaseEpoch,
  type Partition,
  type SourceRecordId,
  type StageId,
  type StateCheckpointId,
  type TopologyId,
  type TopologyInstanceId,
} from "./contracts";

export interface StatePartitionScope {
  readonly topologyId: TopologyId;
  readonly stageId: StageId;
  readonly partition: Partition;
}

export interface StatePartitionLease {
  readonly scope: StatePartitionScope;
  readonly ownerId: TopologyInstanceId;
  readonly epoch: LeaseEpoch;
  readonly expiresAt: number;
}

export type StateMutation<V> =
  | { readonly type: "put"; readonly key: string; readonly value: V }
  | { readonly type: "delete"; readonly key: string };

export interface PartitionStateCommit<V> {
  readonly lease: StatePartitionLease;
  readonly mutations: readonly StateMutation<V>[];
  readonly sourceId?: SourceRecordId;
  readonly sourceOffset?: string;
  readonly checkpointId?: StateCheckpointId;
}

export type PartitionCommitResult = "committed" | "duplicate" | "fenced";

export interface PartitionStateSnapshot<V> {
  readonly values: ReadonlyMap<string, V>;
  readonly sourceOffset?: string;
  readonly checkpointId?: StateCheckpointId;
}

export interface PartitionedStateBackend<V = unknown> {
  acquire(params: {
    scope: StatePartitionScope;
    ownerId: TopologyInstanceId;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined>;

  renew(params: {
    lease: StatePartitionLease;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined>;

  load(lease: StatePartitionLease): Promise<PartitionStateSnapshot<V> | undefined>;

  isProcessed(params: { lease: StatePartitionLease; sourceId: SourceRecordId }): Promise<boolean>;

  commit(commit: PartitionStateCommit<V>): Promise<PartitionCommitResult>;

  release(lease: StatePartitionLease): Promise<boolean>;
}

export interface TransactionalPartitionedStateBackend<
  V = unknown,
  Transaction = unknown,
> extends PartitionedStateBackend<V> {
  readonly transactionDomain: object;

  transaction<A>(work: (transaction: Transaction) => Promise<A>): Promise<A>;

  commitInTransaction(
    transaction: Transaction,
    commit: PartitionStateCommit<V>,
  ): Promise<PartitionCommitResult>;
}

export function isTransactionalPartitionedStateBackend<V = unknown, Transaction = unknown>(
  backend: PartitionedStateBackend<V>,
): backend is TransactionalPartitionedStateBackend<V, Transaction> {
  return (
    "transactionDomain" in backend &&
    typeof (backend as any).transactionDomain === "object" &&
    (backend as any).transactionDomain !== null &&
    "transaction" in backend &&
    typeof (backend as any).transaction === "function" &&
    "commitInTransaction" in backend &&
    typeof (backend as any).commitInTransaction === "function"
  );
}

interface InMemoryPartition<V> {
  ownerId?: TopologyInstanceId;
  epoch: number;
  expiresAt: number;
  values: Map<string, V>;
  processed: Set<SourceRecordId>;
  sourceOffset?: string;
  checkpointId?: StateCheckpointId;
}

export class InMemoryPartitionedState<V = unknown> implements PartitionedStateBackend<V> {
  private readonly partitions = new Map<string, InMemoryPartition<V>>();

  async acquire(params: {
    scope: StatePartitionScope;
    ownerId: TopologyInstanceId;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    validateLeaseMs(params.leaseMs);
    const key = scopeKey(params.scope);
    const now = Date.now();
    const current = this.partitions.get(key) ?? {
      epoch: 0,
      expiresAt: 0,
      values: new Map<string, V>(),
      processed: new Set<SourceRecordId>(),
    };

    if (current.ownerId === params.ownerId && current.expiresAt > now) {
      current.expiresAt = now + params.leaseMs;
    } else {
      if (current.ownerId !== undefined && current.expiresAt > now) return undefined;
      current.ownerId = params.ownerId;
      current.epoch += 1;
      current.expiresAt = now + params.leaseMs;
    }

    this.partitions.set(key, current);
    return leaseOf(params.scope, current);
  }

  async renew(params: {
    lease: StatePartitionLease;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    validateLeaseMs(params.leaseMs);
    const current = this.partitions.get(scopeKey(params.lease.scope));
    if (!current || !owns(current, params.lease)) return undefined;
    current.expiresAt = Date.now() + params.leaseMs;
    return leaseOf(params.lease.scope, current);
  }

  async load(lease: StatePartitionLease): Promise<PartitionStateSnapshot<V> | undefined> {
    const current = this.partitions.get(scopeKey(lease.scope));
    if (!current || !owns(current, lease)) return undefined;
    return {
      values: new Map(current.values),
      sourceOffset: current.sourceOffset,
      checkpointId: current.checkpointId,
    };
  }

  async commit(commit: PartitionStateCommit<V>): Promise<PartitionCommitResult> {
    const current = this.partitions.get(scopeKey(commit.lease.scope));
    if (!current || !owns(current, commit.lease)) return "fenced";
    if (commit.sourceId !== undefined && current.processed.has(commit.sourceId)) return "duplicate";

    for (const mutation of commit.mutations) {
      if (mutation.type === "put") current.values.set(mutation.key, mutation.value);
      else current.values.delete(mutation.key);
    }
    if (commit.sourceId !== undefined) current.processed.add(commit.sourceId);
    if (commit.sourceOffset !== undefined) current.sourceOffset = commit.sourceOffset;
    if (commit.checkpointId !== undefined) current.checkpointId = commit.checkpointId;
    return "committed";
  }

  async isProcessed(params: {
    lease: StatePartitionLease;
    sourceId: SourceRecordId;
  }): Promise<boolean> {
    const current = this.partitions.get(scopeKey(params.lease.scope));
    return Boolean(
      current && owns(current, params.lease) && current.processed.has(params.sourceId),
    );
  }

  async release(lease: StatePartitionLease): Promise<boolean> {
    const current = this.partitions.get(scopeKey(lease.scope));
    if (!current || !owns(current, lease)) return false;
    current.ownerId = undefined;
    current.expiresAt = 0;
    return true;
  }
}

function scopeKey(scope: StatePartitionScope): string {
  return `${scope.topologyId}\u0000${scope.stageId}\u0000${scope.partition}`;
}

function owns<V>(partition: InMemoryPartition<V>, lease: StatePartitionLease): boolean {
  return (
    partition.ownerId === lease.ownerId &&
    partition.epoch === lease.epoch &&
    partition.expiresAt > Date.now()
  );
}

function leaseOf<V>(
  scope: StatePartitionScope,
  partition: InMemoryPartition<V>,
): StatePartitionLease {
  return {
    scope,
    ownerId: partition.ownerId!,
    epoch: LeaseEpoch(partition.epoch),
    expiresAt: partition.expiresAt,
  };
}

function validateLeaseMs(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1) {
    throw new RangeError(`leaseMs must be a positive safe integer, got ${leaseMs}`);
  }
}
