// ---------------------------------------------------------------------------
// StateBackend<K, V> — pluggable keyed mutable state
// Used by stateful stream operators (e.g. windows, process, dedupe).
//
// Ported from promin's typeclasses/state-backend.ts + adapters/memory.
// Lives in core/connect so durable backends (Postgres/Redis/SQLite) can
// implement the checkpoint interface without depending on @perfect/topology.
// Persistence remains a Promise-based driver boundary; stateful operators
// wrap it in an Eff when typed failures need to be exposed.
// ---------------------------------------------------------------------------

import { type Brand, nominal } from "../brand";

/**
 * A checkpoint identifier. Branded because backends with string keys
 * (K = string) would otherwise let a state key slip in where a checkpoint
 * name belongs — both are plain strings at the same call sites.
 */
export type CheckpointName = Brand<string, "CheckpointName">;
// Signature pinned explicitly — see the note in contracts.ts.
export const CheckpointName = nominal<CheckpointName>() as (value: string) => CheckpointName;

export interface StateBackend<K, V> {
  /** Get the value for a key. Returns undefined if not found. */
  get(key: K): Promise<V | undefined>;

  /** Set the value for a key. */
  put(key: K, value: V): Promise<void>;

  /** Delete a key. */
  delete(key: K): Promise<void>;

  /** Get all keys. */
  keys(): Promise<K[]>;

  /** Get all entries. */
  entries(): Promise<[K, V][]>;

  /** Checkpoint current state (for crash recovery). No-op for in-memory. */
  checkpoint(params: { name: CheckpointName }): Promise<void>;

  /** Restore state from a checkpoint. No-op for in-memory. */
  restore(params: { name: CheckpointName }): Promise<void>;

  /** Clear all state. */
  clear(): Promise<void>;
}

/** Map-backed StateBackend — the default when no backend is configured. */
export class InMemoryState<K, V> implements StateBackend<K, V> {
  private store = new Map<K, V>();
  private checkpoints = new Map<CheckpointName, Map<K, V>>();

  async get(key: K): Promise<V | undefined> {
    return this.store.get(key);
  }

  async put(key: K, value: V): Promise<void> {
    this.store.set(key, value);
  }

  async delete(key: K): Promise<void> {
    this.store.delete(key);
  }

  async keys(): Promise<K[]> {
    return [...this.store.keys()];
  }

  async entries(): Promise<[K, V][]> {
    return [...this.store.entries()];
  }

  async checkpoint(params: { name: CheckpointName }): Promise<void> {
    this.checkpoints.set(params.name, new Map(this.store));
  }

  async restore(params: { name: CheckpointName }): Promise<void> {
    const saved = this.checkpoints.get(params.name);
    if (saved) {
      this.store = new Map(saved);
    }
  }

  async clear(): Promise<void> {
    this.store.clear();
  }

  /** Test helper: current size. */
  get size(): number {
    return this.store.size;
  }
}
