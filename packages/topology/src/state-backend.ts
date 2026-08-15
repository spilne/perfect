// ---------------------------------------------------------------------------
// StateBackend<K, V> — pluggable keyed mutable state
// Used by stateful stream operators (e.g. windows, process, dedupe).
//
// Ported from promin's typeclasses/state-backend.ts + adapters/memory.
// Lives in @perfect/topology (not core/connect) — topology is its only
// consumer; durable backends can implement this interface externally.
// ---------------------------------------------------------------------------

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
  checkpoint(params: { name: string }): Promise<void>;

  /** Restore state from a checkpoint. No-op for in-memory. */
  restore(params: { name: string }): Promise<void>;

  /** Clear all state. */
  clear(): Promise<void>;
}

/** Map-backed StateBackend — the default when no backend is configured. */
export class InMemoryState<K, V> implements StateBackend<K, V> {
  private store = new Map<K, V>();
  private checkpoints = new Map<string, Map<K, V>>();

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

  async checkpoint(params: { name: string }): Promise<void> {
    this.checkpoints.set(params.name, new Map(this.store));
  }

  async restore(params: { name: string }): Promise<void> {
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
