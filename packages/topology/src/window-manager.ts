// ---------------------------------------------------------------------------
// WindowManager — manages windowed state for keyed aggregation
//
// Windows are stored per key: key → (window id → window). A window id is
// the window's start time, or "session". Keeping keys separate means that
// flushing one key only looks at that key's windows, and keys that contain
// ":" can't be mixed up with each other (they were, when everything was in
// one map under "key:windowId" strings).
// ---------------------------------------------------------------------------

import type { TimeWindow, WindowType, AggregateSpec } from "./types.js";

interface WindowEntry<S> {
  window: TimeWindow;
  state: S;
  lastActivity: number;
}

/** One saved window. `key` is missing in snapshots from older versions. */
export interface WindowSnapshot<S> {
  readonly windowKey: string;
  readonly key?: string;
  readonly entry: WindowEntry<S>;
}

const SESSION = "session";

/** Manages windows for many keys, flushing completed windows. */
export class WindowManager<S, T, U> {
  private readonly windowsByKey = new Map<string, Map<string, WindowEntry<S>>>();

  constructor(
    private readonly windowType: WindowType,
    private readonly spec: AggregateSpec<S, T, U>,
  ) {}

  /** The keys that have at least one open window. */
  keys(): IterableIterator<string> {
    return this.windowsByKey.keys();
  }

  /** How many windows are open, across all keys. */
  get size(): number {
    let total = 0;
    for (const windows of this.windowsByKey.values()) total += windows.size;
    return total;
  }

  /** Add an item to the appropriate window(s). Returns any completed windows to emit. */
  add(key: string, value: T, eventTimeMs: number): U[] {
    const emitted: U[] = [];

    switch (this.windowType.type) {
      case "tumbling": {
        const { windowMs } = this.windowType;
        const windowStart = Math.floor(eventTimeMs / windowMs) * windowMs;
        const entry = this.getOrCreate(key, windowStart, windowStart + windowMs);
        entry.state = this.spec.add(entry.state, value);
        entry.lastActivity = eventTimeMs;
        break;
      }

      case "sliding": {
        const { windowMs, slideMs } = this.windowType;
        // Item belongs to all windows that contain this event time
        const earliestStart = Math.floor((eventTimeMs - windowMs) / slideMs + 1) * slideMs;
        for (let start = earliestStart; start <= eventTimeMs; start += slideMs) {
          if (start < 0) continue;
          const entry = this.getOrCreate(key, start, start + windowMs);
          entry.state = this.spec.add(entry.state, value);
          entry.lastActivity = eventTimeMs;
        }
        break;
      }

      case "session": {
        const windows = this.windowsFor(key);
        const existing = windows.get(SESSION);

        if (existing && eventTimeMs - existing.lastActivity <= this.windowType.gapMs) {
          // Extend existing session
          existing.state = this.spec.add(existing.state, value);
          existing.window = { start: existing.window.start, end: eventTimeMs };
          existing.lastActivity = eventTimeMs;
        } else {
          // Close old session if exists
          if (existing) emitted.push(this.spec.emit(key, existing.window, existing.state));
          // Start new session
          windows.set(SESSION, {
            window: { start: eventTimeMs, end: eventTimeMs },
            state: this.spec.add(this.spec.init(), value),
            lastActivity: eventTimeMs,
          });
        }
        break;
      }
    }

    return emitted;
  }

  /** Flush this key's windows that have closed (their end time <= watermark). */
  flush(key: string, watermarkMs: number): U[] {
    const windows = this.windowsByKey.get(key);
    if (windows === undefined) return [];
    const emitted: U[] = [];

    for (const [windowId, entry] of windows) {
      const shouldFlush =
        this.windowType.type === "session"
          ? watermarkMs - entry.lastActivity > this.windowType.gapMs
          : entry.window.end <= watermarkMs;

      if (shouldFlush) {
        emitted.push(this.spec.emit(key, entry.window, entry.state));
        windows.delete(windowId);
      }
    }
    if (windows.size === 0) this.windowsByKey.delete(key);

    return emitted;
  }

  /** Flush all remaining windows (e.g., on shutdown). */
  flushAll(): U[] {
    const emitted: U[] = [];
    for (const [key, windows] of this.windowsByKey) {
      for (const entry of windows.values()) {
        emitted.push(this.spec.emit(key, entry.window, entry.state));
      }
    }
    this.windowsByKey.clear();
    return emitted;
  }

  /** Snapshot every window, for checkpointing. */
  snapshot(): WindowSnapshot<S>[] {
    const snapshots: WindowSnapshot<S>[] = [];
    for (const key of this.windowsByKey.keys()) snapshots.push(...this.snapshotKey(key));
    return snapshots;
  }

  /** Snapshot one key's windows, so a checkpoint only writes what changed. */
  snapshotKey(key: string): WindowSnapshot<S>[] {
    const windows = this.windowsByKey.get(key);
    if (windows === undefined) return [];
    return [...windows].map(([windowId, entry]) => ({
      windowKey: `${key}:${windowId}`,
      key,
      entry,
    }));
  }

  /** Restore windows from a checkpoint. Adds to what is already there. */
  restore(snapshots: readonly WindowSnapshot<S>[]): void {
    for (const { windowKey, key, entry } of snapshots) {
      // Older snapshots only have "key:windowId". The window id never
      // contains ":", so the key is everything before the last ":".
      const split = windowKey.lastIndexOf(":");
      const ownerKey = key ?? windowKey.slice(0, split);
      const windowId = windowKey.slice(split + 1);
      this.windowsFor(ownerKey).set(windowId, entry);
    }
  }

  private windowsFor(key: string): Map<string, WindowEntry<S>> {
    let windows = this.windowsByKey.get(key);
    if (windows === undefined) {
      windows = new Map();
      this.windowsByKey.set(key, windows);
    }
    return windows;
  }

  private getOrCreate(key: string, start: number, end: number): WindowEntry<S> {
    const windows = this.windowsFor(key);
    const windowId = String(start);
    let entry = windows.get(windowId);
    if (!entry) {
      entry = {
        window: { start, end },
        state: this.spec.init(),
        lastActivity: start,
      };
      windows.set(windowId, entry);
    }
    return entry;
  }
}
