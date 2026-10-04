// ---------------------------------------------------------------------------
// WindowManager — manages windowed state for keyed aggregation
//
// Windows are stored per key: key → (window id → window). A window id is
// the window's start time. Keeping keys separate means that flushing one key
// only looks at that key's windows, and keys that contain ":" can't be mixed
// up with each other.
//
// Windows close by watermark: the caller says "no record older than this
// will come any more", and every window (of any key) that ends before that
// point is emitted. A record that arrives for windows that already closed is
// late; the caller checks isLate() and drops it.
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

/** Manages windows for many keys, emitting each window once it has closed. */
export class WindowManager<S, T, U> {
  private readonly windowsByKey = new Map<string, Map<string, WindowEntry<S>>>();
  // Keys whose windows changed since takeChangedKeys() was last called.
  private changed = new Set<string>();
  // No window closes before this watermark, so close() can skip the scan
  // over every key until the watermark gets here.
  private nextCloseAt = Infinity;

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

  /**
   * True when every window a record at `eventTimeMs` belongs to has already
   * closed at `watermarkMs`. Adding it would open a window that was emitted
   * already, so the caller should drop it.
   */
  isLate(eventTimeMs: number, watermarkMs: number): boolean {
    switch (this.windowType.type) {
      case "tumbling": {
        const { windowMs } = this.windowType;
        return Math.floor(eventTimeMs / windowMs) * windowMs + windowMs <= watermarkMs;
      }
      case "sliding": {
        // The last window containing the record starts at the slide boundary
        // at or before it.
        const { windowMs, slideMs } = this.windowType;
        return Math.floor(eventTimeMs / slideMs) * slideMs + windowMs <= watermarkMs;
      }
      case "session":
        return watermarkMs - eventTimeMs > this.windowType.gapMs;
    }
  }

  /** Add an item to the window(s) it belongs to. */
  add(key: string, value: T, eventTimeMs: number): void {
    this.changed.add(key);
    switch (this.windowType.type) {
      case "tumbling": {
        const { windowMs } = this.windowType;
        const windowStart = Math.floor(eventTimeMs / windowMs) * windowMs;
        this.addTo(this.getOrCreate(key, windowStart, windowStart + windowMs), value, eventTimeMs);
        break;
      }

      case "sliding": {
        const { windowMs, slideMs } = this.windowType;
        // The item belongs to every window that contains its time.
        const earliestStart = Math.floor((eventTimeMs - windowMs) / slideMs + 1) * slideMs;
        for (let start = earliestStart; start <= eventTimeMs; start += slideMs) {
          if (start < 0) continue;
          this.addTo(this.getOrCreate(key, start, start + windowMs), value, eventTimeMs);
        }
        break;
      }

      case "session":
        this.addToSession(key, value, eventTimeMs);
        break;
    }
  }

  /** Emit and remove every window, of any key, that has closed at the watermark. */
  close(watermarkMs: number): U[] {
    if (watermarkMs < this.nextCloseAt) return [];
    const emitted: U[] = [];
    for (const key of this.windowsByKey.keys()) emitted.push(...this.flush(key, watermarkMs));
    this.nextCloseAt = this.earliestClose();
    return emitted;
  }

  /** Emit and remove this key's windows that have closed at the watermark. */
  flush(key: string, watermarkMs: number): U[] {
    const windows = this.windowsByKey.get(key);
    if (windows === undefined) return [];
    const closed = byStart([...windows].filter(([, entry]) => this.isClosed(entry, watermarkMs)));
    for (const [windowId] of closed) windows.delete(windowId);
    if (closed.length > 0) this.changed.add(key);
    if (windows.size === 0) this.windowsByKey.delete(key);

    return closed.map(([, entry]) => this.spec.emit(key, entry.window, entry.state));
  }

  /** Emit and remove every window (the input has ended). */
  flushAll(): U[] {
    const emitted: U[] = [];
    for (const [key, windows] of this.windowsByKey) {
      for (const [, entry] of byStart([...windows])) {
        emitted.push(this.spec.emit(key, entry.window, entry.state));
      }
      this.changed.add(key);
    }
    this.windowsByKey.clear();
    this.nextCloseAt = Infinity;
    return emitted;
  }

  /** The keys whose windows changed since the last call; only these need saving. */
  takeChangedKeys(): string[] {
    const keys = [...this.changed];
    this.changed.clear();
    return keys;
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
      this.nextCloseAt = Math.min(this.nextCloseAt, this.closesAt(entry));
    }
  }

  // ── Sessions ────────────────────────────────────────────────────

  /**
   * A record joins every session it is within `gapMs` of. If it bridges two
   * sessions, they become one, which needs the aggregate's `merge`; without
   * it, the record joins the earliest of them and the others stay separate.
   */
  private addToSession(key: string, value: T, eventTimeMs: number): void {
    const { gapMs } = this.windowType as { gapMs: number };
    const windows = this.windowsFor(key);
    const touching = [...windows].filter(
      ([, entry]) =>
        eventTimeMs >= entry.window.start - gapMs && eventTimeMs <= entry.window.end + gapMs,
    );
    touching.sort(([, a], [, b]) => a.window.start - b.window.start);

    const merging = this.spec.merge ? touching : touching.slice(0, 1);
    let state = this.spec.init();
    let start = eventTimeMs;
    let end = eventTimeMs;
    for (const [windowId, entry] of merging) {
      state = merging.length === 1 ? entry.state : this.spec.merge!(state, entry.state);
      start = Math.min(start, entry.window.start);
      end = Math.max(end, entry.window.end);
      windows.delete(windowId);
    }

    const entry: WindowEntry<S> = {
      window: { start, end },
      state: this.spec.add(state, value),
      lastActivity: end,
    };
    windows.set(String(start), entry);
    this.nextCloseAt = Math.min(this.nextCloseAt, this.closesAt(entry));
  }

  // ── Helpers ─────────────────────────────────────────────────────

  private isClosed(entry: WindowEntry<S>, watermarkMs: number): boolean {
    return this.windowType.type === "session"
      ? watermarkMs - entry.window.end > this.windowType.gapMs
      : entry.window.end <= watermarkMs;
  }

  /** The watermark at which a window can close (sessions: just after this). */
  private closesAt(entry: WindowEntry<S>): number {
    return this.windowType.type === "session"
      ? entry.window.end + this.windowType.gapMs
      : entry.window.end;
  }

  private earliestClose(): number {
    let earliest = Infinity;
    for (const windows of this.windowsByKey.values()) {
      for (const entry of windows.values()) earliest = Math.min(earliest, this.closesAt(entry));
    }
    return earliest;
  }

  private addTo(entry: WindowEntry<S>, value: T, eventTimeMs: number): void {
    entry.state = this.spec.add(entry.state, value);
    entry.lastActivity = Math.max(entry.lastActivity, eventTimeMs);
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
      this.nextCloseAt = Math.min(this.nextCloseAt, end);
    }
    return entry;
  }
}

/** A key's windows in time order, so they are emitted earliest first. */
function byStart<S>(windows: [string, WindowEntry<S>][]): [string, WindowEntry<S>][] {
  return windows.sort(([, a], [, b]) => a.window.start - b.window.start);
}
