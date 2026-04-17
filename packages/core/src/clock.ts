import { type Eff, Suspend, Op } from "./eff";
import { service, type ServiceTag } from "./service";

// ── Clock service ──────────────────────────────────────────────────

export interface Clock {
  /** Suspend for `ms` milliseconds according to this clock's time. */
  readonly sleep: (ms: number) => Eff<void, never>;
  /** Current time in milliseconds (monotonic for TestClock, wall for RealClock). */
  readonly now: () => number;
}

export const Clock: ServiceTag<Clock> = service<Clock>("Clock");

// ── Real clock: wall time via setTimeout / Date.now ────────────────

export class RealClock implements Clock {
  sleep(ms: number): Eff<void, never> {
    return new Suspend(
      Op.Async,
      (resume: (v: any) => void) => {
        const id = setTimeout(() => resume(new Suspend(Op.Succeed, undefined, null)), ms);
        return () => clearTimeout(id);
      },
      null,
    ) as any;
  }

  now(): number {
    return Date.now();
  }
}

export const realClock: Clock = new RealClock();

// ── Test clock: virtual time, manually advanced ────────────────────

interface PendingSleep {
  readonly deadline: number;
  readonly resume: () => void;
  cancelled: boolean;
}

/**
 * Deterministic clock for tests. Sleeps and `now()` read virtual time;
 * `advance(ms)` moves time forward and fires any sleeps whose deadline is
 * at or before the new time (in deadline order).
 */
export class TestClock implements Clock {
  private time: number;
  private pending: PendingSleep[] = [];

  constructor(startTime = 0) {
    this.time = startTime;
  }

  now(): number {
    return this.time;
  }

  sleep(ms: number): Eff<void, never> {
    return new Suspend(
      Op.Async,
      (resume: (v: any) => void) => {
        if (ms <= 0) {
          // match real-setTimeout semantics (still yields, but immediately ready)
          const entry: PendingSleep = {
            deadline: this.time,
            resume: () => resume(new Suspend(Op.Succeed, undefined, null)),
            cancelled: false,
          };
          this.pending.push(entry);
          return () => {
            entry.cancelled = true;
          };
        }
        const entry: PendingSleep = {
          deadline: this.time + ms,
          resume: () => resume(new Suspend(Op.Succeed, undefined, null)),
          cancelled: false,
        };
        this.pending.push(entry);
        return () => {
          entry.cancelled = true;
        };
      },
      null,
    ) as any;
  }

  /**
   * Advance virtual time by `ms`. All sleeps with `deadline <= time` fire,
   * in deadline order. Returns the number of sleeps that fired.
   */
  advance(ms: number): number {
    if (ms < 0) throw new Error("TestClock.advance: ms must be non-negative");
    this.time += ms;
    return this.drainReady();
  }

  /** Set virtual time to an absolute value (must be >= current time). */
  setTime(t: number): number {
    if (t < this.time) throw new Error("TestClock.setTime: cannot move time backwards");
    this.time = t;
    return this.drainReady();
  }

  private drainReady(): number {
    const ready: PendingSleep[] = [];
    const still: PendingSleep[] = [];
    for (const p of this.pending) {
      if (p.cancelled) continue;
      if (p.deadline <= this.time) ready.push(p);
      else still.push(p);
    }
    this.pending = still;
    ready.sort((a, b) => a.deadline - b.deadline);
    for (const p of ready) p.resume();
    return ready.length;
  }

  /** Number of sleeps still waiting on this clock. */
  get pendingCount(): number {
    return this.pending.filter((p) => !p.cancelled).length;
  }

  /** Deadlines of all still-waiting sleeps, sorted ascending. */
  pendingDeadlines(): number[] {
    return this.pending
      .filter((p) => !p.cancelled)
      .map((p) => p.deadline)
      .sort((a, b) => a - b);
  }
}
