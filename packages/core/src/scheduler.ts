export interface Scheduler {
  schedule(task: () => void): void;
  flush(): void;
  shutdown(): void;
}

export const DEFAULT_BUDGET = 2048;

// detect the best async primitive for the current runtime
const scheduleAsync: (fn: () => void) => void =
  typeof setImmediate === "function"
    ? setImmediate // Bun / Node
    : typeof MessageChannel !== "undefined"
      ? (() => {
          // Browser
          const ch = new MessageChannel();
          let pending: (() => void) | null = null;
          ch.port1.onmessage = () => {
            if (pending) {
              const fn = pending;
              pending = null;
              fn();
            }
          };
          return (fn: () => void) => {
            pending = fn;
            ch.port2.postMessage(null);
          };
        })()
      : (fn: () => void) => setTimeout(fn, 0); // Fallback

const MICRO_DRAIN_BUDGET = 64;

export class AsyncScheduler implements Scheduler {
  private queue: Array<() => void> = [];
  private spare: Array<() => void> = [];
  private scheduled = false;
  private microDrains = 0;

  schedule(task: () => void): void {
    this.queue.push(task);
    if (!this.scheduled) {
      this.scheduled = true;
      this.request();
    }
  }

  // Drain on microtasks: a resume then costs a microtask instead of an event
  // loop turn. Every MICRO_DRAIN_BUDGET drains in a row the next one goes
  // through a macrotask, so I/O and timers still get a turn under load.
  private request(): void {
    if (this.microDrains < MICRO_DRAIN_BUDGET) {
      this.microDrains++;
      queueMicrotask(this.drain);
    } else {
      this.microDrains = 0;
      scheduleAsync(this.macroDrain);
    }
  }

  private readonly macroDrain = (): void => {
    this.microDrains = 0;
    this.drain();
  };

  private readonly drain = (): void => {
    this.scheduled = false;
    this.runBatch();
    if (this.queue.length > 0 && !this.scheduled) {
      this.scheduled = true;
      this.request();
    }
  };

  // Tasks scheduled while a batch runs go to the other buffer and wait for
  // the next drain, so one drain is bounded by what was queued when it began.
  private runBatch(): void {
    const batch = this.queue;
    this.queue = this.spare;
    try {
      for (let i = 0; i < batch.length; i++) batch[i]!();
    } finally {
      batch.length = 0;
      this.spare = batch;
    }
  }

  flush(): void {
    while (this.queue.length > 0) this.runBatch();
    this.scheduled = false;
  }

  shutdown(): void {
    this.queue.length = 0;
    this.scheduled = false;
  }
}

// keep BunScheduler as alias for backwards compat
export const BunScheduler = AsyncScheduler;

export class SyncScheduler implements Scheduler {
  private queue: Array<() => void> = [];
  // A field, not a local, so a flush re-entered from a task continues where
  // the outer one is instead of running tasks again.
  private head = 0;

  schedule(task: () => void): void {
    this.queue.push(task);
  }

  flush(): void {
    while (this.head < this.queue.length) this.queue[this.head++]!();
    this.queue.length = 0;
    this.head = 0;
  }

  shutdown(): void {
    this.queue.length = 0;
    this.head = 0;
  }
}

let defaultScheduler: Scheduler = new AsyncScheduler();

export function getDefaultScheduler(): Scheduler {
  return defaultScheduler;
}

export function setDefaultScheduler(s: Scheduler): void {
  defaultScheduler = s;
}
