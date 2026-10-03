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

// How many times in a row we run queued work as a microtask before we give
// the event loop a turn (see request()).
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

  // We run queued fibers as a microtask because it is much faster than
  // waiting for the next event loop turn (setImmediate). But microtasks
  // run before timers and I/O, so if we only used microtasks, a busy
  // program could block timers and I/O forever. So every 64 runs in a row,
  // we use setImmediate once to let the event loop catch up.
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

  // We keep two arrays and swap them. Tasks added while a batch is running
  // go into the other array and run next time, so one batch can't grow
  // forever. Swapping also means we don't create a new array every time.
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
  // Position of the next task to run. It's a field (not a local variable)
  // so that if a task calls flush() again, that inner flush continues from
  // the same spot instead of running the same tasks twice.
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
