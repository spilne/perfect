export interface Scheduler {
  schedule(task: () => void): void
  flush(): void
  shutdown(): void
}

export const DEFAULT_BUDGET = 2048

// detect the best async primitive for the current runtime
const scheduleAsync: (fn: () => void) => void =
  typeof setImmediate === "function"
    ? setImmediate                                      // Bun / Node
    : typeof MessageChannel !== "undefined"
      ? (() => {                                        // Browser
          const ch = new MessageChannel()
          let pending: (() => void) | null = null
          ch.port1.onmessage = () => { if (pending) { const fn = pending; pending = null; fn() } }
          return (fn: () => void) => { pending = fn; ch.port2.postMessage(null) }
        })()
      : (fn: () => void) => setTimeout(fn, 0)          // Fallback

export class AsyncScheduler implements Scheduler {
  private queue: Array<() => void> = []
  private scheduled = false

  schedule(task: () => void): void {
    this.queue.push(task)
    if (!this.scheduled) {
      this.scheduled = true
      scheduleAsync(this.drain)
    }
  }

  private readonly drain = (): void => {
    this.scheduled = false
    const batch = this.queue.splice(0)
    for (let i = 0; i < batch.length; i++) {
      batch[i]!()
    }
    if (this.queue.length > 0 && !this.scheduled) {
      this.scheduled = true
      scheduleAsync(this.drain)
    }
  }

  flush(): void {
    while (this.queue.length > 0) {
      const batch = this.queue.splice(0)
      for (let i = 0; i < batch.length; i++) batch[i]!()
    }
    this.scheduled = false
  }

  shutdown(): void {
    this.queue.length = 0
    this.scheduled = false
  }
}

// keep BunScheduler as alias for backwards compat
export const BunScheduler = AsyncScheduler

export class SyncScheduler implements Scheduler {
  private queue: Array<() => void> = []

  schedule(task: () => void): void {
    this.queue.push(task)
  }

  flush(): void {
    while (this.queue.length > 0) {
      this.queue.shift()!()
    }
  }

  shutdown(): void {
    this.queue.length = 0
  }
}

let defaultScheduler: Scheduler = new AsyncScheduler()

export function getDefaultScheduler(): Scheduler {
  return defaultScheduler
}

export function setDefaultScheduler(s: Scheduler): void {
  defaultScheduler = s
}
