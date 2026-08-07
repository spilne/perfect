import { Cause } from "./cause";
import { type Cont, Suspend, Op } from "./eff";
import type { Exit } from "./exit";
import type { Context } from "./service";
import { type Scheduler, getDefaultScheduler } from "./scheduler";
import type { Scope } from "./scope";

export const enum FiberState {
  Ready = 0,
  Running = 1,
  Suspended = 2,
  Done = 3,
}

export type FiberResult<A> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly cause: Cause };

export type FiberStatus = "ready" | "running" | "suspended" | "done";

export interface FiberSnapshot {
  readonly status: FiberStatus;
  readonly interrupted: boolean;
  readonly childCount: number;
}

export interface FiberSupervisor {
  onStart?(fiber: Fiber<any>): void;
  onFork?(parent: Fiber<any>, child: Fiber<any>): void;
  onInterrupt?(fiber: Fiber<any>): void;
  onEnd?(fiber: Fiber<any>, result: FiberResult<any>): void;
}

const supervisors = new Set<FiberSupervisor>();

export function addFiberSupervisor(supervisor: FiberSupervisor): () => void {
  supervisors.add(supervisor);
  return () => {
    supervisors.delete(supervisor);
  };
}

function notify(fn: (supervisor: FiberSupervisor) => void): void {
  for (const supervisor of supervisors) {
    try {
      fn(supervisor);
    } catch {
      // Supervision is diagnostic-only and must not perturb fiber semantics.
    }
  }
}

export function notifyFiberStart(fiber: Fiber<any>): void {
  notify((supervisor) => supervisor.onStart?.(fiber));
}

export class Fiber<A = unknown> {
  state = FiberState.Ready;
  result: FiberResult<A> | null = null;
  private listeners: Array<(result: FiberResult<A>) => void> = [];
  interruptHandle: (() => void) | null = null;
  children: Fiber<any>[] = [];
  parent: Fiber<any> | null = null;
  scope: Scope | null = null;

  // interpreter state — saved when yielding
  current: any = undefined;
  stack: Cont | null = null;
  context: Context | null = null;
  opCount = 0;
  scheduler: Scheduler = getDefaultScheduler();

  // interruption masking — true when the fiber will honor interrupts immediately.
  // Starts true. Flipped by Op.SetInterruptible frames on the continuation stack.
  interruptible = true;
  // Set when interrupt() arrives while !interruptible; processed on the next boundary.
  interruptPending = false;

  complete(result: FiberResult<A>): void {
    if (this.state === FiberState.Done) return;
    this.state = FiberState.Done;
    this.result = result;
    this.interruptHandle = null;
    if (this.parent) {
      const index = this.parent.children.indexOf(this);
      if (index >= 0) this.parent.children.splice(index, 1);
      this.parent = null;
    }
    // interrupt children on completion
    for (const child of this.children) child.interrupt();
    this.children.length = 0;
    notify((supervisor) => supervisor.onEnd?.(this, result));
    for (const listener of this.listeners) listener(result);
    this.listeners.length = 0;
  }

  onComplete(listener: (result: FiberResult<A>) => void): void {
    if (this.result !== null) {
      listener(this.result);
    } else {
      this.listeners.push(listener);
    }
  }

  interrupt(): void {
    if (this.state === FiberState.Done) return;
    notify((supervisor) => supervisor.onInterrupt?.(this));
    if (!this.interruptible) {
      this.interruptPending = true;
      return;
    }
    if (this.interruptHandle) {
      this.interruptHandle();
      this.interruptHandle = null;
    }
    // If the fiber has a non-empty continuation stack, inject a Fail(Interrupt)
    // and re-schedule — the interpreter loop walks the stack and fires any
    // EnsuringFrame/ScopeFrame finalizers before completing.
    // Otherwise (no frames to walk), complete directly.
    if (this.stack !== null) {
      this.current = new Suspend(Op.Fail, { _tag: "Interrupt" } as Cause, null);
      this.state = FiberState.Ready;
      // Avoid a circular import on runtime.ts by going through the scheduler;
      // bootstrapFiber installs a `_resume` callback that wraps runFiberLoop.
      this.scheduler.schedule(() => this._resume?.());
      return;
    }
    this.complete({ ok: false, cause: { _tag: "Interrupt" } });
  }

  // Set by bootstrapFiber to point at runFiberLoop(this); avoids a fiber.ts ⇄
  // runtime.ts circular import.
  _resume?: () => void;

  addChild(child: Fiber<any>): void {
    this.children.push(child);
    child.parent = this;
    notify((supervisor) => supervisor.onFork?.(this, child));
  }

  get status(): FiberStatus {
    switch (this.state) {
      case FiberState.Ready:
        return "ready";
      case FiberState.Running:
        return "running";
      case FiberState.Suspended:
        return "suspended";
      case FiberState.Done:
        return "done";
    }
  }

  get interrupted(): boolean {
    if (this.interruptPending) return true;
    return this.result?.ok === false ? Cause.hasInterrupt(this.result.cause) : false;
  }

  get childCount(): number {
    return this.children.length;
  }

  childrenSnapshot(): readonly Fiber<any>[] {
    return this.children.slice();
  }

  snapshot(): FiberSnapshot {
    return {
      status: this.status,
      interrupted: this.interrupted,
      childCount: this.childCount,
    };
  }

  // Await completion and resolve with an Exit — never rejects.
  await(): Promise<Exit<unknown, A>> {
    return new Promise((resolve) => {
      this.onComplete((r) => {
        resolve(r.ok ? { _tag: "Success", value: r.value } : { _tag: "Failure", cause: r.cause });
      });
    });
  }
}
