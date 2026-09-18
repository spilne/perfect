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

// Fiber<any> throughout the supervision API: Fiber is invariant in A (its
// completion listeners consume A), so `Fiber<unknown>` would reject fibers of
// concrete result types. `any` is the only type that admits every fiber.
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

// The cause a Ready fiber is interrupted with. A failure it was about to raise
// (a resume with a failure, a failure all() or race() delivered, a failure an
// op-budget pause stopped at) is kept, with the interrupt after it.
function interruptCause(pending: unknown): Cause {
  if (pending instanceof Suspend && pending.op === Op.Fail) {
    const cause = pending.a as Cause;
    return Cause.hasInterrupt(cause) ? cause : Cause.then(cause, Cause.interrupt());
  }
  return Cause.interrupt();
}

export class Fiber<A = unknown> {
  state = FiberState.Ready;
  result: FiberResult<A> | null = null;
  private listeners: Array<(result: FiberResult<A>) => void> = [];
  interruptHandle: (() => void) | null = null;
  // Fiber<any>: see FiberSupervisor note — Fiber is invariant in A, so a
  // heterogeneous parent/child tree needs `any`.
  private children = new Set<Fiber<any>>();
  parent: Fiber<any> | null = null;
  scope: Scope | null = null;

  // interpreter state — saved when yielding. `unknown`, not `any`: the
  // interpreter casts at the use site.
  current: unknown = undefined;
  stack: Cont | null = null;
  context: Context | null = null;
  opCount = 0;
  scheduler: Scheduler = getDefaultScheduler();

  // interruption masking — true when the fiber will honor interrupts immediately.
  // Starts true. Flipped by Op.SetInterruptible frames on the continuation stack.
  interruptible = true;
  // Set when interrupt() arrives while !interruptible or while the loop is
  // running; processed on the next boundary.
  interruptPending = false;
  // Identifies the wait the fiber is suspended in. Async, All and Race take a
  // fresh value when they suspend and resume the fiber only while it is still
  // current; interrupt() advances it. A callback that cannot be cancelled (a
  // promise settling late, a child finishing after its parent moved on) is
  // then ignored instead of resuming whatever the fiber waits on next.
  asyncToken = 0;
  // Set once an interrupt is delivered while the fiber is interruptible, and
  // never cleared. From then on, whenever the fiber is interruptible, error
  // handlers are bypassed and the interrupt is raised again on leaving an
  // uninterruptible region, so the fiber can only run finalizers and fail.
  interrupting = false;
  // Set while the fiber is Ready with a value an async resume handed over
  // (an item, a permit) and cleared when a loop run starts from it. If
  // interrupt() replaces that value first, it calls this so the value goes
  // back to where it came from.
  handoffDiscard: (() => void) | null = null;

  complete(result: FiberResult<A>): void {
    if (this.state === FiberState.Done) return;
    this.state = FiberState.Done;
    this.result = result;
    this.interruptHandle = null;
    if (this.parent) {
      this.parent.children.delete(this);
      this.parent = null;
    }
    // interrupt children on completion
    for (const child of this.children) child.interrupt();
    this.children.clear();
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
    // A running loop checks interruptPending before its next op; acting here
    // would read a continuation stack the loop has not saved.
    if (!this.interruptible || this.state === FiberState.Running) {
      this.interruptPending = true;
      return;
    }
    // Only a Ready fiber's current is the effect it runs next; a suspended
    // fiber's is left over from an earlier run.
    const pending = this.state === FiberState.Ready ? this.current : undefined;
    this.asyncToken++;
    this.interrupting = true;
    // A value handed over by a resume whose run has not started is replaced
    // below. It is given back once the fiber is consistently interrupted, so a
    // discard that re-enters this fiber sees it interrupted.
    const discard = this.handoffDiscard;
    this.handoffDiscard = null;
    const cause = discard === null ? interruptCause(pending) : Cause.interrupt();
    // Cleared before the call, so a canceler that interrupts again does not
    // run itself a second time.
    const cancel = this.interruptHandle;
    if (cancel !== null) {
      this.interruptHandle = null;
      cancel();
      if ((this.state as FiberState) === FiberState.Done) {
        discard?.();
        return;
      }
    }
    // If the fiber has a non-empty continuation stack, inject a Fail(Interrupt)
    // and re-schedule — the interpreter loop walks the stack and fires any
    // EnsuringFrame/ScopeFrame finalizers before completing. A fiber-level
    // scope isn't represented by a stack frame, so it also forces the loop
    // path — reject() closes it. Otherwise (nothing to finalize), complete
    // directly.
    if (this.stack !== null || (this.scope !== null && !this.scope.isClosed)) {
      this.current = new Suspend(Op.Fail, cause, null);
      // A Ready fiber already has a loop run queued (a resume, a yield, an
      // op-budget pause or an earlier interrupt) that starts from the new
      // current. Queue another anyway in case that one was dropped by
      // scheduler.shutdown(); runFiberLoop ignores a run that finds the fiber
      // no longer Ready, so the extra run cannot replay saved state.
      this.state = FiberState.Ready;
      // Avoid a circular import on runtime.ts by going through the scheduler;
      // bootstrapFiber installs a `_resume` callback that wraps runFiberLoop.
      this.scheduler.schedule(() => this._resume?.());
      discard?.();
      return;
    }
    this.complete({ ok: false, cause });
    discard?.();
  }

  // Set by bootstrapFiber to point at runFiberLoop(this); avoids a fiber.ts ⇄
  // runtime.ts circular import.
  _resume?: () => void;

  addChild(child: Fiber<any>): void {
    this.children.add(child);
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

  // Once done, whether the result carries an interrupt: an interrupt that
  // arrives during the final scope close does not change the result.
  get interrupted(): boolean {
    if (this.result !== null) return !this.result.ok && Cause.hasInterrupt(this.result.cause);
    return this.interruptPending || this.interrupting;
  }

  get childCount(): number {
    return this.children.size;
  }

  childrenSnapshot(): readonly Fiber<any>[] {
    return Array.from(this.children);
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
