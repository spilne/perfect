import type { Cause } from "./cause";
import type { Eff, Cont } from "./eff";
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

export class Fiber<A = unknown> {
  state = FiberState.Ready;
  result: FiberResult<A> | null = null;
  private listeners: Array<(result: FiberResult<A>) => void> = [];
  interruptHandle: (() => void) | null = null;
  children: Fiber[] = [];
  parent: Fiber | null = null;
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
    // interrupt children on completion
    for (const child of this.children) child.interrupt();
    this.children.length = 0;
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
    if (!this.interruptible) {
      this.interruptPending = true;
      return;
    }
    if (this.interruptHandle) {
      this.interruptHandle();
      this.interruptHandle = null;
    }
    this.complete({ ok: false, cause: { _tag: "Interrupt" } });
  }

  addChild(child: Fiber): void {
    this.children.push(child);
    child.parent = this;
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
