import { Cause } from "./cause";
import { type Eff, Suspend, Cont, Op } from "./eff";
import { type Context, emptyContext, mergeContexts } from "./service";
import { Fiber, FiberState } from "./fiber";
import { Scope } from "./scope";
import {
  type Scheduler,
  BunScheduler,
  SyncScheduler,
  DEFAULT_BUDGET,
  getDefaultScheduler,
} from "./scheduler";
import { succeed } from "./constructors";
import { Clock, realClock } from "./clock";
import { Random, realRandom } from "./random";
import { Console, realConsole } from "./console";
import type { Exit } from "./exit";

// Seed the default context once — real Clock/Random/Console are always
// available so sleep() / Random.next / Console.log etc. work without an
// explicit provide(). Tests override via provide(eff, Clock|Random|Console, testImpl).
if (!emptyContext.has(Clock.key)) emptyContext.set(Clock.key, realClock);
if (!emptyContext.has(Random.key)) emptyContext.set(Random.key, realRandom);
if (!emptyContext.has(Console.key)) emptyContext.set(Console.key, realConsole);

type Resolve = (value: any) => void;
type Reject = (cause: Cause) => void;

// Try to evaluate a node fully synchronously without spawning a fiber.
// Returns:
//   { ok: true, value }   — completed sync with a value
//   { ok: false, cause }  — completed sync with a cause
//   null                  — needs the fiber runtime (Async, Fork, Race, nested All, etc.)
export function evalSync(
  node: Suspend,
  ctx: Context,
): { ok: true; value: any } | { ok: false; cause: Cause } | null {
  let cur: any = node;
  let context = ctx;
  let k: Cont | null = null;

  loop: while (true) {
    if (!(cur instanceof Suspend)) {
      while (k !== null) {
        const frame = k;
        k = frame.next;
        switch (frame.op) {
          case Op.FlatMap: {
            cur = frame.fn(cur);
            continue loop;
          }
          case Op.Catch:
          case Op.CatchAll: {
            continue;
          }
          case Op.Provide: {
            context = frame.fn as Context;
            continue;
          }
          case Op.SetInterruptible: {
            continue;
          }
        }
      }
      return { ok: true, value: cur };
    }

    switch (cur.op) {
      case Op.Succeed: {
        cur = cur.a;
        continue loop;
      }
      case Op.Sync: {
        try {
          cur = cur.a();
        } catch (e) {
          cur = new Suspend(Op.Fail, Cause.die(e), null);
        }
        continue loop;
      }
      case Op.Fail: {
        const cause = cur.a as Cause;
        while (k !== null) {
          const frame = k;
          k = frame.next;
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause);
            if (f) {
              cur = frame.fn(f.value);
              continue loop;
            }
          }
          if (frame.op === Op.CatchAll) {
            cur = frame.fn(cause);
            continue loop;
          }
          if (frame.op === Op.Provide) {
            context = frame.fn as Context;
            continue;
          }
          if (frame.op === Op.SetInterruptible) {
            continue;
          }
        }
        return { ok: false, cause };
      }
      case Op.FlatMap: {
        k = new Cont(Op.FlatMap, cur.b, k);
        cur = cur.a;
        continue loop;
      }
      case Op.Catch: {
        k = new Cont(Op.Catch, cur.b, k);
        cur = cur.a;
        continue loop;
      }
      case Op.CatchAll: {
        k = new Cont(Op.CatchAll, cur.b, k);
        cur = cur.a;
        continue loop;
      }
      case Op.Provide: {
        k = new Cont(Op.Provide, context, k);
        context = mergeContexts(context, cur.b as Context);
        cur = cur.a;
        continue loop;
      }
      case Op.GetCtx: {
        const key = cur.a as symbol;
        const val = context.get(key);
        if (val === undefined) {
          return {
            ok: false,
            cause: Cause.die(new Error(`Service not provided: ${key.description}`)),
          };
        }
        cur = val;
        continue loop;
      }
      // Anything else (Async, Fork, ForkDaemon, Race, All, Ensuring, Scoped,
      // AcqRel, GetScope, SetInterruptible, YieldNow) needs the fiber runtime.
      default:
        return null;
    }
  }
}

function runFiberLoop(fiber: Fiber): void {
  if (fiber.state === FiberState.Done) return;
  fiber.state = FiberState.Running;
  fiber.opCount = 0;

  let cur: any = fiber.current;
  let k: Cont | null = fiber.stack;
  let context: Context = fiber.context!;
  const budget = DEFAULT_BUDGET;

  const resolve: Resolve = (value) => {
    fiber.complete({ ok: true, value });
  };
  const reject: Reject = (cause) => {
    // close scope if present
    if (fiber.scope && !fiber.scope.isClosed) {
      const closer = fiber.scope.close();
      stepInline(
        closer as unknown as Suspend,
        context,
        null,
        () => {
          fiber.complete({ ok: false, cause });
        },
        () => {
          fiber.complete({ ok: false, cause });
        },
        fiber,
      );
      return;
    }
    fiber.complete({ ok: false, cause });
  };

  loop: while (true) {
    // check interruption
    if (fiber.state === FiberState.Done) {
      return;
    }

    // honour any pending interrupt the moment we're in interruptible mode
    if (fiber.interruptible && fiber.interruptPending) {
      fiber.interruptPending = false;
      cur = new Suspend(Op.Fail, Cause.interrupt(), null);
    }

    // op budget — yield to scheduler
    if (++fiber.opCount > budget) {
      fiber.current = cur;
      fiber.stack = k;
      fiber.context = context;
      fiber.state = FiberState.Ready;
      fiber.scheduler.schedule(() => runFiberLoop(fiber));
      return;
    }

    // pure value fast path
    if (!(cur instanceof Suspend)) {
      while (k !== null) {
        const frame = k;
        k = frame.next;
        switch (frame.op) {
          case Op.FlatMap: {
            cur = frame.fn(cur);
            continue loop;
          }
          case Op.Catch:
          case Op.CatchAll: {
            continue;
          }
          case Op.Provide: {
            context = frame.fn as Context;
            continue;
          }
          case Op.SetInterruptible: {
            fiber.interruptible = frame.fn as unknown as boolean;
            if (fiber.interruptible && fiber.interruptPending) {
              fiber.interruptPending = false;
              cur = new Suspend(Op.Fail, Cause.interrupt(), null);
              continue loop;
            }
            continue;
          }
          case Op.EnsuringFrame: {
            // Body succeeded with `cur`. Run finalizer (uninterruptibly,
            // swallowing its own failures), then yield the success value.
            const value = cur;
            const finalizer = frame.fn as Suspend;
            cur = new Suspend(
              Op.SetInterruptible,
              new Suspend(
                Op.FlatMap,
                new Suspend(Op.CatchAll, finalizer, () => succeed(undefined)),
                () => new Suspend(Op.Succeed, value, null),
              ),
              false,
            );
            continue loop;
          }
          case Op.ScopeFrame: {
            // Body succeeded. Close scope (uninterruptibly), restore old
            // scope, then yield the value.
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            const value = cur;
            fiber.scope = oldScope;
            cur = new Suspend(
              Op.SetInterruptible,
              new Suspend(
                Op.FlatMap,
                new Suspend(Op.CatchAll, scope.close() as unknown as Suspend, () =>
                  succeed(undefined),
                ),
                () => new Suspend(Op.Succeed, value, null),
              ),
              false,
            );
            continue loop;
          }
        }
      }
      // close scope on success
      if (fiber.scope && !fiber.scope.isClosed) {
        const val = cur;
        const closer = fiber.scope.close();
        stepInline(
          closer as unknown as Suspend,
          context,
          null,
          () => resolve(val),
          () => resolve(val),
          fiber,
        );
        return;
      }
      resolve(cur);
      return;
    }

    switch (cur.op) {
      case Op.Succeed: {
        cur = cur.a;
        continue loop;
      }

      case Op.Sync: {
        try {
          cur = cur.a();
        } catch (e) {
          cur = new Suspend(Op.Fail, Cause.die(e), null);
        }
        continue loop;
      }

      case Op.Fail: {
        const cause = cur.a as Cause;
        while (k !== null) {
          const frame = k;
          k = frame.next;
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause);
            if (f) {
              cur = frame.fn(f.value);
              continue loop;
            }
          }
          if (frame.op === Op.CatchAll) {
            cur = frame.fn(cause);
            continue loop;
          }
          if (frame.op === Op.Provide) {
            context = frame.fn as Context;
            continue;
          }
          if (frame.op === Op.SetInterruptible) {
            fiber.interruptible = frame.fn as unknown as boolean;
            continue;
          }
          if (frame.op === Op.EnsuringFrame) {
            // Run the finalizer (uninterruptibly, swallowing its failures),
            // then re-fail with the original cause.
            const finalizer = frame.fn as Suspend;
            cur = new Suspend(
              Op.SetInterruptible,
              new Suspend(
                Op.FlatMap,
                new Suspend(Op.CatchAll, finalizer, () => succeed(undefined)),
                () => new Suspend(Op.Fail, cause, null),
              ),
              false,
            );
            continue loop;
          }
          if (frame.op === Op.ScopeFrame) {
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            fiber.scope = oldScope;
            cur = new Suspend(
              Op.SetInterruptible,
              new Suspend(
                Op.FlatMap,
                new Suspend(Op.CatchAll, scope.close() as unknown as Suspend, () =>
                  succeed(undefined),
                ),
                () => new Suspend(Op.Fail, cause, null),
              ),
              false,
            );
            continue loop;
          }
        }
        reject(cause);
        return;
      }

      case Op.FlatMap: {
        k = new Cont(Op.FlatMap, cur.b, k);
        cur = cur.a;
        continue loop;
      }

      case Op.Catch: {
        k = new Cont(Op.Catch, cur.b, k);
        cur = cur.a;
        continue loop;
      }

      case Op.CatchAll: {
        k = new Cont(Op.CatchAll, cur.b, k);
        cur = cur.a;
        continue loop;
      }

      case Op.Async: {
        const register = cur.a as (resume: (value: any) => void) => (() => void) | void;

        fiber.stack = k;
        fiber.context = context;
        fiber.state = FiberState.Suspended;

        const cancel = register((value: any) => {
          if (fiber.state === FiberState.Done) return;
          fiber.current = value;
          fiber.state = FiberState.Ready;
          fiber.scheduler.schedule(() => runFiberLoop(fiber));
        });
        if (cancel) fiber.interruptHandle = cancel;
        return;
      }

      case Op.Provide: {
        k = new Cont(Op.Provide, context, k);
        context = mergeContexts(context, cur.b as Context);
        cur = cur.a;
        continue loop;
      }

      case Op.GetCtx: {
        const key = cur.a as symbol;
        const val = context.get(key);
        if (val === undefined) {
          cur = new Suspend(
            Op.Fail,
            Cause.die(new Error(`Service not provided: ${key.description}`)),
            null,
          );
        } else {
          cur = val;
        }
        continue loop;
      }

      case Op.All: {
        const effects = cur.a as Suspend[];
        const len = effects.length;
        if (len === 0) {
          cur = [];
          continue loop;
        }

        // Fast path: only safe when every child is a literal Op.Succeed.
        //
        // Earlier we ran evalSync on each child and bailed on the first async
        // one — but evalSync executes side effects (Op.Sync callbacks, mutation
        // inside Op.FlatMap continuations) that we can't roll back when the
        // bail forces a slow-path restart. That doubled mutations and broke
        // primitives like Queue, Singleflight, etc.
        //
        // Now we only fast-path the trivially safe case (literal pre-computed
        // values — no callbacks, no mutation). Anything more complex goes to
        // the slow path where each child runs in its own fiber.
        let allSucceed = true;
        for (let i = 0; i < len; i++) {
          const child = effects[i] as Suspend;
          if (child.op !== Op.Succeed) {
            allSucceed = false;
            break;
          }
        }
        if (allSucceed) {
          const fastResults = new Array(len);
          for (let i = 0; i < len; i++) {
            fastResults[i] = (effects[i] as Suspend).a;
          }
          cur = fastResults;
          continue loop;
        }

        // Slow path: full fiber-per-element parallel.
        fiber.stack = k;
        fiber.context = context;
        const savedCtx = context;
        const results = new Array(len);
        let remaining = len;
        let failed = false;

        for (let i = 0; i < len; i++) {
          const child = makeChild(fiber, effects[i], savedCtx);

          child.onComplete((result) => {
            if (failed) return;
            if (result.ok) {
              results[i] = result.value;
              if (--remaining === 0) {
                fiber.current = results;
                fiber.state = FiberState.Ready;
                fiber.scheduler.schedule(() => runFiberLoop(fiber));
              }
            } else {
              failed = true;
              for (const c of fiber.children) if (c !== child) c.interrupt();
              fiber.current = new Suspend(Op.Fail, result.cause, null);
              fiber.state = FiberState.Ready;
              fiber.scheduler.schedule(() => runFiberLoop(fiber));
            }
          });

          runChild(child);
        }
        return;
      }

      case Op.Fork: {
        const child = makeChild(fiber, cur.a, context);
        runChild(child);
        cur = child;
        continue loop;
      }

      case Op.Race: {
        const effects = cur.a as Suspend[];
        fiber.stack = k;
        fiber.context = context;
        const savedCtx = context;
        let settled = false;
        const children: Fiber[] = [];

        for (let i = 0; i < effects.length; i++) {
          const child = makeChild(fiber, effects[i], savedCtx);
          children.push(child);

          child.onComplete((result) => {
            if (settled) return;
            settled = true;
            for (const c of children) if (c !== child) c.interrupt();
            if (result.ok) {
              fiber.current = result.value;
            } else {
              fiber.current = new Suspend(Op.Fail, result.cause, null);
            }
            fiber.state = FiberState.Ready;
            fiber.scheduler.schedule(() => runFiberLoop(fiber));
          });

          runChild(child);
        }
        return;
      }

      case Op.Ensuring: {
        // Push a frame; body runs on the same fiber. When the frame is popped
        // (success-path or fail-walk), we run the finalizer then propagate the
        // body's outcome.
        k = new Cont(Op.EnsuringFrame, cur.b /* finalizer */, k);
        cur = cur.a /* body */;
        continue loop;
      }

      case Op.AcqRel: {
        const acquire = cur.a;
        const release = cur.b as (a: any) => Eff<void, never>;

        // acquire, then register release on the fiber's scope
        k = new Cont(
          Op.FlatMap,
          (resource: any) => {
            if (fiber.scope) {
              fiber.scope.addFinalizer(() => release(resource));
            }
            return new Suspend(Op.Succeed, resource, null);
          },
          k,
        );
        cur = acquire;
        continue loop;
      }

      case Op.GetScope: {
        if (!fiber.scope) fiber.scope = new Scope();
        cur = fiber.scope;
        continue loop;
      }

      case Op.Scoped: {
        // Set a fresh scope for the duration of the body, run on same fiber.
        // ScopeFrame stores both the new scope (to close on pop) and the prior
        // scope (to restore).
        const scope = new Scope();
        const oldScope = fiber.scope;
        fiber.scope = scope;
        k = new Cont(Op.ScopeFrame, { scope, oldScope }, k);
        cur = cur.a /* body */;
        continue loop;
      }

      case Op.SetInterruptible: {
        const newValue = cur.b as boolean;
        const prev = fiber.interruptible;
        k = new Cont(Op.SetInterruptible, prev, k);
        fiber.interruptible = newValue;
        cur = cur.a;
        continue loop;
      }

      case Op.YieldNow: {
        fiber.current = new Suspend(Op.Succeed, undefined, null);
        fiber.stack = k;
        fiber.context = context;
        fiber.state = FiberState.Ready;
        fiber.scheduler.schedule(() => runFiberLoop(fiber));
        return;
      }

      case Op.ForkDaemon: {
        const child = makeChild(fiber, cur.a, context, /* structured */ false);
        runChild(child);
        cur = child;
        continue loop;
      }
    }
  }
}

// stepInline: run an effect synchronously where possible, used for finalizers
function stepInline(
  node: Suspend,
  ctx: Context,
  stack: Cont | null,
  resolve: Resolve,
  reject: Reject,
  parentFiber?: Fiber,
): void {
  let cur: any = node;
  let context = ctx;
  let k: Cont | null = stack;

  loop: while (true) {
    if (!(cur instanceof Suspend)) {
      while (k !== null) {
        const frame = k;
        k = frame.next;
        switch (frame.op) {
          case Op.FlatMap: {
            cur = frame.fn(cur);
            continue loop;
          }
          case Op.Catch:
          case Op.CatchAll: {
            continue;
          }
          case Op.Provide: {
            context = frame.fn as Context;
            continue;
          }
          case Op.SetInterruptible: {
            if (parentFiber) parentFiber.interruptible = frame.fn as unknown as boolean;
            continue;
          }
          case Op.EnsuringFrame: {
            const value = cur;
            const finalizer = frame.fn as Suspend;
            cur = new Suspend(
              Op.FlatMap,
              new Suspend(Op.CatchAll, finalizer, () => succeed(undefined)),
              () => new Suspend(Op.Succeed, value, null),
            );
            continue loop;
          }
          case Op.ScopeFrame: {
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            if (parentFiber) parentFiber.scope = oldScope;
            const value = cur;
            cur = new Suspend(
              Op.FlatMap,
              new Suspend(Op.CatchAll, scope.close() as unknown as Suspend, () =>
                succeed(undefined),
              ),
              () => new Suspend(Op.Succeed, value, null),
            );
            continue loop;
          }
        }
      }
      resolve(cur);
      return;
    }

    switch (cur.op) {
      case Op.Succeed: {
        cur = cur.a;
        continue loop;
      }
      case Op.Sync: {
        try {
          cur = cur.a();
        } catch (e) {
          cur = new Suspend(Op.Fail, Cause.die(e), null);
        }
        continue loop;
      }
      case Op.Fail: {
        const cause = cur.a as Cause;
        while (k !== null) {
          const frame = k;
          k = frame.next;
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause);
            if (f) {
              cur = frame.fn(f.value);
              continue loop;
            }
          }
          if (frame.op === Op.CatchAll) {
            cur = frame.fn(cause);
            continue loop;
          }
          if (frame.op === Op.Provide) context = frame.fn as Context;
          if (frame.op === Op.SetInterruptible && parentFiber) {
            parentFiber.interruptible = frame.fn as unknown as boolean;
          }
          if (frame.op === Op.EnsuringFrame) {
            cur = new Suspend(
              Op.FlatMap,
              new Suspend(Op.CatchAll, frame.fn as Suspend, () => succeed(undefined)),
              () => new Suspend(Op.Fail, cause, null),
            );
            continue loop;
          }
          if (frame.op === Op.ScopeFrame) {
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            if (parentFiber) parentFiber.scope = oldScope;
            cur = new Suspend(
              Op.FlatMap,
              new Suspend(Op.CatchAll, scope.close() as unknown as Suspend, () =>
                succeed(undefined),
              ),
              () => new Suspend(Op.Fail, cause, null),
            );
            continue loop;
          }
        }
        reject(cause);
        return;
      }
      case Op.FlatMap: {
        k = new Cont(Op.FlatMap, cur.b, k);
        cur = cur.a;
        continue loop;
      }
      case Op.Catch: {
        k = new Cont(Op.Catch, cur.b, k);
        cur = cur.a;
        continue loop;
      }
      case Op.CatchAll: {
        k = new Cont(Op.CatchAll, cur.b, k);
        cur = cur.a;
        continue loop;
      }
      case Op.Ensuring: {
        const body = cur.a;
        const finalizer = cur.b;
        stepInline(
          body,
          context,
          null,
          (val) => {
            stepInline(
              finalizer,
              context,
              null,
              () => {
                stepInline(
                  new Suspend(Op.Succeed, val, null),
                  context,
                  k,
                  resolve,
                  reject,
                  parentFiber,
                );
              },
              () => {
                stepInline(
                  new Suspend(Op.Succeed, val, null),
                  context,
                  k,
                  resolve,
                  reject,
                  parentFiber,
                );
              },
              parentFiber,
            );
          },
          (cause) => {
            stepInline(
              finalizer,
              context,
              null,
              () => {
                stepInline(
                  new Suspend(Op.Fail, cause, null),
                  context,
                  k,
                  resolve,
                  reject,
                  parentFiber,
                );
              },
              () => {
                stepInline(
                  new Suspend(Op.Fail, cause, null),
                  context,
                  k,
                  resolve,
                  reject,
                  parentFiber,
                );
              },
              parentFiber,
            );
          },
          parentFiber,
        );
        return;
      }
      case Op.Async: {
        // async in finalizer context — still need to handle it
        const register = cur.a;
        register((value: any) => {
          stepInline(
            value instanceof Suspend ? value : new Suspend(Op.Succeed, value, null),
            context,
            k,
            resolve,
            reject,
            parentFiber,
          );
        });
        return;
      }
      default: {
        // Ops we can't inline (Fork, All, etc.) — delegate to the fiber runtime.
        // No structured parent relationship here; the fiber is orphan.
        const child = bootstrapFiber(cur, parentFiber?.scheduler);
        child.context = context;
        child.onComplete((result) => {
          stepInline(
            result.ok
              ? new Suspend(Op.Succeed, result.value, null)
              : new Suspend(Op.Fail, result.cause, null),
            context,
            k,
            resolve,
            reject,
            parentFiber,
          );
        });
        child.scheduler.schedule(() => runFiberLoop(child));
        return;
      }
    }
  }
}

// ── Public API ─────────────────────────────────��───────────────────

// Allocate + bootstrap a fiber ready to be scheduled. Shared by every runner.
function bootstrapFiber<A>(eff: Eff<A, any>, scheduler?: Scheduler): Fiber<A> {
  const fiber = new Fiber<A>();
  fiber.current = eff;
  fiber.context = emptyContext;
  fiber.scheduler = scheduler ?? getDefaultScheduler();
  fiber.state = FiberState.Ready;
  fiber._resume = () => runFiberLoop(fiber);
  return fiber;
}

// Create a child fiber — used by every op that spawns (All, Fork, Race,
// ForkDaemon, stepInline fallback). Doesn't schedule; caller attaches
// onComplete/scope/etc., then calls runChild(child).
function makeChild(parent: Fiber, eff: any, context: Context, structured = true): Fiber {
  const child = new Fiber();
  child.current = eff;
  child.context = context;
  child.scheduler = parent.scheduler;
  child._resume = () => runFiberLoop(child);
  if (structured) parent.addChild(child);
  return child;
}

function runChild(child: Fiber): void {
  child.state = FiberState.Ready;
  child.scheduler.schedule(() => runFiberLoop(child));
}

// Start a bootstrapped fiber and wrap its completion in a Promise. Callers
// decide how to map the FiberResult into the Promise outcome.
function startFiberPromise<A, T>(
  eff: Eff<A, any>,
  scheduler: Scheduler | undefined,
  settle: (
    r: { ok: true; value: A } | { ok: false; cause: Cause },
    resolve: (v: T) => void,
    reject: (e: any) => void,
  ) => void,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const fiber = bootstrapFiber(eff, scheduler);
    fiber.onComplete((r) => settle(r, resolve, reject));
    fiber.scheduler.schedule(() => runFiberLoop(fiber));
  });
}

export function run<A>(eff: Eff<A, any>, scheduler?: Scheduler): Promise<A> {
  // Fast-path: only literal Op.Succeed / Op.Fail leaves. Same restriction
  // as Op.All's fast-path — running evalSync on anything richer might
  // execute Op.Sync callbacks and then bail on a downstream async op,
  // leaking side effects that the slow path then re-runs.
  const node = eff as any;
  if (node.op === Op.Succeed) {
    return Promise.resolve(node.a);
  }
  if (node.op === Op.Fail) {
    return Promise.reject(Cause.squash(node.a));
  }
  return startFiberPromise<A, A>(eff, scheduler, (r, resolve, reject) => {
    if (r.ok) resolve(r.value);
    else reject(Cause.squash(r.cause));
  });
}

export function runSync<A>(eff: Eff<A, never>): A {
  let result: A | undefined;
  let error: Cause | undefined;
  let done = false;

  const scheduler = new SyncScheduler();
  const fiber = bootstrapFiber(eff, scheduler);
  fiber.onComplete((r) => {
    done = true;
    if (r.ok) result = r.value;
    else error = r.cause;
  });
  scheduler.schedule(() => runFiberLoop(fiber));
  scheduler.flush();

  if (!done) throw new Error("runSync: effect did not complete synchronously");
  if (error !== undefined) throw Cause.squash(error);
  return result as A;
}

export function runFiber<A>(eff: Eff<A, any>, scheduler?: Scheduler): Fiber<A> {
  const fiber = bootstrapFiber(eff, scheduler);
  fiber.scheduler.schedule(() => runFiberLoop(fiber));
  return fiber;
}

/**
 * Run an effect and return its Exit. Never throws — on failure the Cause is
 * wrapped as `{ _tag: "Failure", cause }`, on success as `{ _tag: "Success", value }`.
 *
 * Use this when you want to pattern-match on success vs every flavour of
 * failure (typed error, defect, interrupt) and inspect the full Cause tree.
 *
 * Fast-path: purely-synchronous effects (no Async/Fork/Race/etc.) resolve
 * via evalSync without spawning a fiber. Side effects run eagerly during
 * the runExit() call, not on the next microtask — same behaviour as runSync
 * but always returns a Promise.
 */
export function runExit<A>(eff: Eff<A, any>, scheduler?: Scheduler): Promise<Exit<unknown, A>> {
  // Fast-path: literal Op.Succeed / Op.Fail leaves only. Same restriction as
  // run() and Op.All — running evalSync on richer effects might execute
  // Op.Sync callbacks then bail on async, leaking side effects into a
  // slow-path re-run.
  const node = eff as any;
  if (node.op === Op.Succeed) {
    return Promise.resolve({ _tag: "Success" as const, value: node.a });
  }
  if (node.op === Op.Fail) {
    return Promise.resolve({ _tag: "Failure" as const, cause: node.a });
  }
  return startFiberPromise<A, Exit<unknown, A>>(eff, scheduler, (r, resolve) => {
    resolve(r.ok ? { _tag: "Success", value: r.value } : { _tag: "Failure", cause: r.cause });
  });
}

/**
 * Run an effect and return a discriminated `{ data, error }` pair. Never
 * throws for typed failures.
 *
 * By default, only typed errors (Cause.Fail leaves) go into `error`; defects
 * (Cause.Die) and interrupts still throw. Pass `{ catchDefects: true }` to
 * also catch defects — they'll be squashed into the `error` field.
 *
 * @example
 *   const { data, error } = await runSafe(mayFail)
 *   if (error) handle(error); else use(data)
 *
 *   const { data, error } = await runSafe(mayFail, { catchDefects: true })
 *   // error is now typed | unknown (defects as well)
 */
export function runSafe<A, E = unknown>(
  eff: Eff<A, any>,
  opts: { catchDefects: true },
  scheduler?: Scheduler,
): Promise<{ data: A; error: null } | { data: null; error: E | unknown }>;
export function runSafe<A, E = unknown>(
  eff: Eff<A, any>,
  opts?: { catchDefects?: false },
  scheduler?: Scheduler,
): Promise<{ data: A; error: null } | { data: null; error: E }>;
export function runSafe<A>(
  eff: Eff<A, any>,
  opts: { catchDefects?: boolean } = {},
  scheduler?: Scheduler,
): Promise<{ data: A; error: null } | { data: null; error: unknown }> {
  // Fast-path: literal Op.Succeed / Op.Fail leaves only. Same restriction as
  // run() and Op.All — running evalSync on richer effects might execute
  // Op.Sync callbacks then bail on async, leaking side effects into a
  // slow-path re-run.
  const node = eff as any;
  if (node.op === Op.Succeed) {
    return Promise.resolve({ data: node.a, error: null });
  }
  if (node.op === Op.Fail) {
    const cause = node.a;
    const typedFail = Cause.firstFail(cause);
    if (typedFail !== null) {
      return Promise.resolve({ data: null, error: typedFail.value });
    }
    if (opts.catchDefects) {
      return Promise.resolve({ data: null, error: Cause.squash(cause) });
    }
    return Promise.reject(Cause.squash(cause));
  }
  return startFiberPromise<A, { data: A; error: null } | { data: null; error: unknown }>(
    eff,
    scheduler,
    (r, resolve, reject) => {
      if (r.ok) return resolve({ data: r.value, error: null });
      const typedFail = Cause.firstFail(r.cause);
      if (typedFail !== null) {
        return resolve({ data: null, error: typedFail.value });
      }
      if (opts.catchDefects) {
        return resolve({ data: null, error: Cause.squash(r.cause) });
      }
      reject(Cause.squash(r.cause));
    },
  );
}
