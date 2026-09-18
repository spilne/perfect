import { Cause } from "./cause";
import { type Eff, type EffectCheck, Suspend, Cont, Op } from "./eff";
import { type Context, emptyContext, mergeContexts } from "./service";
import { Fiber, FiberState, type FiberResult, notifyFiberStart } from "./fiber";
import { Scope } from "./scope";
import { type Scheduler, SyncScheduler, DEFAULT_BUDGET, getDefaultScheduler } from "./scheduler";
import { Clock, realClock } from "./clock";
import { Random, realRandom } from "./random";
import { Console, realConsole } from "./console";
import { Logger, defaultLogger, LOG_ANNOTATIONS_KEY } from "./logger";
import { Tracer, noopTracer, CURRENT_SPAN_KEY, NO_SPAN } from "./tracing";
import { Metrics, defaultMetricsRegistry } from "./metrics";
import type { Exit } from "./exit";

// Seed the default context once — real Clock/Random/Console are always
// available so sleep() / Random.next / Console.log etc. work without an
// explicit provide(). Tests override via provide(eff, Clock|Random|Console, testImpl).
if (!emptyContext.has(Clock.key)) emptyContext.set(Clock.key, realClock);
if (!emptyContext.has(Random.key)) emptyContext.set(Random.key, realRandom);
if (!emptyContext.has(Console.key)) emptyContext.set(Console.key, realConsole);
if (!emptyContext.has(Logger.key)) emptyContext.set(Logger.key, defaultLogger);
if (!emptyContext.has(LOG_ANNOTATIONS_KEY)) emptyContext.set(LOG_ANNOTATIONS_KEY, {});
if (!emptyContext.has(Tracer.key)) emptyContext.set(Tracer.key, noopTracer);
if (!emptyContext.has(CURRENT_SPAN_KEY)) emptyContext.set(CURRENT_SPAN_KEY, NO_SPAN);
if (!emptyContext.has(Metrics.key)) emptyContext.set(Metrics.key, defaultMetricsRegistry);

type Resolve = (value: any) => void;
type Reject = (cause: Cause) => void;

function succeedAfterFinalizer(finalizer: Suspend, value: any): Suspend {
  return new Suspend(
    Op.CatchAll,
    new Suspend(Op.FlatMap, finalizer, () => new Suspend(Op.Succeed, value, null)),
    (cause: Cause) => new Suspend(Op.Fail, cause, null),
  );
}

// The finalizer's outcome is reified BEFORE the original cause is rethrown —
// rethrowing inside the finalizer's own CatchAll would re-catch it and
// compose the cause with itself.
function failAfterFinalizer(finalizer: Suspend, cause: Cause): Suspend {
  return new Suspend(
    Op.FlatMap,
    new Suspend(
      Op.CatchAll,
      new Suspend(Op.FlatMap, finalizer, () => new Suspend(Op.Succeed, null, null)),
      (finalizerCause: Cause) => new Suspend(Op.Succeed, finalizerCause, null),
    ),
    (finalizerCause: Cause | null) =>
      new Suspend(
        Op.Fail,
        finalizerCause === null ? cause : Cause.then(cause, finalizerCause),
        null,
      ),
  );
}

// Masks interruption for a finalizer the walk is about to run. Flipping the
// flag here, rather than through an Op.SetInterruptible node, leaves no step
// between popping the finalizer's frame and masking: an interrupt pending at
// the top of the loop, or delivered while an op-budget pause has the node
// queued, would otherwise replace the node and drop the finalizer.
function enterUninterruptible(fiber: Fiber<any>, k: Cont | null): Cont {
  const frame = new Cont(Op.SetInterruptible, fiber.interruptible, k);
  fiber.interruptible = false;
  return frame;
}

function withInterrupt(cause: Cause): Cause {
  return Cause.hasInterrupt(cause) ? cause : Cause.then(cause, Cause.interrupt());
}

// The cause that continues past an error handler an interrupting fiber
// bypassed. Its typed failures are dropped: the handler would have consumed or
// mapped them, so keeping them would surface errors the effect's type says
// were handled. Defects and the interrupt stay.
function bypassHandler(cause: Cause): Cause {
  const kept = stripFailures(cause);
  return kept === null ? Cause.interrupt() : withInterrupt(kept);
}

function stripFailures(cause: Cause): Cause | null {
  switch (cause._tag) {
    case "Fail":
      return null;
    case "Die":
    case "Interrupt":
      return cause;
    case "Both":
    case "Then": {
      const left = stripFailures(cause.left);
      const right = stripFailures(cause.right);
      if (left === null) return right;
      if (right === null) return left;
      if (left === cause.left && right === cause.right) return cause;
      return { _tag: cause._tag, left, right };
    }
  }
}

// An Op.Ensuring finalizer is an effect, or a function of the body's Exit
// (onExit) that returns null when it has nothing to run. A finalizer that is a
// plain succeed(...) has nothing to run either, and is skipped outright.
function exitFinalizer(finalizer: unknown, exit: Exit<unknown, unknown>): Suspend | null {
  try {
    return (finalizer as (exit: Exit<unknown, unknown>) => Suspend | null)(exit);
  } catch (e) {
    return new Suspend(Op.Fail, Cause.die(e), null);
  }
}

// Owns resumable execution: saves the continuation on suspension, honors
// interruption, and yields through the scheduler for cooperative fairness.
// Keep opcode handling aligned with stepInline, whose finalization contract
// deliberately excludes normal fiber scheduling and completion.
//
// Every run starts from a Ready fiber and consumes that state. A run that
// finds the fiber in any other state is a duplicate (see Fiber.interrupt) and
// does nothing.
function runFiberLoop(fiber: Fiber<any>): void {
  if (fiber.state !== FiberState.Ready) return;
  fiber.state = FiberState.Running;
  // A value handed over by an async resume is delivered once this run starts.
  fiber.handoffDiscard = null;
  fiber.valueInFlight = false;
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
        (closeCause) => {
          fiber.complete({ ok: false, cause: Cause.then(cause, closeCause) });
        },
        fiber,
      );
      return;
    }
    fiber.complete({ ok: false, cause });
  };

  loop: while (true) {
    // check interruption
    if ((fiber.state as FiberState) === FiberState.Done) {
      return;
    }

    // honour any pending interrupt at the next effect step in interruptible
    // mode; a failure already on its way keeps its cause. A value, or an
    // Op.Succeed holding one, first reaches the next frame's continuation: a
    // primitive's fast path (a queue take, say) returns what it removed that
    // way, and an op-budget pause may separate the two (see Fiber.interrupt).
    if (
      fiber.interruptible &&
      fiber.interruptPending &&
      cur instanceof Suspend &&
      cur.op !== Op.Succeed
    ) {
      fiber.interruptPending = false;
      fiber.interrupting = true;
      cur =
        cur instanceof Suspend && cur.op === Op.Fail
          ? new Suspend(Op.Fail, withInterrupt(cur.a as Cause), null)
          : new Suspend(Op.Fail, Cause.interrupt(), null);
    }

    // op budget — yield to scheduler. Every step counts, so a long unwind of
    // values through already-pushed frames still yields.
    if (++fiber.opCount > budget) {
      fiber.current = cur;
      fiber.stack = k;
      fiber.context = context;
      fiber.state = FiberState.Ready;
      fiber.valueInFlight = !(cur instanceof Suspend) || cur.op === Op.Succeed;
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
            // a throwing continuation (user callback in .map/.flatMap) is a
            // defect, not an interpreter crash
            try {
              cur = (frame.fn as any)(cur);
            } catch (e) {
              cur = new Suspend(Op.Fail, Cause.die(e), null);
            }
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
            // An uninterruptible region that handled an earlier interrupt
            // cannot resume normal execution past its end.
            if (fiber.interruptible && (fiber.interruptPending || fiber.interrupting)) {
              fiber.interruptPending = false;
              fiber.interrupting = true;
              cur = new Suspend(Op.Fail, Cause.interrupt(), null);
              continue loop;
            }
            continue;
          }
          case Op.EnsuringFrame: {
            const value = cur;
            const finalizer =
              typeof frame.fn === "function"
                ? exitFinalizer(frame.fn, { _tag: "Success", value })
                : (frame.fn as Suspend);
            if (finalizer === null || finalizer.op === Op.Succeed) continue;
            k = enterUninterruptible(fiber, k);
            cur = succeedAfterFinalizer(finalizer, value);
            continue loop;
          }
          case Op.ScopeFrame: {
            // Body succeeded. Close scope (uninterruptibly), restore old
            // scope, then yield the value.
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            const value = cur;
            fiber.scope = oldScope;
            k = enterUninterruptible(fiber, k);
            cur = succeedAfterFinalizer(scope.close() as unknown as Suspend, value);
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
          (closeCause) => reject(closeCause),
          fiber,
        );
        return;
      }
      resolve(cur);
      return;
    }

    if (cur.op === Op.Succeed) {
      cur = cur.a;
      continue loop;
    }

    switch (cur.op) {
      case Op.Sync: {
        try {
          cur = (cur.a as any)();
        } catch (e) {
          cur = new Suspend(Op.Fail, Cause.die(e), null);
        }
        continue loop;
      }

      case Op.Fail: {
        let cause = cur.a as Cause;
        while (k !== null) {
          const frame = k;
          k = frame.next;
          // An interrupting fiber runs no error handler while interruptible:
          // recovering would resume normal execution after the interrupt.
          // Handlers inside uninterruptible regions (finalizers) still run.
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause);
            if (f) {
              if (fiber.interrupting && fiber.interruptible) {
                cause = bypassHandler(cause);
                continue;
              }
              try {
                cur = (frame.fn as any)(f.value);
              } catch (e) {
                cur = new Suspend(Op.Fail, Cause.die(e), null);
              }
              continue loop;
            }
          }
          if (frame.op === Op.CatchAll) {
            if (fiber.interrupting && fiber.interruptible) {
              cause = bypassHandler(cause);
              continue;
            }
            try {
              cur = (frame.fn as any)(cause);
            } catch (e) {
              cur = new Suspend(Op.Fail, Cause.die(e), null);
            }
            continue loop;
          }
          if (frame.op === Op.Provide) {
            context = frame.fn as Context;
            continue;
          }
          if (frame.op === Op.SetInterruptible) {
            fiber.interruptible = frame.fn as unknown as boolean;
            // Deliver an interrupt that arrived during the region into the
            // propagating failure, keeping its typed errors and defects. Left
            // pending, the top-of-loop check would replace the next finalizer
            // this walk starts.
            if (fiber.interruptible && (fiber.interruptPending || fiber.interrupting)) {
              fiber.interruptPending = false;
              fiber.interrupting = true;
              cause = withInterrupt(cause);
            }
            continue;
          }
          if (frame.op === Op.EnsuringFrame) {
            const finalizer =
              typeof frame.fn === "function"
                ? exitFinalizer(frame.fn, { _tag: "Failure", cause })
                : (frame.fn as Suspend);
            if (finalizer === null || finalizer.op === Op.Succeed) continue;
            k = enterUninterruptible(fiber, k);
            cur = failAfterFinalizer(finalizer, cause);
            continue loop;
          }
          if (frame.op === Op.ScopeFrame) {
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            fiber.scope = oldScope;
            k = enterUninterruptible(fiber, k);
            cur = failAfterFinalizer(scope.close() as unknown as Suspend, cause);
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
        const register = cur.a as (
          resume: (value: any, onDiscard?: () => void) => void,
        ) => (() => void) | void;

        fiber.stack = k;
        fiber.context = context;
        fiber.state = FiberState.Suspended;
        const token = ++fiber.asyncToken;

        let resumed = false;
        try {
          const cancel = register((value: any, onDiscard?: () => void) => {
            if (resumed || fiber.asyncToken !== token || fiber.state === FiberState.Done) {
              // Nothing waits for this value any more: hand it back.
              if (typeof onDiscard === "function") onDiscard();
              return;
            }
            resumed = true;
            fiber.interruptHandle = null;
            fiber.current = value;
            fiber.handoffDiscard = typeof onDiscard === "function" ? onDiscard : null;
            fiber.state = FiberState.Ready;
            fiber.scheduler.schedule(() => runFiberLoop(fiber));
          });
          if (cancel && !resumed) {
            // A mismatch means register() interrupted this fiber before the
            // canceler could be installed.
            if (fiber.asyncToken === token) fiber.interruptHandle = cancel;
            else cancel();
          }
        } catch (error) {
          if (resumed || fiber.asyncToken !== token) return;
          resumed = true;
          fiber.state = FiberState.Running;
          cur = new Suspend(Op.Fail, Cause.die(error), null);
          continue loop;
        }
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
        const group = new ChildGroup(fiber, k, context);
        const results = new Array(len);
        for (let i = 0; i < len && !group.stopped; i++) {
          group.start(effects[i], (result) => {
            if (result.ok) results[i] = result.value;
            else group.childFailed(result.cause);
            if (!group.settle()) return;
            if (group.failure === null) group.resume(results);
            else group.fail(group.failure);
          });
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
        if (effects.length === 0) {
          cur = new Suspend(Op.Fail, Cause.die(new Error("race: empty input")), null);
          continue loop;
        }
        // The first child to settle wins, and the rest are interrupted.
        const group = new ChildGroup(fiber, k, context);
        let won = false;
        let value: unknown;
        for (let i = 0; i < effects.length && !group.stopped; i++) {
          group.start(effects[i], (result) => {
            if (!result.ok) group.childFailed(result.cause);
            else if (!group.stopped) {
              won = true;
              value = result.value;
              group.stop();
            }
            if (!group.settle()) return;
            if (!won) group.fail(group.failure!);
            else if (group.teardown === null) group.resume(value);
            else group.fail(null);
          });
        }
        return;
      }

      case Op.Ensuring: {
        // Push a frame; body runs on the same fiber. When the frame is popped
        // (success-path or fail-walk), we run the finalizer then propagate the
        // body's outcome.
        k = new Cont(Op.EnsuringFrame, cur.b /* finalizer */, k);
        cur = cur.a; /* body */
        continue loop;
      }

      case Op.AcqRel: {
        const acquire = cur.a;
        const release = cur.b as (a: any) => Eff<void, unknown>;

        // acquire, then register release on the innermost active scope.
        // Without an enclosing scoped(), fall back to a lazily-created
        // fiber-level scope — closed when the fiber completes (success,
        // failure, or interrupt paths all close fiber.scope). A finalizer
        // must never be silently dropped.
        k = new Cont(
          Op.FlatMap,
          (resource: any) => {
            if (!fiber.scope) fiber.scope = new Scope();
            fiber.scope.addFinalizer(() => release(resource));
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
        cur = cur.a; /* body */
        continue loop;
      }

      case Op.SetInterruptible: {
        const newValue = cur.b as boolean;
        const prev = fiber.interruptible;
        k = new Cont(Op.SetInterruptible, prev, k);
        fiber.interruptible = newValue;
        // An interruptible region entered after an interrupt (inside a
        // finalizer, say) is interrupted at once.
        if (newValue && !prev && fiber.interrupting) fiber.interruptPending = true;
        cur = cur.a;
        // uninterruptibleMask: the body is built from the interruptibility
        // the region was entered with.
        if (typeof cur === "function") {
          try {
            cur = cur(prev);
          } catch (e) {
            cur = new Suspend(Op.Fail, Cause.die(e), null);
          }
        }
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

// Structured children of a fiber parked in all() or race(). The fiber resumes
// only once every child has settled, on success, on a child failure and on
// interrupt, so no child is still running its finalizers when the fiber's own
// continuation or finalizers run.
//
// The parked stack is SetInterruptible(false) -> CatchAll(onCause) ->
// SetInterruptible(caller's flag) -> k. While parked the fiber keeps the
// caller's interruptibility: interrupt() reaches it and its handle interrupts
// the children at once. A failure leaving the wait passes the `false` restore
// frame before onCause, so onCause runs even for an interrupted fiber under
// sticky interruption: it waits for the children, and the interrupt is raised
// again when the region ends.
//
// Causes: a child failure that stopped the group comes first; non-interrupt
// failures of children torn down after that (a finalizer that dies, say) are
// joined with Cause.both; an interrupt of the parked fiber goes in front of
// both.
class ChildGroup {
  running = 0;
  stopped = false;
  interrupted = false;
  failure: Cause | null = null;
  teardown: Cause | null = null;
  private delivered: Cause | null = null;
  private drained: (() => void) | null = null;
  private readonly children: Fiber<any>[] = [];
  private readonly token: number;

  constructor(
    private readonly fiber: Fiber<any>,
    k: Cont | null,
    private readonly context: Context,
  ) {
    this.token = ++fiber.asyncToken;
    fiber.stack = new Cont(
      Op.SetInterruptible,
      false,
      new Cont(Op.CatchAll, this.onCause, new Cont(Op.SetInterruptible, fiber.interruptible, k)),
    );
    fiber.context = context;
    fiber.state = FiberState.Suspended;
    fiber.interruptHandle = this.interruptChildren;
  }

  start(effect: unknown, onDone: (result: FiberResult<any>) => void): void {
    const child = makeChild(this.fiber, effect, this.context);
    this.children.push(child);
    this.running++;
    child.onComplete(onDone);
    runChild(child);
    if (this.stopped) child.interrupt();
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    const children = this.children;
    for (let i = 0; i < children.length; i++) children[i]!.interrupt();
  }

  // The first failure stops the group; a later one is teardown, where the
  // interrupt it was stopped with is expected.
  childFailed(cause: Cause): void {
    if (!this.stopped) {
      this.failure = cause;
      this.stop();
      return;
    }
    const extra = Cause.stripInterrupts(cause);
    if (extra !== null)
      this.teardown = this.teardown === null ? extra : Cause.both(this.teardown, extra);
  }

  // Records that a child settled. True when it was the last one and the group
  // should resume the fiber with its outcome.
  settle(): boolean {
    if (--this.running > 0) return false;
    const drained = this.drained;
    if (drained !== null) {
      this.drained = null;
      drained();
      return false;
    }
    return (
      !this.interrupted &&
      this.fiber.asyncToken === this.token &&
      this.fiber.state !== FiberState.Done
    );
  }

  resume(value: unknown): void {
    const fiber = this.fiber;
    fiber.interruptHandle = null;
    fiber.current = value;
    fiber.state = FiberState.Ready;
    fiber.scheduler.schedule(() => runFiberLoop(fiber));
  }

  // Fails with `first` (null when a winner succeeded) and the teardown.
  fail(first: Cause | null): void {
    const teardown = this.teardown;
    const cause =
      first === null ? teardown! : teardown === null ? first : Cause.both(first, teardown);
    this.delivered = cause;
    this.resume(new Suspend(Op.Fail, cause, null));
  }

  private readonly interruptChildren = (): void => {
    this.interrupted = true;
    this.stop();
  };

  private settledCause(cause: Cause): Suspend {
    let combined = cause;
    if (this.failure !== null) combined = Cause.both(combined, this.failure);
    if (this.teardown !== null) combined = Cause.both(combined, this.teardown);
    return new Suspend(Op.Fail, combined, null);
  }

  private readonly onCause = (cause: Cause): Suspend => {
    // Once the group delivered a failure every child has settled. An interrupt
    // that reached the fiber before it ran joined that failure; it gets the
    // same shape as an interrupt that arrives while the group waits.
    if (this.delivered !== null) {
      return cause === this.delivered
        ? new Suspend(Op.Fail, cause, null)
        : this.settledCause(Cause.interrupt());
    }
    this.interruptChildren();
    if (this.running === 0) return this.settledCause(cause);
    return new Suspend(
      Op.Async,
      (resume: (value: Suspend) => void) => {
        // Checked again here, so a child that settled in between cannot leave
        // the wait without a drain signal.
        if (this.running === 0) resume(this.settledCause(cause));
        else this.drained = () => resume(this.settledCause(cause));
      },
      null,
    );
  };
}

// Drives cleanup through resolve/reject callbacks without completing the
// original fiber again. Runs supported opcodes inline; Async resumes here,
// while other opcodes delegate to a detached fiber with the same context.
// parentFiber supplies scheduler/interruption state, not child ownership.
// The separate loop keeps cleanup out of the normal completion path; changes
// to shared opcode semantics must be checked in both interpreters.
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
            // a throwing continuation (user callback in .map/.flatMap) is a
            // defect, not an interpreter crash
            try {
              cur = (frame.fn as any)(cur);
            } catch (e) {
              cur = new Suspend(Op.Fail, Cause.die(e), null);
            }
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
            const finalizer =
              typeof frame.fn === "function"
                ? exitFinalizer(frame.fn, { _tag: "Success", value })
                : (frame.fn as Suspend);
            if (finalizer === null) continue;
            cur = succeedAfterFinalizer(finalizer, value);
            continue loop;
          }
          case Op.ScopeFrame: {
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            if (parentFiber) parentFiber.scope = oldScope;
            const value = cur;
            cur = succeedAfterFinalizer(scope.close() as unknown as Suspend, value);
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
          cur = (cur.a as any)();
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
              try {
                cur = (frame.fn as any)(f.value);
              } catch (e) {
                cur = new Suspend(Op.Fail, Cause.die(e), null);
              }
              continue loop;
            }
          }
          if (frame.op === Op.CatchAll) {
            try {
              cur = (frame.fn as any)(cause);
            } catch (e) {
              cur = new Suspend(Op.Fail, Cause.die(e), null);
            }
            continue loop;
          }
          if (frame.op === Op.Provide) context = frame.fn as Context;
          if (frame.op === Op.SetInterruptible && parentFiber) {
            parentFiber.interruptible = frame.fn as unknown as boolean;
          }
          if (frame.op === Op.EnsuringFrame) {
            const finalizer =
              typeof frame.fn === "function"
                ? exitFinalizer(frame.fn, { _tag: "Failure", cause })
                : (frame.fn as Suspend);
            if (finalizer === null) continue;
            cur = failAfterFinalizer(finalizer, cause);
            continue loop;
          }
          if (frame.op === Op.ScopeFrame) {
            const { scope, oldScope } = frame.fn as { scope: Scope; oldScope: Scope | null };
            if (parentFiber) parentFiber.scope = oldScope;
            cur = failAfterFinalizer(scope.close() as unknown as Suspend, cause);
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
        const body = cur.a as Suspend;
        const finalizer = cur.b;
        const finalizerFor = (exit: Exit<unknown, unknown>): Suspend =>
          typeof finalizer === "function"
            ? (exitFinalizer(finalizer, exit) ?? new Suspend(Op.Succeed, undefined, null))
            : (finalizer as Suspend);
        stepInline(
          body,
          context,
          null,
          (val) => {
            stepInline(
              finalizerFor({ _tag: "Success", value: val }),
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
              (finalizerCause) => {
                stepInline(
                  new Suspend(Op.Fail, finalizerCause, null),
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
              finalizerFor({ _tag: "Failure", cause }),
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
              (finalizerCause) => {
                stepInline(
                  new Suspend(Op.Fail, Cause.then(cause, finalizerCause), null),
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
        const register = cur.a as (resume: (value: any) => void) => unknown;
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
        const child = bootstrapFiber<any>(cur as Eff<any, any>, parentFiber?.scheduler);
        child.context = context;
        // stepInline only runs cleanup. Keep the delegated fiber in the state a
        // finalizer runs in on the owner itself: uninterruptible, and
        // interrupting if the owner was interrupted.
        child.interruptible = false;
        if (parentFiber) child.interrupting = parentFiber.interrupting;
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
/**
 * The "literal-leaf fast path" — used by run/runSync/runExit/runSafe and
 * the thenable shim — always inlines the same two-line check:
 *
 *   if (node.op === Op.Succeed) → resolve with node.a
 *   if (node.op === Op.Fail)    → reject with Cause.squash(node.a)
 *
 * Why not factor it into a helper? Each callsite formats the result
 * differently (Promise, throw, {data, error}, Exit) and a generic helper
 * would either allocate (slow) or use mutable globals (yuck). Hot path
 * stays inline; this comment is the single source of intent.
 *
 * The restriction to literal leaves matches Op.All's fast-path: anything
 * richer might run Op.Sync callbacks then bail on a downstream async op,
 * leaking side effects that the slow path then re-executes.
 */

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
function makeChild(parent: Fiber<any>, eff: any, context: Context, structured = true): Fiber<any> {
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
  notifyFiberStart(child);
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
    // Run the loop synchronously instead of going through scheduler.schedule.
    // Saves one microtask hop. If the fiber completes inline, the onComplete
    // listener fires synchronously and the Promise resolves on the next
    // microtask anyway (per Promise spec). If the fiber suspends on an
    // async op, runFiberLoop registers the resume callback and returns.
    notifyFiberStart(fiber);
    runFiberLoop(fiber);
  });
}

export function run<A, S>(eff: Eff<A, S> & EffectCheck<S>, scheduler?: Scheduler): Promise<A> {
  // Literal-leaf fast path — see top-of-file comment for rationale.
  const node = eff as any;
  if (node.op === Op.Succeed) return Promise.resolve(node.a);
  if (node.op === Op.Fail) return Promise.reject(Cause.squash(node.a));
  return startFiberPromise<A, A>(eff, scheduler, (r, resolve, reject) => {
    if (r.ok) resolve(r.value);
    else reject(Cause.squash(r.cause));
  });
}

export function runSync<A>(eff: Eff<A, never>): A {
  // Literal-leaf fast path — see top-of-file comment for rationale.
  const node = eff as any;
  if (node.op === Op.Succeed) return node.a as A;
  if (node.op === Op.Fail) throw Cause.squash(node.a);

  let result: A | undefined;
  let error: Cause | undefined;
  let done = false;

  const scheduler = new SyncScheduler();
  const fiber = bootstrapFiber<A>(eff as Eff<A, any>, scheduler);
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

export function runFiber<A, S>(eff: Eff<A, S> & EffectCheck<S>, scheduler?: Scheduler): Fiber<A> {
  const fiber = bootstrapFiber<A>(eff as Eff<A, any>, scheduler);
  notifyFiberStart(fiber);
  fiber.scheduler.schedule(() => runFiberLoop(fiber));
  return fiber;
}

/**
 * Run an effect and return its Exit. Never throws — on failure the Cause is
 * wrapped as `{ _tag: "Failure", cause }`, on success as `{ _tag: "Success", value }`.
 *
 * Use this when you want to pattern-match on success vs every flavour of
 * failure (typed error, defect, interrupt) and inspect the full Cause tree.
 */
export function runExit<A>(eff: Eff<A, unknown>, scheduler?: Scheduler): Promise<Exit<unknown, A>> {
  // Literal-leaf fast path — see top-of-file comment for rationale.
  const node = eff as any;
  if (node.op === Op.Succeed) return Promise.resolve({ _tag: "Success" as const, value: node.a });
  if (node.op === Op.Fail) return Promise.resolve({ _tag: "Failure" as const, cause: node.a });
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
  eff: Eff<A, unknown>,
  opts: { catchDefects: true },
  scheduler?: Scheduler,
): Promise<{ data: A; error: null } | { data: null; error: E | unknown }>;
export function runSafe<A, E = unknown>(
  eff: Eff<A, unknown>,
  opts?: { catchDefects?: false },
  scheduler?: Scheduler,
): Promise<{ data: A; error: null } | { data: null; error: E }>;
export function runSafe<A>(
  eff: Eff<A, unknown>,
  opts: { catchDefects?: boolean } = {},
  scheduler?: Scheduler,
): Promise<{ data: A; error: null } | { data: null; error: unknown }> {
  // Literal-leaf fast path — see top-of-file comment for rationale.
  const node = eff as any;
  if (node.op === Op.Succeed) return Promise.resolve({ data: node.a, error: null });
  if (node.op === Op.Fail) {
    const cause = node.a;
    const typedFail = Cause.firstFail(cause);
    if (typedFail !== null) return Promise.resolve({ data: null, error: typedFail.value });
    if (opts.catchDefects) return Promise.resolve({ data: null, error: Cause.squash(cause) });
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
