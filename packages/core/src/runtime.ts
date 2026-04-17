import { Cause } from "./cause"
import { type Eff, Suspend, Cont, Op } from "./eff"
import { type Context, emptyContext, mergeContexts } from "./service"
import { Fiber, FiberState } from "./fiber"
import { Scope } from "./scope"
import { type Scheduler, BunScheduler, SyncScheduler, DEFAULT_BUDGET, getDefaultScheduler } from "./scheduler"
import { succeed } from "./constructors"

type Resolve = (value: any) => void
type Reject = (cause: Cause) => void

// Try to evaluate a node fully synchronously without spawning a fiber.
// Returns:
//   { ok: true, value }   — completed sync with a value
//   { ok: false, cause }  — completed sync with a cause
//   null                  — needs the fiber runtime (Async, Fork, Race, nested All, etc.)
function evalSync(
  node: Suspend,
  ctx: Context,
): { ok: true; value: any } | { ok: false; cause: Cause } | null {
  let cur: any = node
  let context = ctx
  let k: Cont | null = null

  loop: while (true) {
    if (!(cur instanceof Suspend)) {
      while (k !== null) {
        const frame = k
        k = frame.next
        switch (frame.op) {
          case Op.FlatMap: { cur = frame.fn(cur); continue loop }
          case Op.Catch:
          case Op.CatchAll: { continue }
          case Op.Provide: { context = frame.fn as Context; continue }
          case Op.SetInterruptible: { continue }
        }
      }
      return { ok: true, value: cur }
    }

    switch (cur.op) {
      case Op.Succeed: { cur = cur.a; continue loop }
      case Op.Sync: {
        try { cur = cur.a() } catch (e) { cur = new Suspend(Op.Fail, Cause.die(e), null) }
        continue loop
      }
      case Op.Fail: {
        const cause = cur.a as Cause
        while (k !== null) {
          const frame = k
          k = frame.next
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause)
            if (f) { cur = frame.fn(f.value); continue loop }
          }
          if (frame.op === Op.CatchAll) { cur = frame.fn(cause); continue loop }
          if (frame.op === Op.Provide) { context = frame.fn as Context; continue }
          if (frame.op === Op.SetInterruptible) { continue }
        }
        return { ok: false, cause }
      }
      case Op.FlatMap:  { k = new Cont(Op.FlatMap, cur.b, k);  cur = cur.a; continue loop }
      case Op.Catch:    { k = new Cont(Op.Catch, cur.b, k);    cur = cur.a; continue loop }
      case Op.CatchAll: { k = new Cont(Op.CatchAll, cur.b, k); cur = cur.a; continue loop }
      case Op.Provide: {
        k = new Cont(Op.Provide, context, k)
        context = mergeContexts(context, cur.b as Context)
        cur = cur.a
        continue loop
      }
      case Op.GetCtx: {
        const key = cur.a as symbol
        const val = context.get(key)
        if (val === undefined) {
          return { ok: false, cause: Cause.die(new Error(`Service not provided: ${key.description}`)) }
        }
        cur = val
        continue loop
      }
      // Anything else (Async, Fork, ForkDaemon, Race, All, Ensuring, Scoped,
      // AcqRel, GetScope, SetInterruptible, YieldNow) needs the fiber runtime.
      default: return null
    }
  }
}

function runFiberLoop(fiber: Fiber): void {
  if (fiber.state === FiberState.Done) return
  fiber.state = FiberState.Running
  fiber.opCount = 0

  let cur: any = fiber.current
  let k: Cont | null = fiber.stack
  let context: Context = fiber.context!
  const budget = DEFAULT_BUDGET

  const resolve: Resolve = (value) => {
    fiber.complete({ ok: true, value })
  }
  const reject: Reject = (cause) => {
    // close scope if present
    if (fiber.scope && !fiber.scope.isClosed) {
      const closer = fiber.scope.close()
      stepInline(closer as unknown as Suspend, context, null, () => {
        fiber.complete({ ok: false, cause })
      }, () => {
        fiber.complete({ ok: false, cause })
      }, fiber)
      return
    }
    fiber.complete({ ok: false, cause })
  }

  loop: while (true) {
    // check interruption
    if (fiber.state === FiberState.Done) {
      return
    }

    // honour any pending interrupt the moment we're in interruptible mode
    if (fiber.interruptible && fiber.interruptPending) {
      fiber.interruptPending = false
      cur = new Suspend(Op.Fail, Cause.interrupt(), null)
    }

    // op budget — yield to scheduler
    if (++fiber.opCount > budget) {
      fiber.current = cur
      fiber.stack = k
      fiber.context = context
      fiber.state = FiberState.Ready
      fiber.scheduler.schedule(() => runFiberLoop(fiber))
      return
    }

    // pure value fast path
    if (!(cur instanceof Suspend)) {
      while (k !== null) {
        const frame = k
        k = frame.next
        switch (frame.op) {
          case Op.FlatMap: {
            cur = frame.fn(cur)
            continue loop
          }
          case Op.Catch:
          case Op.CatchAll: {
            continue
          }
          case Op.Provide: {
            context = frame.fn as Context
            continue
          }
          case Op.SetInterruptible: {
            fiber.interruptible = frame.fn as unknown as boolean
            if (fiber.interruptible && fiber.interruptPending) {
              fiber.interruptPending = false
              cur = new Suspend(Op.Fail, Cause.interrupt(), null)
              continue loop
            }
            continue
          }
        }
      }
      // close scope on success
      if (fiber.scope && !fiber.scope.isClosed) {
        const val = cur
        const closer = fiber.scope.close()
        stepInline(closer as unknown as Suspend, context, null, () => resolve(val), () => resolve(val), fiber)
        return
      }
      resolve(cur)
      return
    }

    switch (cur.op) {
      case Op.Succeed: {
        cur = cur.a
        continue loop
      }

      case Op.Sync: {
        try {
          cur = cur.a()
        } catch (e) {
          cur = new Suspend(Op.Fail, Cause.die(e), null)
        }
        continue loop
      }

      case Op.Fail: {
        const cause = cur.a as Cause
        while (k !== null) {
          const frame = k
          k = frame.next
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause)
            if (f) {
              cur = frame.fn(f.value)
              continue loop
            }
          }
          if (frame.op === Op.CatchAll) {
            cur = frame.fn(cause)
            continue loop
          }
          if (frame.op === Op.Provide) {
            context = frame.fn as Context
            continue
          }
          if (frame.op === Op.SetInterruptible) {
            fiber.interruptible = frame.fn as unknown as boolean
            continue
          }
        }
        reject(cause)
        return
      }

      case Op.FlatMap: {
        k = new Cont(Op.FlatMap, cur.b, k)
        cur = cur.a
        continue loop
      }

      case Op.Catch: {
        k = new Cont(Op.Catch, cur.b, k)
        cur = cur.a
        continue loop
      }

      case Op.CatchAll: {
        k = new Cont(Op.CatchAll, cur.b, k)
        cur = cur.a
        continue loop
      }

      case Op.Async: {
        const register = cur.a as (
          resume: (value: any) => void,
        ) => (() => void) | void

        fiber.stack = k
        fiber.context = context
        fiber.state = FiberState.Suspended

        const cancel = register((value: any) => {
          if (fiber.state === FiberState.Done) return
          fiber.current = value
          fiber.state = FiberState.Ready
          fiber.scheduler.schedule(() => runFiberLoop(fiber))
        })
        if (cancel) fiber.interruptHandle = cancel
        return
      }

      case Op.Provide: {
        k = new Cont(Op.Provide, context, k)
        context = mergeContexts(context, cur.b as Context)
        cur = cur.a
        continue loop
      }

      case Op.GetCtx: {
        const key = cur.a as symbol
        const val = context.get(key)
        if (val === undefined) {
          cur = new Suspend(Op.Fail, Cause.die(new Error(`Service not provided: ${key.description}`)), null)
        } else {
          cur = val
        }
        continue loop
      }

      case Op.All: {
        const effects = cur.a as Suspend[]
        const len = effects.length
        if (len === 0) {
          cur = []
          continue loop
        }

        // Fast path: try to evaluate every child synchronously without spawning
        // a fiber. evalSync returns null the moment it hits anything async-y
        // (Async, Fork, Race, nested All, Ensuring, Scoped, AcqRel, GetScope,
        // SetInterruptible, YieldNow, ForkDaemon).
        const fastResults = new Array(len)
        let fastOK = true
        let fastFail: Cause | null = null
        for (let i = 0; i < len; i++) {
          const r = evalSync(effects[i] as Suspend, context)
          if (r === null) { fastOK = false; break }
          if (!r.ok) { fastFail = r.cause; break }
          fastResults[i] = r.value
        }
        if (fastOK) {
          if (fastFail !== null) {
            cur = new Suspend(Op.Fail, fastFail, null)
          } else {
            cur = fastResults
          }
          continue loop
        }

        // Slow path: full fiber-per-element parallel.
        fiber.stack = k
        fiber.context = context
        const savedCtx = context
        const results = new Array(len)
        let remaining = len
        let failed = false

        for (let i = 0; i < len; i++) {
          const child = new Fiber()
          child.current = effects[i]
          child.context = savedCtx
          child.scheduler = fiber.scheduler
          fiber.addChild(child)

          child.onComplete((result) => {
            if (failed) return
            if (result.ok) {
              results[i] = result.value
              if (--remaining === 0) {
                fiber.current = results
                fiber.state = FiberState.Ready
                fiber.scheduler.schedule(() => runFiberLoop(fiber))
              }
            } else {
              failed = true
              // interrupt siblings
              for (const c of fiber.children) if (c !== child) c.interrupt()
              fiber.current = new Suspend(Op.Fail, result.cause, null)
              fiber.state = FiberState.Ready
              fiber.scheduler.schedule(() => runFiberLoop(fiber))
            }
          })

          child.state = FiberState.Ready
          fiber.scheduler.schedule(() => runFiberLoop(child))
        }
        return
      }

      case Op.Fork: {
        const child = new Fiber()
        child.current = cur.a
        child.context = context
        child.scheduler = fiber.scheduler
        fiber.addChild(child)

        child.state = FiberState.Ready
        fiber.scheduler.schedule(() => runFiberLoop(child))

        cur = child
        continue loop
      }

      case Op.Race: {
        const effects = cur.a as Suspend[]
        fiber.stack = k
        fiber.context = context
        const savedCtx = context
        let settled = false
        const children: Fiber[] = []

        for (let i = 0; i < effects.length; i++) {
          const child = new Fiber()
          child.current = effects[i]
          child.context = savedCtx
          child.scheduler = fiber.scheduler
          children.push(child)
          fiber.addChild(child)

          child.onComplete((result) => {
            if (settled) return
            settled = true
            for (const c of children) if (c !== child) c.interrupt()
            if (result.ok) {
              fiber.current = result.value
            } else {
              fiber.current = new Suspend(Op.Fail, result.cause, null)
            }
            fiber.state = FiberState.Ready
            fiber.scheduler.schedule(() => runFiberLoop(fiber))
          })

          child.state = FiberState.Ready
          fiber.scheduler.schedule(() => runFiberLoop(child))
        }
        return
      }

      case Op.Ensuring: {
        const body = cur.a as Suspend
        const finalizer = cur.b as Suspend

        fiber.stack = k
        fiber.context = context
        const savedCtx = context

        const child = new Fiber()
        child.current = body
        child.context = savedCtx
        child.scheduler = fiber.scheduler
        // link so parent interrupt propagates into the body
        fiber.addChild(child)

        const resume = (result: { ok: true; value: any } | { ok: false; cause: Cause }) => {
          const cont = () => {
            if (fiber.state === FiberState.Done) return
            fiber.current = result.ok
              ? result.value
              : new Suspend(Op.Fail, result.cause, null)
            fiber.state = FiberState.Ready
            fiber.scheduler.schedule(() => runFiberLoop(fiber))
          }
          stepInline(finalizer, savedCtx, null, cont, cont, fiber)
        }

        child.onComplete(resume)

        child.state = FiberState.Ready
        fiber.scheduler.schedule(() => runFiberLoop(child))
        return
      }

      case Op.AcqRel: {
        const acquire = cur.a
        const release = cur.b as (a: any) => Eff<void, never>

        // acquire, then register release on the fiber's scope
        k = new Cont(Op.FlatMap, (resource: any) => {
          if (fiber.scope) {
            fiber.scope.addFinalizer(() => release(resource))
          }
          return new Suspend(Op.Succeed, resource, null)
        }, k)
        cur = acquire
        continue loop
      }

      case Op.GetScope: {
        if (!fiber.scope) fiber.scope = new Scope()
        cur = fiber.scope
        continue loop
      }

      case Op.Scoped: {
        const body = cur.a
        const scope = new Scope()

        fiber.stack = k
        fiber.context = context
        const savedCtx = context

        const child = new Fiber()
        child.current = body
        child.context = savedCtx
        child.scheduler = fiber.scheduler
        child.scope = scope
        fiber.addChild(child)

        const resume = (result: { ok: true; value: any } | { ok: false; cause: Cause }) => {
          const closer = scope.close()
          const cont = () => {
            if (fiber.state === FiberState.Done) return
            fiber.current = result.ok
              ? result.value
              : new Suspend(Op.Fail, result.cause, null)
            fiber.state = FiberState.Ready
            fiber.scheduler.schedule(() => runFiberLoop(fiber))
          }
          stepInline(closer as unknown as Suspend, savedCtx, null, cont, cont, fiber)
        }

        child.onComplete(resume)

        child.state = FiberState.Ready
        fiber.scheduler.schedule(() => runFiberLoop(child))
        return
      }

      case Op.SetInterruptible: {
        const newValue = cur.b as boolean
        const prev = fiber.interruptible
        k = new Cont(Op.SetInterruptible, prev, k)
        fiber.interruptible = newValue
        cur = cur.a
        continue loop
      }

      case Op.YieldNow: {
        fiber.current = new Suspend(Op.Succeed, undefined, null)
        fiber.stack = k
        fiber.context = context
        fiber.state = FiberState.Ready
        fiber.scheduler.schedule(() => runFiberLoop(fiber))
        return
      }

      case Op.ForkDaemon: {
        const child = new Fiber()
        child.current = cur.a
        child.context = context
        child.scheduler = fiber.scheduler
        // NOT addChild — daemons outlive their parent
        child.state = FiberState.Ready
        fiber.scheduler.schedule(() => runFiberLoop(child))
        cur = child
        continue loop
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
  let cur: any = node
  let context = ctx
  let k: Cont | null = stack

  loop: while (true) {
    if (!(cur instanceof Suspend)) {
      while (k !== null) {
        const frame = k
        k = frame.next
        switch (frame.op) {
          case Op.FlatMap: { cur = frame.fn(cur); continue loop }
          case Op.Catch: case Op.CatchAll: { continue }
          case Op.Provide: { context = frame.fn as Context; continue }
          case Op.SetInterruptible: {
            if (parentFiber) parentFiber.interruptible = frame.fn as unknown as boolean
            continue
          }
        }
      }
      resolve(cur)
      return
    }

    switch (cur.op) {
      case Op.Succeed: { cur = cur.a; continue loop }
      case Op.Sync: {
        try { cur = cur.a() } catch (e) { cur = new Suspend(Op.Fail, Cause.die(e), null) }
        continue loop
      }
      case Op.Fail: {
        const cause = cur.a as Cause
        while (k !== null) {
          const frame = k; k = frame.next
          if (frame.op === Op.Catch) {
            const f = Cause.firstFail(cause)
            if (f) { cur = frame.fn(f.value); continue loop }
          }
          if (frame.op === Op.CatchAll) { cur = frame.fn(cause); continue loop }
          if (frame.op === Op.Provide) context = frame.fn as Context
          if (frame.op === Op.SetInterruptible && parentFiber) {
            parentFiber.interruptible = frame.fn as unknown as boolean
          }
        }
        reject(cause)
        return
      }
      case Op.FlatMap: { k = new Cont(Op.FlatMap, cur.b, k); cur = cur.a; continue loop }
      case Op.Catch: { k = new Cont(Op.Catch, cur.b, k); cur = cur.a; continue loop }
      case Op.CatchAll: { k = new Cont(Op.CatchAll, cur.b, k); cur = cur.a; continue loop }
      case Op.Ensuring: {
        const body = cur.a; const finalizer = cur.b
        stepInline(body, context, null, (val) => {
          stepInline(finalizer, context, null, () => {
            stepInline(new Suspend(Op.Succeed, val, null), context, k, resolve, reject, parentFiber)
          }, () => {
            stepInline(new Suspend(Op.Succeed, val, null), context, k, resolve, reject, parentFiber)
          }, parentFiber)
        }, (cause) => {
          stepInline(finalizer, context, null, () => {
            stepInline(new Suspend(Op.Fail, cause, null), context, k, resolve, reject, parentFiber)
          }, () => {
            stepInline(new Suspend(Op.Fail, cause, null), context, k, resolve, reject, parentFiber)
          }, parentFiber)
        }, parentFiber)
        return
      }
      case Op.Async: {
        // async in finalizer context — still need to handle it
        const register = cur.a
        register((value: any) => {
          stepInline(value instanceof Suspend ? value : new Suspend(Op.Succeed, value, null), context, k, resolve, reject, parentFiber)
        })
        return
      }
      default: {
        // for ops we can't inline (Fork, All, etc.), delegate to fiber runtime
        const child = new Fiber()
        child.current = cur
        child.context = context
        child.scheduler = parentFiber?.scheduler ?? getDefaultScheduler()
        child.onComplete((result) => {
          if (result.ok) {
            stepInline(new Suspend(Op.Succeed, result.value, null), context, k, resolve, reject, parentFiber)
          } else {
            stepInline(new Suspend(Op.Fail, result.cause, null), context, k, resolve, reject, parentFiber)
          }
        })
        child.state = FiberState.Ready
        child.scheduler.schedule(() => runFiberLoop(child))
        return
      }
    }
  }
}

// ── Public API ─────────────────────────────────��───────────────────

export function run<A>(eff: Eff<A, any>, scheduler?: Scheduler): Promise<A> {
  return new Promise<A>((resolve, reject) => {
    const fiber = new Fiber<A>()
    fiber.current = eff
    fiber.context = emptyContext
    fiber.scheduler = scheduler ?? getDefaultScheduler()

    fiber.onComplete((result) => {
      if (result.ok) resolve(result.value)
      else reject(Cause.squash(result.cause))
    })

    fiber.state = FiberState.Ready
    fiber.scheduler.schedule(() => runFiberLoop(fiber))
  })
}

export function runSync<A>(eff: Eff<A, never>): A {
  let result: A | undefined
  let error: Cause | undefined
  let done = false

  const scheduler = new SyncScheduler()
  const fiber = new Fiber<A>()
  fiber.current = eff
  fiber.context = emptyContext
  fiber.scheduler = scheduler

  fiber.onComplete((r) => {
    done = true
    if (r.ok) result = r.value
    else error = r.cause
  })

  fiber.state = FiberState.Ready
  scheduler.schedule(() => runFiberLoop(fiber))
  scheduler.flush()

  if (!done) {
    throw new Error("runSync: effect did not complete synchronously")
  }

  if (error !== undefined) {
    throw Cause.squash(error)
  }

  return result as A
}

export function runFiber<A>(eff: Eff<A, any>, scheduler?: Scheduler): Fiber<A> {
  const fiber = new Fiber<A>()
  fiber.current = eff
  fiber.context = emptyContext
  fiber.scheduler = scheduler ?? getDefaultScheduler()

  fiber.state = FiberState.Ready
  fiber.scheduler.schedule(() => runFiberLoop(fiber))

  return fiber
}
