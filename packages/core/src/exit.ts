import { Cause } from "./cause"

export type Exit<E = unknown, A = unknown> =
  | { readonly _tag: "Success"; readonly value: A }
  | { readonly _tag: "Failure"; readonly cause: Cause<E> }

export const Exit = {
  succeed: <A>(value: A): Exit<never, A> =>
    ({ _tag: "Success", value }),

  failure: <E>(cause: Cause<E>): Exit<E, never> =>
    ({ _tag: "Failure", cause }),

  fail: <E>(error: E): Exit<E, never> =>
    ({ _tag: "Failure", cause: Cause.fail(error) }),

  die: (defect: unknown): Exit<never, never> =>
    ({ _tag: "Failure", cause: Cause.die(defect) }),

  interrupt: (): Exit<never, never> =>
    ({ _tag: "Failure", cause: Cause.interrupt() }),

  isSuccess: <E, A>(e: Exit<E, A>): e is { _tag: "Success"; value: A } =>
    e._tag === "Success",

  isFailure: <E, A>(e: Exit<E, A>): e is { _tag: "Failure"; cause: Cause<E> } =>
    e._tag === "Failure",

  isInterrupted: <E, A>(e: Exit<E, A>): boolean =>
    e._tag === "Failure" && Cause.isInterruptedOnly(e.cause),

  map: <E, A, B>(e: Exit<E, A>, f: (a: A) => B): Exit<E, B> =>
    e._tag === "Success" ? Exit.succeed(f(e.value)) : e,

  mapError: <E, A, E2>(e: Exit<E, A>, f: (e: E) => E2): Exit<E2, A> => {
    if (e._tag === "Success") return e
    const out = Cause.failures(e.cause).map(f)
    // rebuild: keep defects/interrupts structure; replace Fail leaves
    const rebuild = (c: Cause<E>, idx: { i: number }): Cause<E2> => {
      switch (c._tag) {
        case "Fail":      return Cause.fail(out[idx.i++]!)
        case "Die":       return c as Cause<E2>
        case "Interrupt": return c
        case "Both":      return Cause.both(rebuild(c.left, idx), rebuild(c.right, idx))
        case "Then":      return Cause.then(rebuild(c.left, idx), rebuild(c.right, idx))
      }
    }
    return Exit.failure(rebuild(e.cause, { i: 0 }))
  },

  match: <E, A, B>(
    e: Exit<E, A>,
    onSuccess: (a: A) => B,
    onFailure: (cause: Cause<E>) => B,
  ): B => e._tag === "Success" ? onSuccess(e.value) : onFailure(e.cause),
} as const
