// FiberLocal — a value that follows the code running inside a region,
// including every fiber that code forks, without passing it by hand.
//
//   const RequestId = FiberLocal.make<string | undefined>(undefined, { logAs: "requestId" });
//
//   const handle = (request: Request) =>
//     RequestId.locally(request.headers.get("x-request-id") ?? crypto.randomUUID(),
//       loadUser(request));               // loadUser and what it forks see the id
//
//   const loadUser = (request: Request) =>
//     RequestId.get.flatMap((id) => ...); // and Log lines inside carry requestId
//
// The value is set for a region (locally), not changed in place, so it can't
// leak out of the region or into other requests. Values live in the effect
// context, like log annotations, so forked fibers inherit them.

import { type Eff, Suspend, Op } from "./eff.js";
import { LOG_ANNOTATIONS_KEY } from "./logger.js";

/** The context key holding every FiberLocal's current value, by local. */
export const FIBER_LOCALS_KEY = Symbol.for("spilne/svc/FiberLocals");

type Locals = ReadonlyMap<symbol, unknown>;

const getLocals: Eff<Locals, never> = new Suspend(Op.GetCtx, FIBER_LOCALS_KEY, null) as any;

export interface FiberLocal<A> {
  /** The value for the current region, or the initial value outside any. */
  readonly get: Eff<A, never>;
  /** Run `eff` (and the fibers it forks) with this local set to `value`. */
  locally<B, S>(value: A, eff: Eff<B, S>): Eff<B, S>;
  /** Run `eff` with this local changed by `f`, e.g. to append to a list. */
  locallyWith<B, S>(f: (current: A) => A, eff: Eff<B, S>): Eff<B, S>;
}

export interface FiberLocalOptions {
  /** A name for debugging. */
  readonly name?: string;
  /**
   * Also add the value to log annotations under this name inside the
   * region, so every `Log` line there carries it.
   */
  readonly logAs?: string;
}

export const FiberLocal = {
  make<A>(initial: A, options: FiberLocalOptions = {}): FiberLocal<A> {
    const id = Symbol(options.name ?? options.logAs ?? "FiberLocal");
    const valueIn = (locals: Locals): A => (locals.has(id) ? (locals.get(id) as A) : initial);

    const get: Eff<A, never> = new Suspend(
      Op.FlatMap,
      getLocals,
      (locals: Locals) => new Suspend(Op.Succeed, valueIn(locals), null),
    ) as any;

    const locallyWith = <B, S>(f: (current: A) => A, eff: Eff<B, S>): Eff<B, S> =>
      new Suspend(Op.FlatMap, getLocals, (locals: Locals) => {
        const value = f(valueIn(locals));
        const provided = new Map<symbol, unknown>([
          [FIBER_LOCALS_KEY, new Map(locals).set(id, value)],
        ]);
        if (options.logAs === undefined) return new Suspend(Op.Provide, eff, provided);
        return new Suspend(
          Op.FlatMap,
          new Suspend(Op.GetCtx, LOG_ANNOTATIONS_KEY, null),
          (annotations: Record<string, unknown>) => {
            provided.set(LOG_ANNOTATIONS_KEY, { ...annotations, [options.logAs!]: value });
            return new Suspend(Op.Provide, eff, provided);
          },
        );
      }) as any;

    return {
      get,
      locally: (value, eff) => locallyWith(() => value, eff),
      locallyWith,
    };
  },
} as const;
