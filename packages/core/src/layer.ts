// Lean "layer" — just an Eff that produces a record of services.
//
//   const DbLive    : Layer<{ Db: Db }>          = succeed({ Db: new FakeDb() });
//   const CacheLive : Layer<{ Cache: Cache }>    = succeed({ Cache: new LruCache() });
//   const LoggerLive: Layer<{ Logger: Logger }>  = scoped(
//     acquireRelease(openLog, closeLog).map((l) => ({ Logger: l })),
//   );
//
//   const AppLive = Layer.merge(DbLive, CacheLive, LoggerLive);
//   await run(program.with(AppLive));
//
// The record key is the service name (matches `service<T>("Db")`'s name).
// At .with() time we recover the tag's Symbol via Symbol.for — no global
// registry needed because `service` already does that.
//
// Scope: .with(layer) wraps the layer build + program run in a `scoped`
// frame. Any `acquireRelease` inside the layer has its release tied to
// that scope, so resources clean up after the program exits.

import { type Eff, type Needs, Suspend, Op } from "./eff";
import { scoped } from "./constructors";

// ── Type alias ─────────────────────────────────────────────────────

/**
 * A layer is an Eff that yields a record of services.
 * `Services` — record mapping service name → impl
 * `E` — effects needed to build the layer (errors + deps)
 */
export type Layer<Services extends Record<string, any>, E = never> = Eff<Services, E>;

// ── merge ──────────────────────────────────────────────────────────

type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (x: infer I) => void
  ? I
  : never;

type MergedServices<L extends readonly Layer<any, any>[]> =
  UnionToIntersection<L[number] extends Eff<infer S, any> ? S : never> extends infer S
    ? S extends Record<string, any>
      ? S
      : Record<string, any>
    : Record<string, any>;

type MergedEffects<L extends readonly Layer<any, any>[]> =
  L[number] extends Eff<any, infer E> ? E : never;

/**
 * Combine multiple layers horizontally. Each layer is built in sequence;
 * their service records are merged (later keys win on collision).
 */
export function merge<L extends readonly Layer<any, any>[]>(
  ...layers: L
): Layer<MergedServices<L>, MergedEffects<L>> {
  if (layers.length === 0) {
    return new Suspend(Op.Succeed, {}, null) as any;
  }
  // Reduce: build layers left-to-right, spread into accumulator.
  let acc: any = new Suspend(Op.Succeed, {}, null);
  for (const layer of layers) {
    const prev = acc;
    acc = new Suspend(
      Op.FlatMap,
      prev,
      (accServices: any) =>
        new Suspend(
          Op.FlatMap,
          layer,
          (layerServices: any) =>
            new Suspend(Op.Succeed, { ...accServices, ...layerServices }, null),
        ),
    );
  }
  return acc;
}

// ── .with() method ─────────────────────────────────────────────────

declare module "./eff" {
  interface Suspend {
    /**
     * Apply a layer — run the layer, install each service in context,
     * then run the program. Wraps in `scoped` so acquireRelease finalizers
     * inside the layer clean up when the program exits.
     *
     * Chainable: `program.with(DbLive).with(CacheLive).with(LoggerLive)`.
     */
    with<A, S, Services extends Record<string, any>, E>(
      this: Eff<A, S>,
      layer: Layer<Services, E>,
    ): Eff<A, Exclude<S, Needs<Services[keyof Services]>> | E>;

    /**
     * Horizontal merge for layers — chain two layers together.
     * `DbLive.and(CacheLive).and(LoggerLive)` ≡ `Layer.merge(DbLive, CacheLive, LoggerLive)`.
     */
    and<S1 extends Record<string, any>, E1, S2 extends Record<string, any>, E2>(
      this: Layer<S1, E1>,
      other: Layer<S2, E2>,
    ): Layer<S1 & S2, E1 | E2>;

    /**
     * Vertical composition — feed this layer's services into `inner`'s
     * dependencies. Result is a layer producing `inner`'s services; `this`'s
     * services are consumed (not visible in the output).
     *
     * Example: `DbLive.provideTo(CacheLive)` — Cache is built using Db.
     */
    provideTo<S1 extends Record<string, any>, E1, S2 extends Record<string, any>, E2>(
      this: Layer<S1, E1>,
      inner: Layer<S2, E2>,
    ): Layer<S2, E1 | Exclude<E2, Needs<S1[keyof S1]>>>;
  }
}

function servicesToContext(services: Record<string, unknown>): Map<symbol, unknown> {
  const ctx = new Map<symbol, unknown>();
  for (const name of Object.keys(services)) {
    ctx.set(Symbol.for(`spilne/svc/${name}`), services[name]);
  }
  return ctx;
}

Suspend.prototype.with = function (this: Suspend, layer: any): any {
  const program = this;
  const body = new Suspend(
    Op.FlatMap,
    layer,
    (services: any) => new Suspend(Op.Provide, program, servicesToContext(services)),
  );
  return scoped(body as any);
};

Suspend.prototype.and = function (this: Suspend, other: any): any {
  return merge(this as any, other);
};

Suspend.prototype.provideTo = function (this: Suspend, inner: any): any {
  // Build outer, install its services into context, then build inner.
  return new Suspend(
    Op.FlatMap,
    this,
    (outerServices: any) => new Suspend(Op.Provide, inner, servicesToContext(outerServices)),
  );
};

// ── Namespace export ───────────────────────────────────────────────

export const Layer = {
  merge,
} as const;
