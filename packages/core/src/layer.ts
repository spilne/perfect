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
import type { Scope } from "./scope";

// ── Type alias ─────────────────────────────────────────────────────

/**
 * A layer is an Eff that yields a record of services.
 * `Services` — record mapping service name → impl
 * `E` — effects needed to build the layer (errors + deps)
 *
 * The `Record<string, any>` constraints in this module are deliberate:
 * `Record<string, unknown>` would reject interface-typed service records
 * (interfaces have no implicit index signature). The `any` never escapes —
 * concrete Services types flow through unchanged.
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

export function memoize<Services extends Record<string, any>, E>(
  layer: Layer<Services, E>,
): Layer<Services, E> {
  const cache = new WeakMap<Scope, Services>();
  return new Suspend(Op.FlatMap, new Suspend(Op.GetScope, null, null), (scope: Scope) => {
    const cached = cache.get(scope);
    if (cached !== undefined) return new Suspend(Op.Succeed, cached, null);
    return new Suspend(Op.FlatMap, layer, (services: Services) => {
      cache.set(scope, services);
      return new Suspend(Op.Succeed, services, null);
    });
  }) as any;
}

// ── Auto-wiring ────────────────────────────────────────────────────
//
// `merge` builds layers left-to-right in the order written, which means the
// author has to topologically sort by hand and gets a runtime
// "Service not provided" defect if they get it wrong.
//
// `Layer.build` does the sort instead. It needs each layer to declare what it
// provides and requires, because a layer is an opaque `Eff` — what it
// consumes is only visible in the type, and types are gone at runtime.
// `Layer.describe` attaches that declaration; undescribed layers are treated
// as providing nothing and requiring nothing, so they simply build first.

const LAYER_DEPS: unique symbol = Symbol.for("spilne/layer-deps");

interface LayerDeps {
  readonly provides: readonly string[];
  readonly requires: readonly string[];
}

export class LayerCycleError extends Error {
  constructor(readonly cycle: readonly string[]) {
    super(`Layer dependency cycle: ${cycle.join(" -> ")}`);
    this.name = "LayerCycleError";
  }
}

export class LayerMissingDependencyError extends Error {
  constructor(
    readonly service: string,
    readonly requiredBy: readonly string[],
  ) {
    super(
      `No layer provides "${service}" (required by ${requiredBy.map((s) => `"${s}"`).join(", ")})`,
    );
    this.name = "LayerMissingDependencyError";
  }
}

/**
 * Declare a layer's edges so {@link build} can order it.
 *
 *   const CacheLive = Layer.describe(
 *     { provides: ["Cache"], requires: ["Db"] },
 *     eff(function* () { ... }),
 *   );
 *
 * Names are the same strings passed to `service<T>(name)`.
 */
export function describe<Services extends Record<string, any>, E>(
  deps: { readonly provides: readonly string[]; readonly requires?: readonly string[] },
  layer: Layer<Services, E>,
): Layer<Services, E> {
  const described = layer as any;
  described[LAYER_DEPS] = {
    provides: deps.provides,
    requires: deps.requires ?? [],
  } satisfies LayerDeps;
  return described;
}

function depsOf(layer: unknown): LayerDeps {
  const deps = (layer as any)?.[LAYER_DEPS] as LayerDeps | undefined;
  return deps ?? { provides: [], requires: [] };
}

/**
 * Topologically sort layers by their declared dependencies, then merge them.
 * Each layer's services are visible to every layer built after it, so the
 * order is correct by construction rather than by convention.
 *
 * Throws {@link LayerCycleError} naming the cycle, or
 * {@link LayerMissingDependencyError} naming the unsatisfied service and who
 * wanted it. Both are thrown when `build` is called — a wiring mistake is a
 * programmer error, not a typed failure of the resulting effect.
 */
export function build<L extends readonly Layer<any, any>[]>(
  ...layers: L
): Layer<MergedServices<L>, MergedEffects<L>> {
  const nodes = layers.map((layer, index) => ({ layer, index, deps: depsOf(layer) }));

  // service name -> the node that provides it (last declaration wins, matching
  // merge's "later keys win on collision")
  const providers = new Map<string, number>();
  for (const node of nodes) {
    for (const name of node.deps.provides) providers.set(name, node.index);
  }

  // Every required service must be provided by some layer in this call.
  const missing = new Map<string, string[]>();
  for (const node of nodes) {
    for (const name of node.deps.requires) {
      if (!providers.has(name)) {
        const wanters = missing.get(name) ?? [];
        wanters.push(node.deps.provides.join("+") || `layer #${node.index}`);
        missing.set(name, wanters);
      }
    }
  }
  if (missing.size > 0) {
    const [service, requiredBy] = [...missing.entries()][0]!;
    throw new LayerMissingDependencyError(service, requiredBy);
  }

  // DFS with three-colour marking: white = unvisited, grey = on the current
  // path (a back-edge to grey is a cycle), black = finished.
  const WHITE = 0;
  const GREY = 1;
  const BLACK = 2;
  const colour = new Array<number>(nodes.length).fill(WHITE);
  const order: number[] = [];
  const path: number[] = [];

  const label = (index: number): string =>
    nodes[index]!.deps.provides.join("+") || `layer #${index}`;

  const visit = (index: number): void => {
    if (colour[index] === BLACK) return;
    if (colour[index] === GREY) {
      const start = path.indexOf(index);
      throw new LayerCycleError([...path.slice(start), index].map(label));
    }
    colour[index] = GREY;
    path.push(index);
    for (const name of nodes[index]!.deps.requires) {
      const provider = providers.get(name)!;
      // A layer that requires something it also provides is not a cycle.
      if (provider !== index) visit(provider);
    }
    path.pop();
    colour[index] = BLACK;
    order.push(index);
  };

  for (const node of nodes) visit(node.index);

  // Not `merge`: that is horizontal composition — every layer builds against
  // the caller's context, so a layer could not see one merged before it. Here
  // each layer is built with everything already constructed installed in its
  // context, which is the whole point of sorting them.
  let acc: any = new Suspend(Op.Succeed, {}, null);
  for (const index of order) {
    const prev = acc;
    const layer = nodes[index]!.layer;
    acc = new Suspend(
      Op.FlatMap,
      prev,
      (built: Record<string, unknown>) =>
        new Suspend(
          Op.FlatMap,
          new Suspend(Op.Provide, layer, servicesToContext(built)),
          (services: Record<string, unknown>) =>
            new Suspend(Op.Succeed, { ...built, ...services }, null),
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

    /**
     * Memoize this layer once per active scope.
     */
    memoize<Services extends Record<string, any>, E>(this: Layer<Services, E>): Layer<Services, E>;
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

Suspend.prototype.memoize = function (this: Suspend): any {
  return memoize(this as any);
};

// ── Namespace export ───────────────────────────────────────────────

export const Layer = {
  merge,
  memoize,
  describe,
  build,
} as const;
