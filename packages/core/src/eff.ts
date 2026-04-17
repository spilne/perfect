import type { Cause } from "./cause";

// ── Brand ──────────────────────────────────────────────────────────
const EFF_TAG: unique symbol = Symbol.for("spilne/eff");
type EFF_TAG = typeof EFF_TAG;

// ── Effect tags (phantom) ──────────────────────────────────────────
export interface Throws<E> {
  readonly _throws: E;
}
export interface Needs<D> {
  readonly _needs: D;
}

// ── Op codes ───────────────────────────────────────────────────────
export const enum Op {
  Succeed = 0,
  Sync = 1,
  Fail = 2,
  FlatMap = 3,
  Catch = 4,
  Async = 5,
  Provide = 6,
  GetCtx = 7,
  All = 8,
  Fork = 9,
  Race = 10,
  Ensuring = 11,
  CatchAll = 12,
  AcqRel = 13,
  GetScope = 14,
  Scoped = 15,
  SetInterruptible = 16,
  YieldNow = 17,
  ForkDaemon = 18,
}

// ── Continuation cell (singly-linked stack) ────────────────────────
export class Cont {
  constructor(
    public readonly op: Op.FlatMap | Op.Catch | Op.Provide | Op.CatchAll | Op.SetInterruptible,
    public readonly fn: any,
    public next: Cont | null,
  ) {}
}

// ── Suspend node (the runtime representation of an effect) ─────────
export class Suspend {
  readonly [EFF_TAG] = true as const;
  // phantom variance markers — never read at runtime
  declare readonly _A: never;
  declare readonly _S: never;

  constructor(
    public readonly op: Op,
    public readonly a: any,
    public readonly b: any,
  ) {}
}

// ── The Eff type ───────────────────────────────────────────────────
// At the type level: Eff<A, S> is a Suspend node.
// At runtime: succeed(v) produces a Suspend(Op.Succeed, v) node too —
// keeping one hidden class. The interpreter special-cases Op.Succeed.
export type Eff<out A, out S = never> = Suspend & { readonly _A: A; readonly _S: S };

// ── Type-level helpers ─────────────────────────────────────────────
export type InferValue<T> = T extends Eff<infer A, any> ? A : never;
export type InferEffects<T> = T extends Eff<any, infer S> ? S : never;

// ── Compile-time effect diagnostics ────────────────────────────────
// Extract specific effect categories from the union
type ExtractErrors<S> = S extends Throws<infer E> ? E : never;
type ExtractServices<S> = S extends Needs<infer D> ? D : never;
type ExtractOther<S> = Exclude<S, Throws<any> | Needs<any>>;

// Produce a readable error object when effects aren't fully handled
export type EffectCheck<S> = [S] extends [never]
  ? unknown // all handled — no constraint
  : [ExtractErrors<S>] extends [never]
    ? [ExtractServices<S>] extends [never]
      ? { "Unhandled effects — resolve before calling run()": ExtractOther<S> }
      : { "Missing services — use provide() to supply": ExtractServices<S> }
    : [ExtractServices<S>] extends [never]
      ? { "Unhandled errors — use .catch() or .catchTag()": ExtractErrors<S> }
      : {
          "Unhandled errors — use .catch() or .catchTag()": ExtractErrors<S>;
          "Missing services — use provide() to supply": ExtractServices<S>;
        };

// ── Runtime check ──────────────────────────────────────────────────
export function isSuspend(v: unknown): v is Suspend {
  return v !== null && typeof v === "object" && (v as any)[EFF_TAG] === true;
}
