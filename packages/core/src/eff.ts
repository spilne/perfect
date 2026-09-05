// ── Brand ──────────────────────────────────────────────────────────
const EFF_TAG: unique symbol = Symbol.for("spilne/eff");
type EFF_TAG = typeof EFF_TAG;

// ── Effect tags (phantom) ──────────────────────────────────────────
export interface Throws<E> {
  readonly _throws: E;
}
export interface Needs<D, Name extends string = string> {
  readonly _needs: D;
  readonly _service: Name;
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
  // Continuation frames (only ever appear on the Cont stack, not as Suspend nodes):
  EnsuringFrame = 19,
  ScopeFrame = 20,
}

// ── Continuation cell (singly-linked stack) ────────────────────────
export class Cont {
  constructor(
    public readonly op:
      | Op.FlatMap
      | Op.Catch
      | Op.Provide
      | Op.CatchAll
      | Op.SetInterruptible
      | Op.EnsuringFrame
      | Op.ScopeFrame,
    // unknown, not any: the frame payload is genuinely erased — the
    // interpreter casts at the use site.
    public readonly fn: unknown,
    public next: Cont | null,
  ) {}
}

// ── Suspend node (the runtime representation of an effect) ─────────
export class Suspend {
  readonly [EFF_TAG] = true as const;
  // NOTE: no phantom fields here. Declaring `_A: never` on the class made
  // the Eff intersection compute `never & A = never` for every A — all
  // Eff<A, S> types collapsed to one structural type and any Eff was
  // assignable to any other. The phantoms live only on the Eff alias.

  constructor(
    public readonly op: Op,
    // unknown, not any: operand types are genuinely erased per-op — the
    // interpreter casts at the use site. Keeps `.a`/`.b` from leaking `any`
    // to consumers through the public Eff type.
    public readonly a: unknown,
    public readonly b: unknown,
  ) {}
}

// ── The Eff type ───────────────────────────────────────────────────
// At the type level: Eff<A, S> is a Suspend node plus phantom readonly
// markers, which make Eff properly covariant in A (a produced value can
// widen) and S (a requirement set can grow). Runtime values never carry
// these fields — constructors cast at the boundary.
// At runtime: succeed(v) produces a Suspend(Op.Succeed, v) node too —
// keeping one hidden class. The interpreter special-cases Op.Succeed.
export type Eff<A, S = never> = Suspend & { readonly _A: A; readonly _S: S };

// ── Type-level helpers ─────────────────────────────────────────────
export type InferValue<T> = T extends Eff<infer A, unknown> ? A : never;
export type InferEffects<T> = T extends Eff<unknown, infer S> ? S : never;

// ── Error-channel type helpers ─────────────────────────────────────
// These power the catch-family signatures: infer S WHOLE and compute, never
// split `S | Throws<E>` across two inference variables — TS cannot divide a
// union that way, so E inferred as never and stripping silently failed
// (invisible while Eff assignability was degenerate).

/** Union of typed error payloads in S. */
export type ErrorsOf<S> = S extends Throws<infer E> ? E : never;

/**
 * S with the given error tags removed. Distributes over S so the union
 * shape is preserved; a Throws whose payload empties collapses to never.
 */
export type ExcludeTags<S, Tags extends string> =
  S extends Throws<infer E>
    ? [Exclude<E, { readonly _tag: Tags }>] extends [never]
      ? never
      : Throws<Exclude<E, { readonly _tag: Tags }>>
    : S;

// ── Compile-time effect diagnostics ────────────────────────────────
// Extract specific effect categories from the union
type ExtractErrors<S> = ErrorsOf<S>;
type ExtractServices<S> = S extends Needs<infer D> ? D : never;
type ExtractOther<S> = Exclude<S, Throws<unknown> | Needs<unknown>>;

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
