import { type Eff, type Throws, type ErrorsOf, type ExcludeTags, Suspend, Op } from "../eff.js";
import { Cause } from "../cause.js";
import { type Exit, Exit as ExitNS } from "../exit.js";
import { succeed, fail, failCause, die } from "../constructors.js";
import type { Either, WithError } from "../either.js";

declare module "../eff.js" {
  interface Suspend {
    catch<A, S, B, S2>(
      this: Eff<A, S>,
      handler: (error: ErrorsOf<S>) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<unknown>> | S2>;

    catchTag<A, S, Tag extends string, B, S2>(
      this: Eff<A, S>,
      tag: Tag,
      handler: (error: Extract<ErrorsOf<S>, { readonly _tag: Tag }>) => Eff<B, S2>,
    ): Eff<A | B, ExcludeTags<S, Tag> | S2>;

    catchSome<A, S, B, S2>(
      this: Eff<A, S>,
      handler: (error: ErrorsOf<S>) => Eff<B, S2> | undefined,
    ): Eff<A | B, S | S2>;

    catchAllCause<A, S, B, S2>(
      this: Eff<A, S>,
      handler: (cause: Cause) => Eff<B, S2>,
    ): Eff<A | B, S2>;

    orElse<A, S1, B, S2>(this: Eff<A, S1>, that: () => Eff<B, S2>): Eff<A | B, S2>;

    orDie<A, S>(this: Eff<A, S>): Eff<A, Exclude<S, Throws<unknown>>>;

    option<A, S>(this: Eff<A, S>): Eff<A | undefined, Exclude<S, Throws<unknown>>>;

    either<A, S>(this: Eff<A, S>): Eff<Either<ErrorsOf<S>, A>, Exclude<S, Throws<unknown>>>;

    /**
     * Inverse of `either()` / `exit()`: a Left fails with its
     * value, a Failure with its whole cause, keeping defects and interrupts.
     */
    rethrow<E, A, S>(this: Eff<Either<E, A>, S>): Eff<A, WithError<S, E>>;
    rethrow<E, A, S>(this: Eff<Exit<E, A>, S>): Eff<A, WithError<S, E>>;

    mapError<A, S, E2>(
      this: Eff<A, S>,
      f: (e: ErrorsOf<S>) => E2,
    ): Eff<A, Exclude<S, Throws<unknown>> | Throws<E2>>;

    tapError<A, S, S2>(this: Eff<A, S>, f: (e: ErrorsOf<S>) => Eff<unknown, S2>): Eff<A, S | S2>;

    tapErrorCause<A, S, S2>(this: Eff<A, S>, f: (cause: Cause) => Eff<unknown, S2>): Eff<A, S | S2>;

    tapBoth<A, S, S2, S3>(
      this: Eff<A, S>,
      onError: (e: ErrorsOf<S>) => Eff<unknown, S2>,
      onSuccess: (a: A) => Eff<unknown, S3>,
    ): Eff<A, S | S2 | S3>;

    /**
     * Bulk tag handler — `eff.catchTags({ NotFound: e => ..., Forbidden: e => ... })`
     * is equivalent to chaining `.catchTag()` calls. TS narrows each handler's
     * `e` parameter to its tag's payload type.
     *
     * The `(e: never) =>` patterns below match handlers whose parameter is a
     * narrowed error type: `unknown` would fail contravariance inside
     * `extends`, while every parameter type accepts `never`.
     */
    catchTags<
      A,
      S,
      Handlers extends {
        readonly [K in Extract<ErrorsOf<S>, { readonly _tag: string }>["_tag"]]?: (
          error: Extract<ErrorsOf<S>, { readonly _tag: K }>,
        ) => Eff<unknown, unknown>;
      },
    >(
      this: Eff<A, S>,
      handlers: Handlers,
    ): Eff<
      | A
      | (Handlers[keyof Handlers] extends ((e: never) => Eff<infer B, unknown>) | undefined
          ? B
          : never),
      | ExcludeTags<S, keyof Handlers & string>
      | (Handlers[keyof Handlers] extends ((e: never) => Eff<unknown, infer S2>) | undefined
          ? S2
          : never)
    >;

    /**
     * Switch-style match on a single tag — returns `onMatch(value)` if the
     * tag matches, else returns `onElse(otherError)`.
     */
    matchTag<A, S, Tag extends string, B, S2, C, S3>(
      this: Eff<A, S>,
      tag: Tag,
      onMatch: (error: Extract<ErrorsOf<S>, { readonly _tag: Tag }>) => Eff<B, S2>,
      onElse: (error: Exclude<ErrorsOf<S>, { readonly _tag: Tag }>) => Eff<C, S3>,
    ): Eff<A | B | C, Exclude<S, Throws<unknown>> | S2 | S3>;

    /**
     * Convert `Eff<A, S | Throws<E>>` to `Eff<Exit<E, A>, never>` — never fails
     * for typed/defect/interrupt; the outcome is in the success channel.
     */
    exit<A, S>(this: Eff<A, S>): Eff<Exit<ErrorsOf<S>, A>, Exclude<S, Throws<unknown>>>;

    /** Observe defects (uncaught throws → Cause.Die) only — re-fails after. */
    tapDefect<A, S, S2>(this: Eff<A, S>, f: (defect: unknown) => Eff<unknown, S2>): Eff<A, S | S2>;

    /** Transform the entire Cause tree, not just typed leaves. */
    mapErrorCause<A, S>(this: Eff<A, S>, f: (cause: Cause) => Cause): Eff<A, S>;

    // ── Cats-flavored aliases (for users migrating from cats-effect / promin) ──

    /** @deprecated Same as `.catch` (the cats-effect name). Use `.catch`. */
    handleErrorWith<A, S, B, S2>(
      this: Eff<A, S>,
      handler: (error: ErrorsOf<S>) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<unknown>> | S2>;

    /** alias: catchSome via predicate + plain-value handler (cats `recover`). */
    recover<A, S, B>(
      this: Eff<A, S>,
      predicate: (error: ErrorsOf<S>) => boolean,
      handler: (error: ErrorsOf<S>) => B,
    ): Eff<A | B, S>;

    /** alias: catchSome via predicate + Eff-returning handler (cats `recoverWith`). */
    recoverWith<A, S, B, S2>(
      this: Eff<A, S>,
      predicate: (error: ErrorsOf<S>) => boolean,
      handler: (error: ErrorsOf<S>) => Eff<B, S2>,
    ): Eff<A | B, S | S2>;

    /** cats: match both channels with plain-value functions. */
    redeem<A, S, B>(
      this: Eff<A, S>,
      onError: (error: ErrorsOf<S>) => B,
      onSuccess: (value: A) => B,
    ): Eff<B, Exclude<S, Throws<unknown>>>;

    /** cats: match both channels with Eff-returning functions. */
    redeemWith<A, S, B, S2, S3>(
      this: Eff<A, S>,
      onError: (error: ErrorsOf<S>) => Eff<B, S2>,
      onSuccess: (value: A) => Eff<B, S3>,
    ): Eff<B, Exclude<S, Throws<unknown>> | S2 | S3>;
  }
}

// Implementations work on erased types; the public signatures above are
// the contract, so each one is cast once to its declared method type.
type AnyEff = Eff<unknown, unknown>;
type Tagged = { readonly _tag: string };

Suspend.prototype.catch = function (this: Suspend, handler: (error: unknown) => Suspend) {
  return new Suspend(Op.Catch, this, handler) as AnyEff;
} as Suspend["catch"];

Suspend.prototype.catchTag = function (
  this: Suspend,
  tag: string,
  handler: (error: never) => Suspend,
) {
  return new Suspend(Op.Catch, this, (error: unknown) => {
    if (error !== null && typeof error === "object" && (error as Tagged)._tag === tag) {
      return handler(error as never);
    }
    return fail(error);
  }) as AnyEff;
} as Suspend["catchTag"];

Suspend.prototype.catchSome = function (
  this: Suspend,
  handler: (error: unknown) => Suspend | undefined,
) {
  return new Suspend(Op.Catch, this, (error: unknown) => {
    const alt = handler(error);
    return alt ?? fail(error);
  }) as AnyEff;
} as Suspend["catchSome"];

Suspend.prototype.catchAllCause = function (this: Suspend, handler: (cause: Cause) => Suspend) {
  return new Suspend(Op.CatchAll, this, handler) as AnyEff;
} as Suspend["catchAllCause"];

Suspend.prototype.orElse = function (this: Suspend, that: () => Suspend) {
  return new Suspend(Op.Catch, this, () => that()) as AnyEff;
} as Suspend["orElse"];

Suspend.prototype.orDie = function (this: Suspend) {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const f = Cause.firstFail(cause);
    if (f) return die(f.value);
    return new Suspend(Op.Fail, cause, null);
  }) as AnyEff;
} as Suspend["orDie"];

Suspend.prototype.option = function (this: Suspend) {
  return new Suspend(Op.Catch, this, () => succeed(undefined)) as AnyEff;
} as Suspend["option"];

Suspend.prototype.either = function (this: AnyEff) {
  return this.map((a) => ({ _tag: "Right" as const, right: a })).catch((e) =>
    succeed({ _tag: "Left" as const, left: e }),
  );
} as Suspend["either"];

Suspend.prototype.rethrow = function (this: Suspend) {
  return new Suspend(Op.FlatMap, this, rethrowOutcome) as AnyEff;
} as Suspend["rethrow"];

function rethrowOutcome(
  outcome: Either<unknown, unknown> | Exit<unknown, unknown>,
): Eff<unknown, unknown> {
  switch (outcome._tag) {
    case "Right":
      return succeed(outcome.right);
    case "Success":
      return succeed(outcome.value);
    case "Left":
      return fail(outcome.left);
    case "Failure":
      return failCause(outcome.cause);
  }
}

Suspend.prototype.mapError = function (this: Suspend, f: (e: unknown) => unknown) {
  return new Suspend(Op.Catch, this, (e: unknown) => fail(f(e))) as AnyEff;
} as Suspend["mapError"];

Suspend.prototype.tapError = function (this: Suspend, f: (e: unknown) => Suspend) {
  return new Suspend(
    Op.Catch,
    this,
    (e: unknown) => new Suspend(Op.FlatMap, f(e), () => fail(e)),
  ) as AnyEff;
} as Suspend["tapError"];

Suspend.prototype.tapErrorCause = function (this: Suspend, f: (cause: Cause) => Suspend) {
  return new Suspend(
    Op.CatchAll,
    this,
    (cause: Cause) => new Suspend(Op.FlatMap, f(cause), () => new Suspend(Op.Fail, cause, null)),
  ) as AnyEff;
} as Suspend["tapErrorCause"];

Suspend.prototype.tapBoth = function (
  this: AnyEff,
  onError: (e: unknown) => AnyEff,
  onSuccess: (a: unknown) => AnyEff,
) {
  return this.tapError(onError).tap(onSuccess);
} as Suspend["tapBoth"];

Suspend.prototype.catchTags = function (
  this: Suspend,
  handlers: Record<string, ((error: unknown) => Suspend) | undefined>,
) {
  return new Suspend(Op.Catch, this, (error: unknown) => {
    if (error !== null && typeof error === "object" && (error as Tagged)._tag in handlers) {
      const handler = handlers[(error as Tagged)._tag];
      if (typeof handler === "function") return handler(error);
    }
    return fail(error);
  }) as AnyEff;
} as Suspend["catchTags"];

Suspend.prototype.matchTag = function (
  this: Suspend,
  tag: string,
  onMatch: (error: never) => Suspend,
  onElse: (error: never) => Suspend,
) {
  return new Suspend(Op.Catch, this, (error: unknown) => {
    if (error !== null && typeof error === "object" && (error as Tagged)._tag === tag) {
      return onMatch(error as never);
    }
    return onElse(error as never);
  }) as AnyEff;
} as Suspend["matchTag"];

Suspend.prototype.exit = function (this: Suspend) {
  return new Suspend(
    Op.CatchAll,
    new Suspend(Op.FlatMap, this, (a: unknown) => succeed(ExitNS.succeed(a))),
    (cause: Cause) => succeed(ExitNS.failure(cause)),
  ) as AnyEff;
} as Suspend["exit"];

Suspend.prototype.tapDefect = function (this: Suspend, f: (defect: unknown) => Suspend) {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const d = Cause.firstDie(cause);
    if (d === null) return new Suspend(Op.Fail, cause, null);
    return new Suspend(Op.FlatMap, f(d.value), () => new Suspend(Op.Fail, cause, null));
  }) as AnyEff;
} as Suspend["tapDefect"];

Suspend.prototype.mapErrorCause = function (this: Suspend, f: (cause: Cause) => Cause) {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const next = f(cause);
    return new Suspend(Op.Fail, next, null);
  }) as AnyEff;
} as Suspend["mapErrorCause"];

// ── Cats-flavored aliases ────────────────────────────────────────

Suspend.prototype.handleErrorWith = Suspend.prototype.catch;

Suspend.prototype.recover = function (
  this: AnyEff,
  predicate: (e: unknown) => boolean,
  handler: (e: unknown) => unknown,
) {
  return this.catchSome((e) => (predicate(e) ? succeed(handler(e)) : undefined));
} as Suspend["recover"];

Suspend.prototype.recoverWith = function (
  this: AnyEff,
  predicate: (e: unknown) => boolean,
  handler: (e: unknown) => AnyEff,
) {
  return this.catchSome((e) => (predicate(e) ? handler(e) : undefined));
} as Suspend["recoverWith"];

Suspend.prototype.redeem = function (
  this: AnyEff,
  onError: (e: unknown) => unknown,
  onSuccess: (a: unknown) => unknown,
) {
  return this.map((a) => onSuccess(a)).catch((e) => succeed(onError(e)));
} as Suspend["redeem"];

Suspend.prototype.redeemWith = function (
  this: AnyEff,
  onError: (e: unknown) => AnyEff,
  onSuccess: (a: unknown) => AnyEff,
) {
  return this.flatMap((a) => onSuccess(a)).catch((e) => onError(e));
} as Suspend["redeemWith"];
