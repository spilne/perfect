import { type Eff, type Throws, type ErrorsOf, type ExcludeTags, Suspend, Op } from "../eff";
import { Cause } from "../cause";
import { type Exit, Exit as ExitNS } from "../exit";
import { succeed, fail, die } from "../constructors";

type Either<E, A> =
  | { readonly _tag: "Left"; readonly left: E }
  | { readonly _tag: "Right"; readonly right: A };

declare module "../eff" {
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

    mapError<A, S, E2>(
      this: Eff<A, S>,
      f: (e: ErrorsOf<S>) => E2,
    ): Eff<A, Exclude<S, Throws<unknown>> | Throws<E2>>;

    tapError<A, S, S2>(
      this: Eff<A, S>,
      f: (e: ErrorsOf<S>) => Eff<unknown, S2>,
    ): Eff<A, S | S2>;

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
     * The `any`s below are inference machinery, not a leak: `(e: any) =>`
     * patterns must stay `any` so conditional types match handlers whose
     * parameter is a narrowed error type (`unknown` would fail contravariance
     * inside `extends`), and each `any` sits inside an `extends`/`infer`
     * clause that never surfaces in the result type.
     */
    catchTags<
      A,
      S,
      Handlers extends {
        readonly [K in Extract<ErrorsOf<S>, { readonly _tag: string }>["_tag"]]?: (
          error: Extract<ErrorsOf<S>, { readonly _tag: K }>,
        ) => Eff<any, any>;
      },
    >(
      this: Eff<A, S>,
      handlers: Handlers,
    ): Eff<
      | A
      | (Handlers[keyof Handlers] extends ((e: any) => Eff<infer B, any>) | undefined ? B : never),
      | ExcludeTags<S, keyof Handlers & string>
      | (Handlers[keyof Handlers] extends ((e: any) => Eff<any, infer S2>) | undefined ? S2 : never)
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

    /** alias: catch — total handler returning an Eff for any error. */
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

Suspend.prototype.catch = function (handler: any) {
  return new Suspend(Op.Catch, this, handler) as any;
};

Suspend.prototype.catchTag = function (tag: any, handler: any) {
  return new Suspend(Op.Catch, this, (error: any) => {
    if (error !== null && typeof error === "object" && error._tag === tag) {
      return handler(error);
    }
    return fail(error);
  }) as any;
};

Suspend.prototype.catchSome = function (handler: any) {
  return new Suspend(Op.Catch, this, (error: any) => {
    const alt = handler(error);
    return alt ?? fail(error);
  }) as any;
};

Suspend.prototype.catchAllCause = function (handler: any) {
  return new Suspend(Op.CatchAll, this, handler) as any;
};

Suspend.prototype.orElse = function (that: any) {
  return new Suspend(Op.Catch, this, () => that()) as any;
};

Suspend.prototype.orDie = function () {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const f = Cause.firstFail(cause);
    if (f) return die(f.value);
    return new Suspend(Op.Fail, cause, null);
  }) as any;
};

Suspend.prototype.option = function () {
  return new Suspend(Op.Catch, this, () => succeed(undefined)) as any;
};

Suspend.prototype.either = function () {
  return (this as any)
    .map((a: any) => ({ _tag: "Right" as const, right: a }))
    .catch((e: any) => succeed({ _tag: "Left" as const, left: e }));
};

Suspend.prototype.mapError = function (f: any) {
  return new Suspend(Op.Catch, this, (e: any) => fail(f(e))) as any;
};

Suspend.prototype.tapError = function (f: any) {
  return new Suspend(
    Op.Catch,
    this,
    (e: any) => new Suspend(Op.FlatMap, f(e), () => fail(e)),
  ) as any;
};

Suspend.prototype.tapErrorCause = function (f: any) {
  return new Suspend(
    Op.CatchAll,
    this,
    (cause: Cause) => new Suspend(Op.FlatMap, f(cause), () => new Suspend(Op.Fail, cause, null)),
  ) as any;
};

Suspend.prototype.tapBoth = function (onError: any, onSuccess: any) {
  return (this as any).tapError(onError).tap(onSuccess);
};

Suspend.prototype.catchTags = function (handlers: any) {
  return new Suspend(Op.Catch, this, (error: any) => {
    if (error !== null && typeof error === "object" && error._tag in handlers) {
      const handler = handlers[error._tag];
      if (typeof handler === "function") return handler(error);
    }
    return fail(error);
  }) as any;
};

Suspend.prototype.matchTag = function (tag: any, onMatch: any, onElse: any) {
  return new Suspend(Op.Catch, this, (error: any) => {
    if (error !== null && typeof error === "object" && error._tag === tag) {
      return onMatch(error);
    }
    return onElse(error);
  }) as any;
};

Suspend.prototype.exit = function () {
  return new Suspend(
    Op.CatchAll,
    new Suspend(Op.FlatMap, this, (a: any) => succeed(ExitNS.succeed(a))),
    (cause: Cause) => succeed(ExitNS.failure(cause)),
  ) as any;
};

Suspend.prototype.tapDefect = function (f: any) {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const d = Cause.firstDie(cause);
    if (d === null) return new Suspend(Op.Fail, cause, null);
    return new Suspend(Op.FlatMap, f(d.value), () => new Suspend(Op.Fail, cause, null));
  }) as any;
};

Suspend.prototype.mapErrorCause = function (f: any) {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const next = f(cause);
    return new Suspend(Op.Fail, next, null);
  }) as any;
};

// ── Cats-flavored aliases ────────────────────────────────────────

Suspend.prototype.handleErrorWith = Suspend.prototype.catch;

Suspend.prototype.recover = function (predicate: any, handler: any) {
  return (this as any).catchSome((e: any) => (predicate(e) ? succeed(handler(e)) : undefined));
};

Suspend.prototype.recoverWith = function (predicate: any, handler: any) {
  return (this as any).catchSome((e: any) => (predicate(e) ? handler(e) : undefined));
};

Suspend.prototype.redeem = function (onError: any, onSuccess: any) {
  return (this as any).map((a: any) => onSuccess(a)).catch((e: any) => succeed(onError(e)));
};

Suspend.prototype.redeemWith = function (onError: any, onSuccess: any) {
  return (this as any).flatMap((a: any) => onSuccess(a)).catch((e: any) => onError(e));
};
