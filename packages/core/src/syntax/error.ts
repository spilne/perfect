import { type Eff, type Throws, Suspend, Op } from "../eff";
import { Cause } from "../cause";
import { type Exit, Exit as ExitNS } from "../exit";
import { succeed, fail, die } from "../constructors";

type Either<E, A> =
  | { readonly _tag: "Left"; readonly left: E }
  | { readonly _tag: "Right"; readonly right: A };

declare module "../eff" {
  interface Suspend {
    catch<A, S, E, B, S2>(
      this: Eff<A, S | Throws<E>>,
      handler: (error: E) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<E>> | S2>;

    catchTag<A, S, Tag extends string, E extends { readonly _tag: Tag }, B, S2>(
      this: Eff<A, S | Throws<E>>,
      tag: Tag,
      handler: (error: Extract<E, { readonly _tag: Tag }>) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<Extract<E, { readonly _tag: Tag }>>> | S2>;

    catchSome<A, S, E, B, S2>(
      this: Eff<A, S | Throws<E>>,
      handler: (error: E) => Eff<B, S2> | undefined,
    ): Eff<A | B, S | Throws<E> | S2>;

    catchAllCause<A, S, B, S2>(
      this: Eff<A, S>,
      handler: (cause: Cause) => Eff<B, S2>,
    ): Eff<A | B, S2>;

    orElse<A, S1, B, S2>(this: Eff<A, S1>, that: () => Eff<B, S2>): Eff<A | B, S2>;

    orDie<A, S>(this: Eff<A, S>): Eff<A, Exclude<S, Throws<any>>>;

    option<A, S, E>(this: Eff<A, S | Throws<E>>): Eff<A | undefined, Exclude<S, Throws<E>>>;

    either<A, S, E>(this: Eff<A, S | Throws<E>>): Eff<Either<E, A>, Exclude<S, Throws<E>>>;

    mapError<A, S, E, E2>(
      this: Eff<A, S | Throws<E>>,
      f: (e: E) => E2,
    ): Eff<A, Exclude<S, Throws<E>> | Throws<E2>>;

    tapError<A, S, E, S2>(
      this: Eff<A, S | Throws<E>>,
      f: (e: E) => Eff<any, S2>,
    ): Eff<A, S | Throws<E> | S2>;

    tapErrorCause<A, S, S2>(this: Eff<A, S>, f: (cause: Cause) => Eff<any, S2>): Eff<A, S | S2>;

    tapBoth<A, S, E, S2, S3>(
      this: Eff<A, S | Throws<E>>,
      onError: (e: E) => Eff<any, S2>,
      onSuccess: (a: A) => Eff<any, S3>,
    ): Eff<A, S | Throws<E> | S2 | S3>;

    /**
     * Bulk tag handler — `eff.catchTags({ NotFound: e => ..., Forbidden: e => ... })`
     * is equivalent to chaining `.catchTag()` calls. TS narrows each handler's
     * `e` parameter to its tag's payload type.
     */
    catchTags<
      A,
      S,
      E extends { readonly _tag: string },
      Handlers extends {
        readonly [K in E["_tag"]]?: (error: Extract<E, { readonly _tag: K }>) => Eff<any, any>;
      },
    >(
      this: Eff<A, S | Throws<E>>,
      handlers: Handlers,
    ): Eff<
      | A
      | (Handlers[keyof Handlers] extends ((e: any) => Eff<infer B, any>) | undefined ? B : never),
      | Exclude<S, Throws<E>>
      | Throws<
          Exclude<E["_tag"], keyof Handlers> extends infer R extends string
            ? Extract<E, { readonly _tag: R }>
            : never
        >
      | (Handlers[keyof Handlers] extends ((e: any) => Eff<any, infer S2>) | undefined ? S2 : never)
    >;

    /**
     * Switch-style match on a single tag — returns `onMatch(value)` if the
     * tag matches, else returns `onElse(otherError)`.
     */
    matchTag<A, S, E extends { readonly _tag: string }, Tag extends E["_tag"], B, S2, C, S3>(
      this: Eff<A, S | Throws<E>>,
      tag: Tag,
      onMatch: (error: Extract<E, { readonly _tag: Tag }>) => Eff<B, S2>,
      onElse: (error: Exclude<E, { readonly _tag: Tag }>) => Eff<C, S3>,
    ): Eff<A | B | C, Exclude<S, Throws<E>> | S2 | S3>;

    /**
     * Convert `Eff<A, S | Throws<E>>` to `Eff<Exit<E, A>, never>` — never fails
     * for typed/defect/interrupt; the outcome is in the success channel.
     */
    exit<A, S, E>(this: Eff<A, S | Throws<E>>): Eff<Exit<E, A>, Exclude<S, Throws<E>>>;

    /** Observe defects (uncaught throws → Cause.Die) only — re-fails after. */
    tapDefect<A, S, S2>(this: Eff<A, S>, f: (defect: unknown) => Eff<any, S2>): Eff<A, S | S2>;

    /** Transform the entire Cause tree, not just typed leaves. */
    mapErrorCause<A, S>(this: Eff<A, S>, f: (cause: Cause) => Cause): Eff<A, S>;

    // ── Cats-flavored aliases (for users migrating from cats-effect / promin) ──

    /** alias: catch — total handler returning an Eff for any error. */
    handleErrorWith<A, S, E, B, S2>(
      this: Eff<A, S | Throws<E>>,
      handler: (error: E) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<E>> | S2>;

    /** alias: catchSome via predicate + plain-value handler (cats `recover`). */
    recover<A, S, E, B>(
      this: Eff<A, S | Throws<E>>,
      predicate: (error: E) => boolean,
      handler: (error: E) => B,
    ): Eff<A | B, S | Throws<E>>;

    /** alias: catchSome via predicate + Eff-returning handler (cats `recoverWith`). */
    recoverWith<A, S, E, B, S2>(
      this: Eff<A, S | Throws<E>>,
      predicate: (error: E) => boolean,
      handler: (error: E) => Eff<B, S2>,
    ): Eff<A | B, S | Throws<E> | S2>;

    /** cats: match both channels with plain-value functions. */
    redeem<A, S, E, B>(
      this: Eff<A, S | Throws<E>>,
      onError: (error: E) => B,
      onSuccess: (value: A) => B,
    ): Eff<B, Exclude<S, Throws<E>>>;

    /** cats: match both channels with Eff-returning functions. */
    redeemWith<A, S, E, B, S2, S3>(
      this: Eff<A, S | Throws<E>>,
      onError: (error: E) => Eff<B, S2>,
      onSuccess: (value: A) => Eff<B, S3>,
    ): Eff<B, Exclude<S, Throws<E>> | S2 | S3>;
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
