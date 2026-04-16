import { type Eff, type Throws, Suspend, Op } from "../eff"
import { Cause } from "../cause"
import { succeed, fail, die } from "../constructors"

type Either<E, A> = { readonly _tag: "Left"; readonly left: E } | { readonly _tag: "Right"; readonly right: A }

declare module "../eff" {
  interface Suspend {
    catch<A, S, E, B, S2>(
      this: Eff<A, S | Throws<E>>,
      handler: (error: E) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<E>> | S2>

    catchTag<A, S, Tag extends string, E extends { readonly _tag: Tag }, B, S2>(
      this: Eff<A, S | Throws<E>>,
      tag: Tag,
      handler: (error: Extract<E, { readonly _tag: Tag }>) => Eff<B, S2>,
    ): Eff<A | B, Exclude<S, Throws<Extract<E, { readonly _tag: Tag }>>> | S2>

    catchSome<A, S, E, B, S2>(
      this: Eff<A, S | Throws<E>>,
      handler: (error: E) => Eff<B, S2> | undefined,
    ): Eff<A | B, S | Throws<E> | S2>

    catchAllCause<A, S, B, S2>(
      this: Eff<A, S>,
      handler: (cause: Cause) => Eff<B, S2>,
    ): Eff<A | B, S2>

    orElse<A, S1, B, S2>(
      this: Eff<A, S1>,
      that: () => Eff<B, S2>,
    ): Eff<A | B, S2>

    orDie<A, S>(this: Eff<A, S>): Eff<A, Exclude<S, Throws<any>>>

    option<A, S, E>(
      this: Eff<A, S | Throws<E>>,
    ): Eff<A | undefined, Exclude<S, Throws<E>>>

    either<A, S, E>(
      this: Eff<A, S | Throws<E>>,
    ): Eff<Either<E, A>, Exclude<S, Throws<E>>>

    mapError<A, S, E, E2>(
      this: Eff<A, S | Throws<E>>,
      f: (e: E) => E2,
    ): Eff<A, Exclude<S, Throws<E>> | Throws<E2>>

    tapError<A, S, E, S2>(
      this: Eff<A, S | Throws<E>>,
      f: (e: E) => Eff<any, S2>,
    ): Eff<A, S | Throws<E> | S2>

    tapErrorCause<A, S, S2>(
      this: Eff<A, S>,
      f: (cause: Cause) => Eff<any, S2>,
    ): Eff<A, S | S2>

    tapBoth<A, S, E, S2, S3>(
      this: Eff<A, S | Throws<E>>,
      onError: (e: E) => Eff<any, S2>,
      onSuccess: (a: A) => Eff<any, S3>,
    ): Eff<A, S | Throws<E> | S2 | S3>
  }
}

Suspend.prototype.catch = function (handler: any) {
  return new Suspend(Op.Catch, this, handler) as any
}

Suspend.prototype.catchTag = function (tag: any, handler: any) {
  return new Suspend(Op.Catch, this, (error: any) => {
    if (error !== null && typeof error === "object" && error._tag === tag) {
      return handler(error)
    }
    return fail(error)
  }) as any
}

Suspend.prototype.catchSome = function (handler: any) {
  return new Suspend(Op.Catch, this, (error: any) => {
    const alt = handler(error)
    return alt ?? fail(error)
  }) as any
}

Suspend.prototype.catchAllCause = function (handler: any) {
  return new Suspend(Op.CatchAll, this, handler) as any
}

Suspend.prototype.orElse = function (that: any) {
  return new Suspend(Op.Catch, this, () => that()) as any
}

Suspend.prototype.orDie = function () {
  return new Suspend(Op.CatchAll, this, (cause: Cause) => {
    const f = Cause.firstFail(cause)
    if (f) return die(f.value)
    return new Suspend(Op.Fail, cause, null)
  }) as any
}

Suspend.prototype.option = function () {
  return new Suspend(Op.Catch, this, () => succeed(undefined)) as any
}

Suspend.prototype.either = function () {
  return (this as any)
    .map((a: any) => ({ _tag: "Right" as const, right: a }))
    .catch((e: any) => succeed({ _tag: "Left" as const, left: e }))
}

Suspend.prototype.mapError = function (f: any) {
  return new Suspend(Op.Catch, this, (e: any) => fail(f(e))) as any
}

Suspend.prototype.tapError = function (f: any) {
  return new Suspend(Op.Catch, this, (e: any) =>
    new Suspend(Op.FlatMap, f(e), () => fail(e)),
  ) as any
}

Suspend.prototype.tapErrorCause = function (f: any) {
  return new Suspend(Op.CatchAll, this, (cause: Cause) =>
    new Suspend(Op.FlatMap, f(cause), () => new Suspend(Op.Fail, cause, null)),
  ) as any
}

Suspend.prototype.tapBoth = function (onError: any, onSuccess: any) {
  return (this as any).tapError(onError).tap(onSuccess)
}
