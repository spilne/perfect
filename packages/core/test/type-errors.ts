// This file is NOT a test that runs — it's a type-level test.
// Each line marked @ts-expect-error should produce a helpful error message.
// Run: bunx tsc --noEmit test/type-errors.ts

import {
  type Eff,
  type Throws,
  type Needs,
  succeed,
  fail,
  service,
  provide,
  run,
  runSync,
  all,
  race,
  Stream,
  Sinks,
} from "../src";

class NotFound {
  readonly _tag = "NotFound" as const;
}
class Forbidden {
  readonly _tag = "Forbidden" as const;
}
class Unauthorized {
  readonly _tag = "Unauthorized" as const;
}
interface UserRepo {
  find(id: string): Eff<string, Throws<NotFound>>;
}
interface Logger {
  info(msg: string): Eff<void, never>;
}
const UserRepo = service<UserRepo>("UserRepo");
const Logger = service<Logger>("Logger");

// ── These should compile fine ──────────────────────────────────────

// Fully handled: no effects remaining
const _ok1 = run(succeed(42));
const _ok2 = runSync(succeed("hello"));
const _ok3 = run(provide(UserRepo.get, UserRepo, { find: (id) => succeed(`user-${id}`) }));
const _ok4: Eff<string | number, never> = race([
  succeed("ok"),
  fail(new NotFound()).catchTag("NotFound", () => succeed(1)),
]);
const _ok5: Eff<readonly [number, string], never> = all([succeed(1), succeed("two")] as const);
const _ok6: Eff<{ a: number; b: string }, never> = all({ a: succeed(1), b: succeed("two") });
const _ok7: Eff<number[], never> = Stream.of(1, 2, 3).runSink(Sinks.collectAll());
const _ok8: Eff<number | undefined, never> = Stream.of(1, 2, 3).runSink(Sinks.head());
const _okSink1: Eff<number[], never> = Stream.of(1, 2, 3).runSink(Sinks.collectN(2));
const _okSink2: Eff<number, never> = Stream.of(1, 2, 3).runSink(
  Sinks.foldEffect(0, (acc, n) => succeed(acc + n)),
);
const _okSink3: Eff<void, never> = Stream.of(1, 2, 3).runSink(
  Sinks.forEachWhile((n) => succeed(n < 2)),
);
const _okSink4: Eff<string, never> = Stream.of(1, 2, 3).runSink(Sinks.drainWith(succeed("done")));
const _okSink5: Eff<number, never> = Stream.of(1, 2, 3).runSink(Sinks.fromEffect(succeed(42)));
const _okSink6: Eff<string, never> = Stream.of("a", "bb").runSink(
  Sinks.fold(0, (acc: number, n: number) => acc + n)
    .contramap((s: string) => s.length)
    .map((n) => `total:${n}`),
);
const _okLayerMemo: Eff<{ UserRepo: UserRepo }, never> = succeed({
  UserRepo: { find: (id: string) => succeed(id) } as UserRepo,
}).memoize();

type Many =
  | Throws<NotFound>
  | Throws<Forbidden>
  | Throws<Unauthorized>
  | Throws<{ readonly _tag: "E4" }>
  | Throws<{ readonly _tag: "E5" }>
  | Throws<{ readonly _tag: "E6" }>
  | Throws<{ readonly _tag: "E7" }>
  | Throws<{ readonly _tag: "E8" }>
  | Throws<{ readonly _tag: "E9" }>
  | Throws<{ readonly _tag: "E10" }>;
const _largeUnion: Eff<number, Many> = fail(new NotFound()) as any;
const _largeUnionHandled = _largeUnion
  .catchTag("NotFound", () => succeed(1))
  .catchTag("Forbidden", () => succeed(2))
  .catchTag("Unauthorized", () => succeed(3))
  .catch(() => succeed(4));
const _ok9: Eff<number, never> = _largeUnionHandled;

// ── These should show HELPFUL errors ───────────────────────────────

// Missing service → "Missing services — use provide() to supply": UserRepo
// @ts-expect-error
const _err1 = run(UserRepo.get.flatMap((repo) => repo.find("1")));

// Unhandled error → "Unhandled errors — use .catch() or .catchTag()": NotFound
// @ts-expect-error
const _err2 = run(fail(new NotFound()));

// Both missing service AND unhandled error
// @ts-expect-error
const _err3 = run(UserRepo.get.flatMap((repo) => repo.find("1")));

// Multiple missing services
const _err4 = run(
  // @ts-expect-error
  UserRepo.get.flatMap((repo) =>
    Logger.get.flatMap((log) => log.info("hello").flatMap(() => repo.find("1"))),
  ),
);

// all() preserves unhandled failures
// @ts-expect-error
const _err5 = run(all([succeed(1), fail(new Forbidden())] as const));

// race() preserves unhandled failures from either branch
// @ts-expect-error
const _err6 = run(race([succeed(1) as Eff<number, Throws<Forbidden>>, fail(new Forbidden())]));

// Stream terminal effects preserve unhandled failures
// @ts-expect-error
const _err7 = run(Stream.fail(new Forbidden()).runSink(Sinks.collectAll()));

// Sinks.forEach contributes its own effect requirements
const _err8 = run(
  // @ts-expect-error
  Stream.of("1").runSink(
    Sinks.forEach((id) => UserRepo.get.flatMap((repo) => repo.find(id)).as(undefined)),
  ),
);

// ── Eff variance (regression: phantom collapse) ────────────────────
// Suspend used to declare `_A: never`, collapsing every Eff<A, S> into one
// structural type — any Eff was assignable to any other. These assertions
// pin the fixed behavior.
{
  const effNum: Eff<number, never> = succeed(1);

  // @ts-expect-error a produced number is not a string
  const _wrongA: Eff<string, never> = effNum;

  const effFails: Eff<number, Throws<"boom">> = effNum;
  // @ts-expect-error requirements cannot be dropped — Throws<"boom"> ⊄ never
  const _dropReq: Eff<number, never> = effFails;

  // covariance positives — these must stay legal:
  const _widenS: Eff<number, Throws<"boom"> | Needs<{ db: true }>> = effFails;
  const _widenA: Eff<number | string, never> = effNum;
  const _bottom: Eff<never, never> = null as unknown as Eff<never, never>;
  const _bottomAnywhere: Eff<number, Throws<"x">> = _bottom;
  void [_widenS, _widenA, _bottomAnywhere];
}

// ── catch-family stripping (regression: union-split inference) ─────
// The old `this: Eff<A, S | Throws<E>>` signatures could not actually
// split a union across two inference variables, so stripping silently
// failed. These pin the whole-S formulations.
{
  const many: Eff<number, Many> = null as unknown as Eff<number, Many>;

  const _optStrips: Eff<number | undefined, never> = many.option();
  const _eitherStrips: Eff<unknown, never> = many.either();
  const _redeemStrips: Eff<string, never> = many.redeem(
    () => "e",
    (n) => String(n),
  );
  const _mapErrorReplaces: Eff<number, Throws<"wrapped">> = many.mapError(() => "wrapped" as const);
  void [_optStrips, _eitherStrips, _redeemStrips, _mapErrorReplaces];

  // catchTag removes exactly one tag, keeps the rest
  const two: Eff<number, Throws<{ readonly _tag: "A" }> | Throws<{ readonly _tag: "B" }>> =
    null as unknown as never;
  const afterTag = two.catchTag("A", () => succeed(0));
  const _keepsB: Eff<number, Throws<{ readonly _tag: "B" }>> = afterTag;
  // @ts-expect-error B is still unhandled
  const _notNever: Eff<number, never> = afterTag;
  void [_keepsB];
}
