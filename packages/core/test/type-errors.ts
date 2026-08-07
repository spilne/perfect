// This file is NOT a test that runs — it's a type-level test.
// Each line marked @ts-expect-error should produce a helpful error message.
// Run: bunx tsc --noEmit test/type-errors.ts

import {
  type Eff,
  type Throws,
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
const _ok5: Eff<[number, string], never> = all([succeed(1), succeed("two")] as const);
const _ok6: Eff<{ a: number; b: string }, never> = all({ a: succeed(1), b: succeed("two") });
const _ok7: Eff<number[], never> = Stream.of(1, 2, 3).runSink(Sinks.collectAll());
const _ok8: Eff<number | undefined, never> = Stream.of(1, 2, 3).runSink(Sinks.head());

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
