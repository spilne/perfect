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
} from "../src";

class NotFound {
  readonly _tag = "NotFound" as const;
}
class DbError {
  readonly _tag = "DbError" as const;
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
const ok1 = run(succeed(42));
const ok2 = runSync(succeed("hello"));
const ok3 = run(provide(UserRepo.get, UserRepo, { find: (id) => succeed(`user-${id}`) }));

// ── These should show HELPFUL errors ───────────────────────────────

// Missing service → "Missing services — use provide() to supply": UserRepo
// @ts-expect-error
const err1 = run(UserRepo.get.flatMap((repo) => repo.find("1")));

// Unhandled error → "Unhandled errors — use .catch() or .catchTag()": NotFound
// @ts-expect-error
const err2 = run(fail(new NotFound()));

// Both missing service AND unhandled error
// @ts-expect-error
const err3 = run(UserRepo.get.flatMap((repo) => repo.find("1")));

// Multiple missing services
// @ts-expect-error
const err4 = run(
  UserRepo.get.flatMap((repo) =>
    Logger.get.flatMap((log) => log.info("hello").flatMap(() => repo.find("1"))),
  ),
);
