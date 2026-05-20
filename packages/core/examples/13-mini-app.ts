// End-to-end: a tiny "user lookup" app pulling everything together.
//
//   - Service interfaces for Db + Cache + Logger
//   - Layer wiring
//   - Generator syntax for the program
//   - Error handling with .catchTag
//   - Resource cleanup with scoped + acquireRelease
//   - Retry on transient errors
//
// Run: bun packages/core/examples/13-mini-app.ts

import {
  eff, succeed, fail, sync, acquireRelease, RetryPolicy,
  service, run, Layer, type Eff, type Throws,
} from "../src";
import { assertEq, assertContains } from "./_assert";

// ── Service contracts ──────────────────────────────────────────────

interface User { id: number; name: string }

type DbErr = { _tag: "NotFound"; id: number } | { _tag: "Transient" };

interface Db {
  findUser(id: number): Eff<User, Throws<DbErr>>;
  close(): Eff<void, never>;
}
interface Cache { get(k: string): User | undefined; set(k: string, v: User): void }
interface Logger { log(msg: string): Eff<void, never> }

const Db = service<Db>("Db");
const Cache = service<Cache>("Cache");
const Logger = service<Logger>("Logger");

// >>> example: app-program
// The application is an effect that uses three services. Note: nothing in
// this code references implementations — it's all interfaces + tags.
function getUser(id: number): Eff<User, Throws<DbErr>> {
  return eff(function* () {
    const cache = yield* Cache.get;
    const log = yield* Logger.get;
    const db = yield* Db.get;

    const cached = cache.get(`user:${id}`);
    if (cached) {
      yield* log.log(`cache hit ${id}`);
      return cached;
    }

    yield* log.log(`cache miss ${id}, querying db`);
    // Retry transient db errors with exponential backoff (fluent .retry)
    const user = yield* db.findUser(id).retry(
      RetryPolicy.exponential({ initial: 5, factor: 2 })
        .withMaxRetries(3)
        .whenError((e: DbErr) => e._tag === "Transient"),
    );
    cache.set(`user:${id}`, user);
    return user;
  }) as Eff<User, Throws<DbErr>>;
}
// <<< example

// ── Live wiring ────────────────────────────────────────────────────

const dbEvents: string[] = [];
let dbCalls = 0;

const DbLive = eff(function* () {
  // Simulate a connection acquired + released via scoped layer
  const conn = yield* acquireRelease(
    sync(() => {
      dbEvents.push("connect");
      return { id: 1 };
    }),
    () => sync(() => { dbEvents.push("disconnect"); }),
  );
  return {
    Db: {
      findUser(id: number): Eff<User, Throws<DbErr>> {
        return eff(function* () {
          dbCalls++;
          if (dbCalls === 1) {
            yield* (fail({ _tag: "Transient" }) as Eff<never, Throws<DbErr>>);
          }
          if (id === 0) {
            yield* (fail({ _tag: "NotFound", id }) as Eff<never, Throws<DbErr>>);
          }
          return { id, name: `user-${id}-from-conn-${conn.id}` };
        }) as Eff<User, Throws<DbErr>>;
      },
      close: () => sync(() => {}),
    } as Db,
  };
});

const cacheStore = new Map<string, User>();
const CacheLive = succeed({
  Cache: {
    get: (k: string) => cacheStore.get(k),
    set: (k: string, v: User) => void cacheStore.set(k, v),
  } as Cache,
});

const logLines: string[] = [];
const LoggerLive = succeed({
  Logger: { log: (m: string) => sync(() => { logLines.push(m); }) } as Logger,
});

const AppLive = Layer.merge(DbLive, CacheLive, LoggerLive);

// ── Run + assert ───────────────────────────────────────────────────

// First call: cache miss + transient error + retry succeeds + caches result
const result = await getUser(7)
  .catchTag("NotFound", (e) => succeed({ id: e.id, name: "(missing)" } as User))
  .with(AppLive).run();
assertEq(result, { id: 7, name: "user-7-from-conn-1" });
assertEq(dbCalls, 2); // 1st call failed transient, 2nd succeeded
assertContains(logLines.join("|"), "cache miss 7");

// Second call: cache hit, no extra db calls
const cached = await getUser(7)
  .catch(() => succeed({ id: -1, name: "" } as User))
  .with(AppLive).run();
assertEq(cached, { id: 7, name: "user-7-from-conn-1" });
assertContains(logLines.join("|"), "cache hit 7");

// Each .with(AppLive) opens + closes its own scope, so we see 2 connect/disconnect cycles.
assertEq(dbEvents, ["connect", "disconnect", "connect", "disconnect"]);
