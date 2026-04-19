// Layers — clean composition of services. A Layer<S> is an Eff that
// produces a record of services. Build, merge, and apply with .with().
//
// Run: bun packages/core/examples/05-layers.ts

import {
  eff, succeed, sync, acquireRelease, service, run, runSync, Layer, type Eff,
} from "../src";
import { assertEq } from "./_assert";

interface Db { query(sql: string): Eff<string, never> }
interface Cache { get(k: string): string | undefined }
interface Logger { log(msg: string): void }

const Db = service<Db>("Db");
const Cache = service<Cache>("Cache");
const Logger = service<Logger>("Logger");

const logs: string[] = [];

// >>> example: layer-build
// Build layers using existing constructors — succeed, eff, scoped.
const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });

const CacheLive = succeed({
  Cache: { get: () => undefined } as Cache,
});

const LoggerLive = succeed({
  Logger: { log: (m: string) => logs.push(m) } as Logger,
});
// <<< example

// >>> example: layer-apply
// Compose horizontally with Layer.merge, apply with .with()
const AppLive = Layer.merge(DbLive, CacheLive, LoggerLive);

const program = eff(function* () {
  const db = yield* Db.get;
  const log = yield* Logger.get;
  log.log("running");
  return yield* db.query("SELECT 1");
});

assertEq(runSync(program.with(AppLive)), "db:SELECT 1");
// <<< example

// >>> example: layer-chain
// Three equivalent chain styles:
const a = program.with(Layer.merge(DbLive, CacheLive, LoggerLive));
const b = program.with(DbLive.and(CacheLive).and(LoggerLive));
const c = program.with(DbLive).with(CacheLive).with(LoggerLive);
assertEq([runSync(a), runSync(b), runSync(c)], ["db:SELECT 1", "db:SELECT 1", "db:SELECT 1"]);
// <<< example

// >>> example: layer-scoped
// Scoped layer: acquireRelease finalizers fire when the program exits.
const events: string[] = [];
const ScopedLogger = eff(function* () {
  const logger = yield* acquireRelease(
    sync(() => {
      events.push("acquire");
      return { log: (m: string) => events.push(`log:${m}`) } as Logger;
    }),
    () => sync(() => { events.push("release"); }),
  );
  return { Logger: logger };
});

await run(program.with(Layer.merge(DbLive, CacheLive, ScopedLogger)));
assertEq(events, ["acquire", "log:running", "release"]);
// <<< example

// >>> example: layer-test-swap
// Test-time swap is a one-liner — supply a different layer.
const FakeAll = succeed({
  Db: { query: () => succeed("FAKE") } as Db,
  Cache: { get: () => undefined } as Cache,
  Logger: { log: () => {} } as Logger,
});

assertEq(runSync(program.with(FakeAll)), "FAKE");
// <<< example
