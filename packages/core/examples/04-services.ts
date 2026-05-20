// Services — effect-tracked dependency injection.
//
// service<T>(name) creates a typed handle. .get is an effect that retrieves
// the implementation from context. provide(eff, tag, impl) installs one.
//
// Run: bun packages/core/examples/04-services.ts

import { eff, succeed, service, provide, runSync, type Eff } from "../src";
import { assertEq } from "./_assert";

// >>> example: service-define
// Define a service interface and a tag.
interface Greeter {
  greet(name: string): Eff<string, never>;
}
const Greeter = service<Greeter>("Greeter");

// Use the service inside a program.
const program = eff(function* () {
  const greeter = yield* Greeter.get;
  return yield* greeter.greet("world");
});

// Provide an implementation when running.
const wired = provide(
  program,
  Greeter,
  { greet: (name) => succeed(`hello, ${name}`) },
);

assertEq(wired.runSync(), "hello, world");
// <<< example

// >>> example: service-define-flat
// Same program, chainable form: Greeter.get is an effect — flatMap into it.
const programFlat = Greeter.get.flatMap((g) => g.greet("world"));

const wiredFlat = provide(
  programFlat,
  Greeter,
  { greet: (name) => succeed(`hello, ${name}`) },
);

assertEq(wiredFlat.runSync(), "hello, world");
// <<< example

// >>> example: service-multiple
// Multiple services nest awkwardly with provide() — see Layer for the cure.
interface Db { query(sql: string): Eff<string, never> }
interface Logger { log(msg: string): void }

const Db = service<Db>("Db");
const Logger = service<Logger>("Logger");

const captured: string[] = [];
const app = eff(function* () {
  const db = yield* Db.get;
  const log = yield* Logger.get;
  log.log("querying");
  return yield* db.query("SELECT 1");
});

const wired2 = provide(
  provide(app, Db, { query: (s) => succeed(`row:${s}`) }),
  Logger,
  { log: (m) => captured.push(m) },
);

assertEq(wired2.runSync(), "row:SELECT 1");
assertEq(captured, ["querying"]);
// <<< example

// >>> example: service-multiple-flat
// Multiple services in chainable form — nested .flatMap for each .get.
const capturedFlat: string[] = [];
const appFlat = Db.get.flatMap((db) =>
  Logger.get.flatMap((log) => {
    log.log("querying");
    return db.query("SELECT 1");
  }),
);

const wired2Flat = provide(
  provide(appFlat, Db, { query: (s) => succeed(`row:${s}`) }),
  Logger,
  { log: (m) => capturedFlat.push(m) },
);

assertEq(wired2Flat.runSync(), "row:SELECT 1");
assertEq(capturedFlat, ["querying"]);
// <<< example
