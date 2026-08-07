# Services and Layers

Dependency injection, typed and tracked. Define a service interface, get a
tag, request the service inside a program. Provide it once at the edge.

## Services

`service<T>("Name")` creates a tag. The tag's `.get` is an effect that
retrieves the implementation from context (and adds `Needs<T>` to the
effect channel).

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/04-services.ts#service-define -->
```ts
import { eff, succeed, service, provide, type Eff } from "@perfect/core";

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
const wired = provide(program, Greeter, { greet: (name) => succeed(`hello, ${name}`) });

console.log(wired.runSync()); // → "hello, world"
```
<!-- @end -->

:::

::: syntax chainable
<!-- @embed packages/core/examples/04-services.ts#service-define-flat -->
```ts
import { succeed, provide } from "@perfect/core";

// Same program, chainable form: Greeter.get is an effect — flatMap into it.
const programFlat = Greeter.get.flatMap((g) => g.greet("world"));

const wiredFlat = provide(programFlat, Greeter, { greet: (name) => succeed(`hello, ${name}`) });

console.log(wiredFlat.runSync()); // → "hello, world"
```
<!-- @end -->
:::

::::

Multiple services nest with `provide` — readable enough for one or two,
ugly for three or more. That's where Layers come in:

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/04-services.ts#service-multiple -->
```ts
import { eff, succeed, service, provide, type Eff } from "@perfect/core";

// Multiple services nest awkwardly with provide() — see Layer for the cure.
interface Db {
  query(sql: string): Eff<string, never>;
}
interface Logger {
  log(msg: string): void;
}

const Db = service<Db>("Db");
const Logger = service<Logger>("Logger");

const captured: string[] = [];
const app = eff(function* () {
  const db = yield* Db.get;
  const log = yield* Logger.get;
  log.log("querying");
  return yield* db.query("SELECT 1");
});

const wired2 = provide(provide(app, Db, { query: (s) => succeed(`row:${s}`) }), Logger, {
  log: (m) => captured.push(m),
});

console.log(wired2.runSync()); // → "row:SELECT 1"
console.log(captured); // → ["querying"]
```
<!-- @end -->

:::

::: syntax chainable
<!-- @embed packages/core/examples/04-services.ts#service-multiple-flat -->
```ts
import { succeed, provide } from "@perfect/core";

// Multiple services in chainable form — nested .flatMap for each .get.
const capturedFlat: string[] = [];
const appFlat = Db.get.flatMap((db) =>
  Logger.get.flatMap((log) => {
    log.log("querying");
    return db.query("SELECT 1");
  }),
);

const wired2Flat = provide(provide(appFlat, Db, { query: (s) => succeed(`row:${s}`) }), Logger, {
  log: (m) => capturedFlat.push(m),
});

console.log(wired2Flat.runSync()); // → "row:SELECT 1"
console.log(capturedFlat); // → ["querying"]
```
<!-- @end -->
:::

::::

## Layers

A `Layer<S>` is just an `Eff` that produces a record of services. No new
type, no new constructors — reuse `succeed` / `eff` / `scoped`.

<!-- @embed packages/core/examples/05-layers.ts#layer-build -->
```ts
import { succeed } from "@perfect/core";

// Build layers using existing constructors — succeed, eff, scoped.
const DbLive = succeed({ Db: { query: (s: string) => succeed(`db:${s}`) } as Db });

const CacheLive = succeed({
  Cache: { get: () => undefined } as Cache,
});

const LoggerLive = succeed({
  Logger: { log: (m: string) => logs.push(m) } as Logger,
});
```
<!-- @end -->

### Compose

Three equivalent chain styles. Pick whichever reads best at the call site:

<!-- @embed packages/core/examples/05-layers.ts#layer-chain -->
```ts
import { Layer } from "@perfect/core";

// Three equivalent chain styles:
const a = program.with(Layer.merge(DbLive, CacheLive, LoggerLive));
const b = program.with(DbLive.and(CacheLive).and(LoggerLive));
const c = program.with(DbLive).with(CacheLive).with(LoggerLive);
console.log([a.runSync(), b.runSync(), c.runSync()]); // → ["db:SELECT 1", "db:SELECT 1", "db:SELECT 1"]
```
<!-- @end -->

### Apply

`.with(layer)` wraps the program in a `scoped` frame, runs the layer, installs
the services, runs the program. Releases fire in LIFO order on exit.

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/05-layers.ts#layer-apply -->
```ts
import { eff, Layer } from "@perfect/core";

// Compose horizontally with Layer.merge, apply with .with()
const AppLive = Layer.merge(DbLive, CacheLive, LoggerLive);

const program = eff(function* () {
  const db = yield* Db.get;
  const log = yield* Logger.get;
  log.log("running");
  return yield* db.query("SELECT 1");
});

console.log(program.with(AppLive).runSync()); // → "db:SELECT 1"
```
<!-- @end -->

:::

::: syntax chainable
<!-- @embed packages/core/examples/05-layers.ts#layer-apply-flat -->
```ts
// Same program, chainable form — .flatMap into each service, .with() the layer.
const programFlat = Db.get.flatMap((db) =>
  Logger.get.flatMap((log) => {
    log.log("running");
    return db.query("SELECT 1");
  }),
);

console.log(programFlat.with(AppLive).runSync()); // → "db:SELECT 1"
```
<!-- @end -->
:::

::::

### Resources

If a layer uses `acquireRelease`, the release fires when the program ends —
success, failure, or interrupt:

<!-- @embed packages/core/examples/05-layers.ts#layer-scoped -->
```ts
import { eff, sync, acquireRelease, Layer } from "@perfect/core";

// Scoped layer: acquireRelease finalizers fire when the program exits.
const events: string[] = [];
const ScopedLogger = eff(function* () {
  const logger = yield* acquireRelease(
    sync(() => {
      events.push("acquire");
      return { log: (m: string) => events.push(`log:${m}`) } as Logger;
    }),
    () =>
      sync(() => {
        events.push("release");
      }),
  );
  return { Logger: logger };
});

await program.with(Layer.merge(DbLive, CacheLive, ScopedLogger)).run();
console.log(events); // → ["acquire", "log:running", "release"]
```
<!-- @end -->

### Test-time swap

Pass a different layer:

<!-- @embed packages/core/examples/05-layers.ts#layer-test-swap -->
```ts
import { succeed } from "@perfect/core";

// Test-time swap is a one-liner — supply a different layer.
const FakeAll = succeed({
  Db: { query: () => succeed("FAKE") } as Db,
  Cache: { get: () => undefined } as Cache,
  Logger: { log: () => {} } as Logger,
});

console.log(program.with(FakeAll).runSync()); // → "FAKE"
```
<!-- @end -->

## Vertical composition

When one layer's services depend on another's, either:

1. Use `yield*` inline:

   ```ts
   const CacheLive = eff(function* () {
     const { Db } = yield* DbLive;
     return { Cache: new DbBackedCache(Db) };
   });
   ```

2. Or compose explicitly with `.provideTo`:

   ```ts
   const CacheWired = DbLive.provideTo(CacheNeedsDb);
   ```

`.provideTo` consumes the outer layer's services (they don't appear in the
result type); `merge` keeps both available.

## Memoization

Layers build each time they are applied unless you opt into memoization.
Use `.memoize()` when a layer should be constructed at most once per active
scope:

```ts
const DbLive = sync(() => ({ Db: openDb() })).memoize();

const AppLive = Layer.merge(DbLive, DbLive);
// DbLive builds once inside this `.with(...)` scope.
await program.with(AppLive).run();
```

Memoization is scoped, not global. A second independent `program.with(DbLive)`
run builds the layer again and owns its own finalizers.

## API summary

| | |
|---|---|
| `service<T>(name)` | create a service tag |
| `Tag.get` | effect that retrieves the impl, adds `Needs<T>` |
| `provide(eff, tag, impl)` | install a single service |
| `Layer.merge(...)` | horizontal: combine multiple layers |
| `layer.and(other)` | fluent merge — chainable |
| `layer.provideTo(inner)` | vertical: use this layer's outputs to satisfy inner's deps |
| `layer.memoize()` | cache one layer build per active scope |
| `eff.with(layer)` | apply a layer to a program (wraps in `scoped`) |

## Pitfalls

- **Service names must match the record key.** `service<T>("Db")` and
  `succeed({ Db: impl })` resolve to the same `Symbol.for("spilne/svc/Db")`.
- **Never resolve a service inside a tight loop.** Get it once at the top of
  the block, reuse it. See the bench: per-step lookup is ~14× slower.
- **Memoization is per scope.** Chained `.with(A).with(A)` creates nested
  scopes; use `Layer.merge(A.memoize(), A.memoize())` or share the same
  memoized layer inside one `.with(...)` when you want reuse.

## Next

- [Error handling](./05-error-handling.md)
- [Resources and scopes](./07-resources-and-scopes.md)
