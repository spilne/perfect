# Services and Layers

Dependency injection, typed and tracked. Define a service interface, get a
tag, request the service inside a program. Provide it once at the edge.

## Services

`service<T>()("Name")` creates a tag. The tag's `.get` is an effect that
retrieves the implementation from context (and adds `Needs<T, "Name">` to the
effect channel).

Why two calls? The second call keeps the name as a literal type (`"Db"`, not
just `string`). `provide` and layers check both the name and the type, so two
services with the same shape can't be mixed up. Keep names literal rather than
widening tags to `ServiceTag<T>` when they will be used for provisioning.

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/04-services.ts#service-define -->

```ts
import { eff, succeed, service, provide, type Eff } from "@spilne/perfect-core";

// Define a service interface and a tag.
interface Greeter {
  greet(name: string): Eff<string, never>;
}
const Greeter = service<Greeter>()("Greeter");

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
import { succeed, provide } from "@spilne/perfect-core";

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
import { eff, succeed, service, provide, type Eff } from "@spilne/perfect-core";

// Multiple services nest awkwardly with provide() — see Layer for the cure.
interface Db {
  query(sql: string): Eff<string, never>;
}
interface Logger {
  log(msg: string): void;
}

const Db = service<Db>()("Db");
const Logger = service<Logger>()("Logger");

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
import { succeed, provide } from "@spilne/perfect-core";

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
import { succeed } from "@spilne/perfect-core";

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
import { Layer } from "@spilne/perfect-core";

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
import { eff, Layer } from "@spilne/perfect-core";

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
import { eff, sync, acquireRelease, Layer } from "@spilne/perfect-core";

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
import { succeed } from "@spilne/perfect-core";

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

   Note: this builds `Db` again every time `CacheLive` is built. If other
   layers also need `Db`, memoize it once (`const DbShared = DbLive.memoize()`)
   and `yield*` that instead.

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

## Automatic ordering with `Layer.build`

With `Layer.merge` you list layers in the right order yourself. `Layer.build`
works it out: describe what each layer provides and needs, and it builds every
layer after the ones it depends on. Each layer can use the services built
before it, and the result type no longer asks for services that another layer
in the same build provides.

<!-- @embed packages/core/examples/05-layers.ts#layer-auto-wire -->

```ts
import { eff, succeed, sync, Layer } from "@spilne/perfect-core";

// Layer.build puts layers in the right order for you. Each layer says what
// it provides and what it needs (Layer.describe), and build() makes sure a
// layer is built after the layers it needs, whatever order you pass them in.
const built: string[] = [];
const DbWired = Layer.describe(
  { provides: ["Db"] },
  sync(() => {
    built.push("Db");
    return { Db: { query: (s: string) => succeed(`db:${s}`) } as Db };
  }),
);
const CacheWired = Layer.describe(
  { provides: ["Cache"], requires: ["Db"] },
  eff(function* () {
    yield* Db.get; // a real cache would load from the database here
    built.push("Cache");
    return { Cache: { get: (k: string) => k } as Cache };
  }),
);

const AppWired = Layer.build(CacheWired, DbWired); // Cache listed first on purpose

const lookup = eff(function* () {
  const cache = yield* Cache.get;
  return cache.get("user:1");
});
console.log(lookup.with(AppWired).runSync()); // → "user:1"
console.log(built); // → ["Db", "Cache"]
```

<!-- @end -->

Wiring mistakes are reported as soon as `Layer.build` is called, not when the
program runs:

- `LayerMissingDependencyError`: a layer needs a service that no layer in the
  build provides.
- `LayerCycleError`: layers need each other in a loop (`A -> B -> A`).

Layers without `Layer.describe` count as providing and needing nothing, so
they are built first.

## API summary

| API / concept | Behavior |
|---|---|
| `service<T>()(name)` | create a service tag |
| `Tag.get` | retrieves the implementation, adds `Needs<T, Name>` |
| `provide(eff, tag, impl)` | install a single service |
| `Layer.merge(...)` | horizontal: combine multiple layers |
| `layer.and(other)` | fluent merge — chainable |
| `layer.provideTo(inner)` | vertical: use this layer's outputs to satisfy inner's deps |
| `layer.memoize()` | cache one layer build per active scope |
| `Layer.build(...layers)` | merge layers in dependency order (see above) |
| `Layer.describe({ provides, requires }, layer)` | tell `Layer.build` what a layer provides and needs |
| `eff.with(layer)` | apply a layer to a program (wraps in `scoped`) |

## Pitfalls

- **Service names must match the record key.** `service<T>()("Db")` and
  `succeed({ Db: impl })` resolve to the same `Symbol.for("spilne/svc/Db")`.
- **Resolve reusable services outside hot loops.** Fetch once at the start
  of the operation when the context is unchanged, then reuse the implementation.
- **Memoization is per scope.** Chained `.with(A).with(A)` creates nested
  scopes. To build `A` only once, memoize it once and reuse that value:
  `const AMemo = A.memoize(); Layer.merge(AMemo, AMemo)`. Calling
  `.memoize()` twice gives two separate caches, so `A` would be built twice.

## Next

- [Error handling](./05-error-handling.md)
- [Resources and scopes](./07-resources-and-scopes.md)
