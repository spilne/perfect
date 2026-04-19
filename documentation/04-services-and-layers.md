# Services and Layers

Dependency injection, typed and tracked. Define a service interface, get a
tag, request the service inside a program. Provide it once at the edge.

## Services

`service<T>("Name")` creates a tag. The tag's `.get` is an effect that
retrieves the implementation from context (and adds `Needs<T>` to the
effect channel).

<!-- @embed packages/core/examples/04-services.ts#service-define -->
```ts
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

assertEq(runSync(wired), "hello, world");
```
<!-- @end -->

Multiple services nest with `provide` — readable enough for one or two,
ugly for three or more. That's where Layers come in:

<!-- @embed packages/core/examples/04-services.ts#service-multiple -->
```ts
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

assertEq(runSync(wired2), "row:SELECT 1");
assertEq(captured, ["querying"]);
```
<!-- @end -->

## Layers

A `Layer<S>` is just an `Eff` that produces a record of services. No new
type, no new constructors — reuse `succeed` / `eff` / `scoped`.

<!-- @embed packages/core/examples/05-layers.ts#layer-build -->
```ts
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
// Three equivalent chain styles:
const a = program.with(Layer.merge(DbLive, CacheLive, LoggerLive));
const b = program.with(DbLive.and(CacheLive).and(LoggerLive));
const c = program.with(DbLive).with(CacheLive).with(LoggerLive);
assertEq([runSync(a), runSync(b), runSync(c)], ["db:SELECT 1", "db:SELECT 1", "db:SELECT 1"]);
```
<!-- @end -->

### Apply

`.with(layer)` wraps the program in a `scoped` frame, runs the layer, installs
the services, runs the program. Releases fire in LIFO order on exit.

<!-- @embed packages/core/examples/05-layers.ts#layer-apply -->
```ts
// Compose horizontally with Layer.merge, apply with .with()
const AppLive = Layer.merge(DbLive, CacheLive, LoggerLive);

const program = eff(function* () {
  const db = yield* Db.get;
  const log = yield* Logger.get;
  log.log("running");
  return yield* db.query("SELECT 1");
});

assertEq(runSync(program.with(AppLive)), "db:SELECT 1");
```
<!-- @end -->

### Resources

If a layer uses `acquireRelease`, the release fires when the program ends —
success, failure, or interrupt:

<!-- @embed packages/core/examples/05-layers.ts#layer-scoped -->
```ts
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
```
<!-- @end -->

### Test-time swap

Pass a different layer:

<!-- @embed packages/core/examples/05-layers.ts#layer-test-swap -->
```ts
// Test-time swap is a one-liner — supply a different layer.
const FakeAll = succeed({
  Db: { query: () => succeed("FAKE") } as Db,
  Cache: { get: () => undefined } as Cache,
  Logger: { log: () => {} } as Logger,
});

assertEq(runSync(program.with(FakeAll)), "FAKE");
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

## API summary

| | |
|---|---|
| `service<T>(name)` | create a service tag |
| `Tag.get` | effect that retrieves the impl, adds `Needs<T>` |
| `provide(eff, tag, impl)` | install a single service |
| `Layer.merge(...)` | horizontal: combine multiple layers |
| `layer.and(other)` | fluent merge — chainable |
| `layer.provideTo(inner)` | vertical: use this layer's outputs to satisfy inner's deps |
| `eff.with(layer)` | apply a layer to a program (wraps in `scoped`) |

## Pitfalls

- **Service names must match the record key.** `service<T>("Db")` and
  `succeed({ Db: impl })` resolve to the same `Symbol.for("spilne/svc/Db")`.
- **Never resolve a service inside a tight loop.** Get it once at the top of
  the block, reuse it. See the bench: per-step lookup is ~14× slower.

## Next

- [Error handling](./05-error-handling.md)
- [Resources and scopes](./07-resources-and-scopes.md)
