# Utilities

## Duration

Type-safe time arithmetic. Eliminates magic millisecond numbers in your
code. APIs explicitly typed with `DurationInput` accept
`number | string | Duration`. For APIs that accept milliseconds as a number,
pass `.toMillis()` or `resolveMs(...)`.

<!-- @embed packages/core/examples/15-duration-cache.ts#duration-basics -->

```ts
import { Duration, resolveMs } from "@spilne/perfect-core";

// Type-safe time arithmetic — no more raw `5000` magic numbers.
const fiveMins = Duration.minutes(5);
console.log(fiveMins.toMillis()); // → 300_000

// Arithmetic + comparison
const total = Duration.hours(1).plus(Duration.minutes(30));
console.log(total.toMillis()); // → 5_400_000
console.log(total.gt(Duration.hours(1))); // → true

// Parsing
console.log(Duration.parse("2h").toMillis()); // → 7_200_000
console.log(Duration.parse("500ms").toMillis()); // → 500

// Coercion: APIs that accept `DurationInput` (number | string | Duration)
console.log(resolveMs(100)); // → 100
console.log(resolveMs("5s")); // → 5000
console.log(resolveMs(Duration.hours(1))); // → 3_600_000
```

<!-- @end -->

| API / concept | Behavior |
|---|---|
| `Duration.millis(n)` / `seconds` / `minutes` / `hours` / `days` / `weeks` | factories |
| `Duration.parse("5m")` | parse `ms`, `s`, `m`, `h`, `d`, `w` |
| `Duration.from(input)` | coerce `number | string | Duration` |
| `.toMillis()` / `.toSeconds()` / `.toMinutes()` / `.toHours()` / `.toDays()` | convert |
| `.plus(other)` / `.minus(other)` / `.times(n)` | arithmetic |
| `.gt(o)` / `.gte(o)` / `.lt(o)` / `.lte(o)` / `.eq(o)` | comparison |
| `.toString()` | picks largest natural unit (`"5m"`, `"3h"`) |
| `resolveMs(input)` | shortcut to `Duration.from(input).toMillis()` |

**Tip**: in your own APIs, accept `DurationInput` and use `resolveMs`:

```ts
function delayBy<A, S>(eff: Eff<A, S>, time: DurationInput): Eff<A, S> {
  return sleep(resolveMs(time)).flatMap(() => eff);
}
delayBy(myEff, 500);                // ms number
delayBy(myEff, "5s");               // string
delayBy(myEff, Duration.minutes(2)); // Duration
```

## cached and cachedBy

`cached(eff)` runs an effect once and remembers its result. It is the
simplest way to load something expensive (a config, a token) only once.

<!-- @embed packages/core/examples/16-basic-primitives.ts#cached -->

```ts
import { sync, all, cached } from "@spilne/perfect-core";

// cached(eff) runs eff once and remembers the result. Failures are not
// remembered, so a failed run is tried again next time. Callers that ask
// at the same time share one run.
let loads = 0;
const config = cached(
  sync(() => {
    loads++;
    return { region: "eu" };
  }),
  { ttlMs: 60_000 }, // optional: forget the value after a minute
);

await all([config, config, config]).run();
console.log(loads); // → 1

await config.invalidate.run(); // forget it now
await config.run();
console.log(loads); // → 2
```

<!-- @end -->

- Only successes are remembered. If the effect fails, the next run tries
  again.
- `ttlMs` makes the value expire after that many milliseconds.
- `.invalidate` forgets the value now; `.current` reads it without running
  anything (`undefined` when there is none); `.isFresh` says whether a value
  is there and not expired.

`cachedBy(build)` does the same per key:

<!-- @embed packages/core/examples/16-basic-primitives.ts#cached-by -->

```ts
import { sync, cachedBy } from "@spilne/perfect-core";

// cachedBy keeps one cached value per key. maxSize drops the least recently
// used key when the cache is full.
const fetched: string[] = [];
const users = cachedBy(
  (id: string) =>
    sync(() => {
      fetched.push(id);
      return { id, name: `user ${id}` };
    }),
  { ttlMs: 30_000, maxSize: 1_000 },
);

await users.get("a").run();
await users.get("a").run();
await users.get("b").run();
console.log(fetched); // → ["a", "b"]
```

<!-- @end -->

| option | what it does |
| --- | --- |
| `ttlMs` | expiry in ms, or a function `(value) => ms` to pick it per value |
| `maxSize` | keep at most this many keys; the least recently used one is dropped |
| `keyFn` | turn a key into the string used to store it (for object keys) |

The keyed cache has `get(key)`, `invalidate(key)`, `invalidateAll`,
`has(key)` and `size`.

## CacheStore

Pluggable key-value cache with TTL + LRU eviction. The `CacheStore<K, V>`
interface is `Eff`-typed; in-process implementation ships with
`@spilne/perfect-core`. Distributed backends (Redis, memcached) implement the
same interface.

This is the **storage layer** — different from the
`cached` / `cachedBy` *combinators*, which provide
closed-over memoization built on a private Map. Use `cached(eff)` for
"memoize this one effect"; use `CacheStore` when you need a pluggable
key-value backend.

### In-memory store

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-memory -->

```ts
import { eff, CacheStore } from "@spilne/perfect-core";

// In-process LRU + TTL cache. Pluggable behind the CacheStore interface —
// distributed backends (Redis, memcached) implement the same shape.
const store = CacheStore.memory<string, number>({
  ttlMs: 60_000,
  maxSize: 100,
});

await eff(function* () {
  yield* store.set("hits", 0);
  yield* store.set("hits", 1);
  const v = yield* store.get("hits");
  console.log(v); // → 1

  const present = yield* store.has("hits");
  console.log(present); // → true

  yield* store.delete("hits");
  const after = yield* store.get("hits");
  console.log(after); // → undefined
}).run();
```

<!-- @end -->

### TTL — default + per-entry override

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-ttl -->

```ts
import { CacheStore } from "@spilne/perfect-core";

// Per-entry TTL overrides the store default.
const ttlStore = CacheStore.memory<string, string>({ ttlMs: 60_000 });

ttlStore.set("short", "expires-fast", 30).runSync(); // overrides default
ttlStore.set("long", "stays-around").runSync(); // uses default 60s

console.log(ttlStore.get("short").runSync()); // → "expires-fast"
await new Promise((r) => setTimeout(r, 40));
// expired
console.log(ttlStore.get("short").runSync()); // → undefined
console.log(ttlStore.get("long").runSync()); // → "stays-around"
```

<!-- @end -->

### LRU eviction

<!-- @embed packages/core/examples/15-duration-cache.ts#cache-store-lru -->

```ts
import { CacheStore } from "@spilne/perfect-core";

// LRU eviction at maxSize.
const lru = CacheStore.memory<string, number>({ maxSize: 3 });
lru.set("a", 1).runSync();
lru.set("b", 2).runSync();
lru.set("c", 3).runSync();
lru.get("a").runSync(); // touches "a" → most recent
lru.set("d", 4).runSync(); // evicts "b" (now LRU), not "a"
console.log(lru.has("a").runSync()); // → true
console.log(lru.has("b").runSync()); // → false
```

<!-- @end -->

### API

| API / concept | Behavior |
|---|---|
| `CacheStore.memory<K, V>({ ttlMs?, maxSize? })` | in-process LRU + TTL |
| `store.get(k)` | returns `V | undefined` (`undefined` if missing or expired) |
| `store.set(k, v, ttlMs?)` | per-entry TTL overrides default |
| `store.delete(k)` / `store.clear()` | removal |
| `store.has(k)` | presence check |
| `store.size` | current entry count |

### Distributed backends

`@spilne/perfect-redis` ships the distributed implementation. It preserves the same
interface while adding `Throws<RedisError>` to each effect:

```ts
import { RedisCacheStore } from "@spilne/perfect-redis";

const store = RedisCacheStore.make<string, User>({
  redis,
  prefix: "users:",
  ttlMs: 60_000,
});
```

The implementation uses prefix-scoped scanning for `clear()` and supports a
custom value codec and key encoder. See
[Distributed backends](./17-distributed-backends.md#redis).

## Config

`Config` reads settings, from environment variables by default, as typed
values. A missing or invalid setting is a typed `ConfigError`, and
`Config.all` lists every problem at once, so a misconfigured deploy is fixed
in one go.

<!-- @embed packages/core/examples/17-runtime-utilities.ts#config -->

```ts
import { provide, run, Config, ConfigProvider, TestConfigProvider } from "@spilne/perfect-core";

// Read settings as typed values. Config.all reports every missing or invalid
// setting at once. In tests, provide a TestConfigProvider instead of the
// environment.
const settings = Config.all({
  port: Config.number("PORT", { default: 3000 }),
  database: Config.url("DATABASE_URL"),
  env: Config.oneOf("APP_ENV", ["dev", "prod"]),
  apiKey: Config.secret("API_KEY"),
});

const loaded = await provide(
  settings,
  ConfigProvider,
  new TestConfigProvider({
    DATABASE_URL: "postgres://db:5432/app",
    APP_ENV: "prod",
    API_KEY: "s3cr3t",
  }),
)
  .orDie()
  .run();
console.log(loaded.port); // → 3000
console.log(loaded.env); // → "prod"
// safe to log
console.log(String(loaded.apiKey)); // → "<secret>"

const broken = await provide(settings, ConfigProvider, new TestConfigProvider({ APP_ENV: "qa" }))
  .either()
  .run();
assertEq(
  broken._tag === "Left" ? broken.left.message : "",
  'DATABASE_URL is not set; APP_ENV: "qa" is not one of dev, prod; API_KEY is not set',
);
```

<!-- @end -->

| Reader | Gives |
| --- | --- |
| `Config.string(name)` | the text |
| `Config.number(name, { integer?, min?, max? })` | a number |
| `Config.boolean(name)` | `true` for true/1/yes/on, `false` for false/0/no/off |
| `Config.url(name)` | a `URL` |
| `Config.duration(name)` | milliseconds, from `500`, `"30s"`, `"5m"`… |
| `Config.oneOf(name, ["dev", "prod"])` | one of the listed words |
| `Config.secret(name)` | a `Secret` that prints as `<secret>`; `.value()` gives the text |
| `Config.optional(reader)` | `undefined` when the setting isn't set |
| `Config.all({ ... })` | an object of all of them |

Every reader takes `{ default }`, used when the setting isn't set. A value
that is set but invalid is still an error. In tests, provide
`new TestConfigProvider({ NAME: "value" })` for `ConfigProvider`.

## FileSystem

`FileSystem` is a service for reading and writing files. Every operation
that can fail at the OS level (a missing file, no permission) fails with a
typed `FileSystemError` that says which operation and path failed, instead
of throwing. Provide `realFileSystem` in your app and `TestFileSystem` in
tests.

<!-- @embed packages/core/examples/17-runtime-utilities.ts#file-system -->

```ts
import { eff, provide, run, sync, FileSystem, TestFileSystem } from "@spilne/perfect-core";

// Code reads files through the FileSystem service. In tests, provide a
// TestFileSystem that keeps everything in memory.
const loadConfig = eff(function* () {
  const fs = yield* FileSystem.get;
  const text = yield* fs.readFile("/etc/app.conf");
  return text.trim();
}).catchTag("FileSystemError", (e) => sync(() => `missing ${e.path}`));

const files = new TestFileSystem({ "/etc/app.conf": "debug=true\n" });
console.log(await provide(loadConfig, FileSystem, files).run()); // → "debug=true"
assertEq(
  await provide(loadConfig, FileSystem, new TestFileSystem()).run(),
  "missing /etc/app.conf",
);
```

<!-- @end -->

| API | What it does |
| --- | --- |
| `readFile(path)` / `readFileBytes(path)` | read as text / bytes |
| `writeFile(path, contents)` / `appendFile(path, text)` | write or append |
| `exists(path)` | never fails, just `true` / `false` |
| `remove(path, { recursive? })` / `mkdir(path, { recursive? })` | delete / create |
| `readDir(path)` / `stat(path)` | list a folder / size, type and modification time |
| `watch(path, { recursive? })` | a `Stream` of changes; it runs until you stop it (`take`, `interruptOn`), and the watcher is closed when the stream ends |

`TestFileSystem` keeps everything in memory and does not normalize paths,
so use one spelling (`/a/b`, not `/a/./b`) in a test.

## Branded types — `nominal` and `refined`

A user id and an order id are both strings, so TypeScript lets you pass one
where the other is expected. A brand makes them different types. It only
exists in the types, so it costs nothing at runtime.

<!-- @embed packages/core/examples/17-runtime-utilities.ts#brands -->

```ts
import { nominal, refined, BrandError, type Brand } from "@spilne/perfect-core";

// A brand makes two kinds of string (or number) incompatible, so they can't
// be swapped by accident. It costs nothing at runtime.
type UserId = Brand<string, "UserId">;
type OrderId = Brand<string, "OrderId">;
const UserId = nominal<UserId>();
const OrderId = nominal<OrderId>();

function cancelOrder(user: UserId, order: OrderId): string {
  return `${user} cancels ${order}`;
}
console.log(cancelOrder(UserId("u-1"), OrderId("o-9"))); // → "u-1 cancels o-9"
// cancelOrder(OrderId("o-9"), UserId("u-1")) does not compile.

// refined() also checks the value, and throws BrandError when it is wrong.
type Port = Brand<number, "Port">;
const Port = refined<Port>(
  (n) => Number.isInteger(n) && n > 0 && n < 65_536,
  (n) => `${n} is not a valid port`,
);
console.log(Port(8080)); // → 8080
let rejected = "";
try {
  Port(70_000);
} catch (e) {
  if (e instanceof BrandError) rejected = e.message;
}
console.log(rejected); // → "70000 is not a valid port"
```

<!-- @end -->

Brand identifiers that are easy to mix up (ids, topic names, offsets). Plain
text like log messages doesn't need it.

## Graceful shutdown

`createGracefulShutdown()` gives you one place to stop everything when the
process is asked to exit:

<!-- @embed packages/core/examples/17-runtime-utilities.ts#graceful-shutdown -->

```ts
import { run, createGracefulShutdown } from "@spilne/perfect-core";

// One object to stop everything: streams stop on `signal`, other resources
// register a teardown with `onShutdown`. run() aborts the signal and waits
// for every teardown, so work in flight can finish first.
const shutdown = createGracefulShutdown();
const closed: string[] = [];
shutdown.onShutdown(async () => {
  closed.push("db pool");
});
shutdown.onShutdown(async () => {
  closed.push("kafka producer");
});

// In an app: process.once("SIGTERM", () => shutdown.run().then(() => process.exit(0)));
await shutdown.run();
await shutdown.run(); // safe to call twice; teardowns run once
console.log(shutdown.signal.aborted); // → true
console.log(closed); // → ["db pool", "kafka producer"]
```

<!-- @end -->

- `signal` is an `AbortSignal`; connect streams to it with
  `stream.interruptOn(shutdown.signal)`.
- `onShutdown(close)` registers a cleanup function (close a client, flush a
  producer).
- `run()` aborts the signal and waits for every cleanup. A cleanup that
  fails doesn't stop the others. Calling `run()` again returns the same
  promise, so two signals in a row are harmless.

## Next

- [Resilience + Coordination Primitives](./11-resilience-and-coordination.md)
- [Comparison vs other libs](./comparison.md)
