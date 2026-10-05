// Runtime utilities: WorkerPool, FileSystem, branded types, graceful
// shutdown, and choosing a scheduler.
//
// Run: bun packages/core/examples/17-runtime-utilities.ts

import {
  eff,
  join,
  provide,
  run,
  runFiber,
  sync,
  FileSystem,
  TestFileSystem,
  WorkerPool,
  nominal,
  refined,
  BrandError,
  createGracefulShutdown,
  SyncScheduler,
  type Brand,
  Config,
  ConfigProvider,
  TestConfigProvider,
} from "../src";
import { assertEq } from "./_assert";

// >>> example: worker-pool
// The function is sent to the worker as source text, so it can only use its
// argument: no variables or imports from outside the function.
const fib = (n: number): number => {
  let [a, b] = [0, 1];
  for (let i = 0; i < n; i++) [a, b] = [b, a + b];
  return a;
};

const results = await eff(function* () {
  const pool = yield* WorkerPool.make(2); // omit the size to use one worker per CPU
  try {
    return yield* pool.parMap([10, 20, 30], fib);
  } finally {
    yield* pool.shutdown();
  }
})
  .orDie()
  .run();
assertEq(results, [55, 6765, 832040]);
// <<< example

// >>> example: file-system
// Code reads files through the FileSystem service. In tests, provide a
// TestFileSystem that keeps everything in memory.
const loadConfig = eff(function* () {
  const fs = yield* FileSystem.get;
  const text = yield* fs.readFile("/etc/app.conf");
  return text.trim();
}).catchTag("FileSystemError", (e) => sync(() => `missing ${e.path}`));

const files = new TestFileSystem({ "/etc/app.conf": "debug=true\n" });
assertEq(await provide(loadConfig, FileSystem, files).run(), "debug=true");
assertEq(
  await provide(loadConfig, FileSystem, new TestFileSystem()).run(),
  "missing /etc/app.conf",
);
// <<< example

// >>> example: brands
// A brand makes two kinds of string (or number) incompatible, so they can't
// be swapped by accident. It costs nothing at runtime.
type UserId = Brand<string, "UserId">;
type OrderId = Brand<string, "OrderId">;
const UserId = nominal<UserId>();
const OrderId = nominal<OrderId>();

function cancelOrder(user: UserId, order: OrderId): string {
  return `${user} cancels ${order}`;
}
assertEq(cancelOrder(UserId("u-1"), OrderId("o-9")), "u-1 cancels o-9");
// cancelOrder(OrderId("o-9"), UserId("u-1")) does not compile.

// refined() also checks the value, and throws BrandError when it is wrong.
type Port = Brand<number, "Port">;
const Port = refined<Port>(
  (n) => Number.isInteger(n) && n > 0 && n < 65_536,
  (n) => `${n} is not a valid port`,
);
assertEq(Port(8080), 8080);
let rejected = "";
try {
  Port(70_000);
} catch (e) {
  if (e instanceof BrandError) rejected = e.message;
}
assertEq(rejected, "70000 is not a valid port");
// <<< example

// >>> example: graceful-shutdown
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
assertEq(shutdown.signal.aborted, true);
assertEq(closed, ["db pool", "kafka producer"]);
// <<< example

// >>> example: sync-scheduler
// SyncScheduler runs fibers only when you call flush(), which makes the
// order of steps in a test fully predictable.
const scheduler = new SyncScheduler();
const steps: string[] = [];
const fiber = runFiber(
  sync(() => steps.push("ran")),
  scheduler,
);
assertEq(steps, []); // nothing yet
scheduler.flush();
assertEq(steps, ["ran"]);
assertEq(await run(join(fiber)), 1);
// <<< example

// >>> example: config
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
assertEq(loaded.port, 3000);
assertEq(loaded.env, "prod");
assertEq(String(loaded.apiKey), "<secret>"); // safe to log

const broken = await provide(settings, ConfigProvider, new TestConfigProvider({ APP_ENV: "qa" }))
  .either()
  .run();
assertEq(
  broken._tag === "Left" ? broken.left.message : "",
  'DATABASE_URL is not set; APP_ENV: "qa" is not one of dev, prod; API_KEY is not set',
);
// <<< example
