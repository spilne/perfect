import { spawnSync } from "node:child_process";
import { readdirSync } from "node:fs";
import { join, resolve } from "node:path";

const LOADER = "./scripts/node-bun-test-loader.mjs";

const ALL_TEST_ROOTS = [
  "packages/core/test",
  "packages/http/test",
  "packages/http-otel/test",
  "packages/otel/test",
  "packages/topology/test",
  "packages/transform/test",
  "packages/kafka/test",
  "packages/redis/test",
  "packages/postgres/test",
  "packages/integration/test",
];

const EXCLUDED_FROM_NODE_CI = new Set([
  "packages/core/test/stream-operators.test.ts",
  "packages/core/test/worker.test.ts",
  "packages/core/test/clock-routing.test.ts",
  "packages/core/test/test-clock.test.ts",
  "packages/kafka/test/commit-batch-within.test.ts",
  "packages/integration/test/kafka.test.ts",
]);

const args = new Set(process.argv.slice(2));
const includeAllTests = args.has("--all");
const listOnly = args.has("--list");

function collectTestFiles(dir, out) {
  const entries = readdirSync(dir, { withFileTypes: true });

  entries.sort((a, b) => a.name.localeCompare(b.name));
  for (const entry of entries) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      collectTestFiles(full, out);
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".test.ts")) {
      out.push(full);
    }
  }
}

const files = [];
for (const root of ALL_TEST_ROOTS) {
  collectTestFiles(root, files);
}

const normalized = files
  .map((file) => file.replace(/\\/g, "/"))
  .filter((file) => includeAllTests || !EXCLUDED_FROM_NODE_CI.has(file))
  .map((file) => resolve(file));

if (normalized.length === 0) {
  console.error("No Node test files found after applying filters.");
  process.exit(1);
}

if (listOnly) {
  normalized.forEach((file) => console.log(file));
  process.exit(0);
}

const result = spawnSync(
  process.execPath,
  ["--test", "--loader", LOADER, ...normalized],
  {
    cwd: process.cwd(),
    stdio: "inherit",
  },
);

if (result.error) {
  throw result.error;
}

if (typeof result.status === "number") {
  process.exit(result.status);
}

process.exit(1);
