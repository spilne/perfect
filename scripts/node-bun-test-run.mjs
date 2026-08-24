import { readdirSync, readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
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
  // Uses Bun.serve/Bun.sleep to drive a slow response body; there is no
  // node equivalent in this harness.
  "packages/http/test/transport-body-lifetime.test.ts",
]);

const args = process.argv.slice(2);
const argSet = new Set(args);
const includeAllTests = argSet.has("--all");
const listOnly = argSet.has("--list");
const includeCoverage = argSet.has("--coverage");

if (includeCoverage) {
  argSet.delete("--coverage");
}

let coverageDir = resolve(process.cwd(), "coverage/node");
for (const arg of argSet) {
  if (arg.startsWith("--coverage-dir=")) {
    coverageDir = resolve(process.cwd(), arg.slice("--coverage-dir=".length));
    argSet.delete(arg);
    break;
  }
}

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

function collectCoverageReports(dir, out) {
  const entries = readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      collectCoverageReports(full, out);
      continue;
    }
    if (entry.isFile() && full.endsWith(".json")) {
      out.push(full);
    }
  }
}

function readCoverageSummary(dir) {
  let files = [];
  try {
    collectCoverageReports(dir, files);
  } catch {
    console.log(`[coverage] no coverage files found in ${dir}`);
    return;
  }

  let totalFunctions = 0;
  let coveredFunctions = 0;
  for (const file of files) {
    const raw = readFileSync(file, "utf8");
    const parsed = JSON.parse(raw);
    const entries = Array.isArray(parsed.result) ? parsed.result : [];
    for (const entry of entries) {
      const functions = Array.isArray(entry.functions) ? entry.functions : [];
      totalFunctions += functions.length;
      for (const fn of functions) {
        const ranges = Array.isArray(fn.ranges) ? fn.ranges : [];
        const hit = ranges.some((range) => typeof range.count === "number" && range.count > 0);
        if (hit) {
          coveredFunctions += 1;
        }
      }
    }
  }

  if (totalFunctions === 0) {
    console.log("[coverage] no function coverage could be extracted.");
    return;
  }

  const percent = ((coveredFunctions / totalFunctions) * 100).toFixed(2);
  console.log(`[coverage] function coverage: ${coveredFunctions}/${totalFunctions} (${percent}%)`);
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
  [
    "--test",
    ...(includeCoverage ? ["--experimental-test-coverage"] : []),
    "--loader",
    LOADER,
    ...normalized,
  ],
  {
    cwd: process.cwd(),
    stdio: "inherit",
    env: {
      ...process.env,
      NODE_V8_COVERAGE: includeCoverage ? coverageDir : process.env.NODE_V8_COVERAGE,
    },
  },
);

if (result.error) {
  throw result.error;
}

if (includeCoverage) {
  readCoverageSummary(coverageDir);
  console.log(`[coverage] raw report files in ${coverageDir}`);
}

if (typeof result.status === "number") {
  process.exit(result.status);
}

process.exit(1);
