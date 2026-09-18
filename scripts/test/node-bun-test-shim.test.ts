import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { formatEachTitle } from "../node-bun-test-titles.mjs";
import { TITLE_MATRIX } from "./fixtures/bun-test-titles.matrix";

// The Node lane (`bun run test:node`) runs the same test files through
// scripts/node-bun-test-shim.mjs. Fixtures use the `bun:test` features the
// shim emulates; under both runners they must pass with the same titles.

const root = resolve(import.meta.dir, "../..");
const COMPAT_FIXTURE = "scripts/test/fixtures/bun-test-compat.fixture.ts";
const TITLES_FIXTURE = "scripts/test/fixtures/bun-test-titles.fixture.ts";
const node = Bun.which("node");

const COMPAT_TITLES = [
  "scalar 1",
  "scalar 2",
  "tuple 1 and a",
  "tuple 2 and b",
  "pretty 0",
  "pretty -1",
  "pretty 1.5",
  "pretty NaN",
  "pretty Infinity",
  'quoted "str"',
  "alpha has 1",
  "nested deep, missing $zzz",
  "row 0 is x-y",
  "too few only one %s",
  "done callback 1",
  "skipped 1",
  "suite d1 > inner true in d1",
  "suite d2 > inner true in d2",
  "wrapped suite > sees wrapped",
  "expect takes a custom failure message",
];

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function spawn(cmd: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const child = Bun.spawn(cmd, { cwd: root, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { code, stdout, stderr };
}

const XML_ENTITIES: Record<string, string> = {
  "&quot;": '"',
  "&apos;": "'",
  "&lt;": "<",
  "&gt;": ">",
  "&amp;": "&",
};

const decodeXml = (text: string): string =>
  text.replace(/&(?:quot|apos|lt|gt|amp|#\d+);/g, (entity) =>
    entity.startsWith("&#")
      ? String.fromCharCode(Number(entity.slice(2, -1)))
      : XML_ENTITIES[entity]!,
  );

/** Test titles `bun test` reports for a fixture, prefixed by their describe titles. */
async function bunTitles(fixture: string): Promise<string[]> {
  const directory = await mkdtemp(join(tmpdir(), "perfect-bun-test-compat-"));
  directories.push(directory);
  const report = join(directory, "report.xml");
  const result = await spawn([
    process.execPath,
    "test",
    "--reporter=junit",
    `--reporter-outfile=${report}`,
    `./${fixture}`,
  ]);
  expect(result.code, result.stderr).toBe(0);

  const titles: string[] = [];
  const suites: string[] = [];
  // The outermost suite is the file.
  for (const match of (await readFile(report, "utf8")).matchAll(
    /<testsuite name="([^"]*)"|<\/testsuite>|<testcase name="([^"]*)"/g,
  )) {
    if (match[1] !== undefined) suites.push(decodeXml(match[1]));
    else if (match[2] !== undefined)
      titles.push([...suites.slice(1), decodeXml(match[2])].join(" > "));
    else suites.pop();
  }
  return titles;
}

/** Test titles Node reports for a fixture run through the shim. */
async function nodeTitles(fixture: string): Promise<string[]> {
  const result = await spawn([
    node!,
    "--test",
    "--test-reporter=./scripts/test/fixtures/test-names-reporter.mjs",
    "--loader",
    "./scripts/node-bun-test-loader.mjs",
    fixture,
  ]);
  expect(result.code, `${result.stdout}\n${result.stderr}`).toBe(0);

  // A test is reported before the suite around it, one nesting level deeper.
  const pending = new Map<number, string[]>();
  for (const line of result.stdout.split("\n")) {
    if (line.trim() === "") continue;
    const { name, nesting } = JSON.parse(line) as { name: string; nesting: number };
    const children = pending.get(nesting + 1) ?? [];
    pending.delete(nesting + 1);
    const titles = children.length === 0 ? [name] : children.map((child) => `${name} > ${child}`);
    pending.set(nesting, [...(pending.get(nesting) ?? []), ...titles]);
  }
  // Some Node versions wrap a file's tests in a test named after the file.
  const top = pending.get(0) ?? [];
  return top.length > 0 && top.every((title) => title.startsWith(`${resolve(root, fixture)} > `))
    ? top.map((title) => title.slice(resolve(root, fixture).length + 3))
    : top;
}

const matrixTitles = TITLE_MATRIX.flatMap(([title, rows]) =>
  rows.map((row, index) => formatEachTitle(title, Array.isArray(row) ? row : [row], index)),
);

test("bun test reports the compat fixture titles", async () => {
  expect(await bunTitles(COMPAT_FIXTURE)).toEqual(COMPAT_TITLES);
});

test("the shim formats every title in the matrix as Bun does", async () => {
  expect(await bunTitles(TITLES_FIXTURE)).toEqual(matrixTitles);
});

test.skipIf(node === null)(
  "the Node shim runs the compat fixture with the same titles",
  async () => {
    expect(await nodeTitles(COMPAT_FIXTURE)).toEqual(COMPAT_TITLES);
  },
  60_000,
);

test.skipIf(node === null)(
  "the Node shim runs the title matrix with the same titles",
  async () => {
    expect(await nodeTitles(TITLES_FIXTURE)).toEqual(matrixTitles);
  },
  60_000,
);
