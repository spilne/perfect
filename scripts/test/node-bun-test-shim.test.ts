import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

// The Node lane (`bun run test:node`) runs the same test files through
// scripts/node-bun-test-shim.mjs. The fixture uses the `bun:test` features the
// shim emulates; both runners must report the same titles and pass.

const root = resolve(import.meta.dir, "../..");
const FIXTURE = "scripts/test/fixtures/bun-test-compat.fixture.ts";
const node = Bun.which("node");

const EXPECTED_TITLES = [
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

// Test titles from a Bun JUnit report, prefixed by their describe titles. The
// outermost suite is the file.
function junitTitles(xml: string): string[] {
  const titles: string[] = [];
  const suites: string[] = [];
  for (const match of xml.matchAll(
    /<testsuite name="([^"]*)"|<\/testsuite>|<testcase name="([^"]*)"/g,
  )) {
    if (match[1] !== undefined) suites.push(decodeXml(match[1]));
    else if (match[2] !== undefined)
      titles.push([...suites.slice(1), decodeXml(match[2])].join(" > "));
    else suites.pop();
  }
  return titles;
}

// Test titles from node:test's TAP output. A subtest's result line comes
// before its parent's, indented four spaces per level.
function tapTitles(tap: string): string[] {
  const pending = new Map<number, string[]>();
  for (const line of tap.split("\n")) {
    const match = /^( *)(?:not )?ok \d+ - (.*?)(?: # (?:SKIP|TODO).*)?$/.exec(line);
    if (match === null) continue;
    const depth = match[1]!.length / 4;
    const name = match[2]!.replace(/\\#/g, "#").replace(/\\\\/g, "\\");
    const children = pending.get(depth + 1) ?? [];
    pending.delete(depth + 1);
    const entries = children.length === 0 ? [name] : children.map((child) => `${name} > ${child}`);
    pending.set(depth, [...(pending.get(depth) ?? []), ...entries]);
  }
  // Some Node versions wrap a file's tests in a subtest named after the file.
  return (pending.get(0) ?? []).map((title) =>
    title.replace(new RegExp(`^.*${FIXTURE.replaceAll(".", "\\.")} > `), ""),
  );
}

test("bun test reports the fixture titles", async () => {
  const directory = await mkdtemp(join(tmpdir(), "perfect-bun-test-compat-"));
  directories.push(directory);
  const report = join(directory, "report.xml");

  const result = await spawn([
    process.execPath,
    "test",
    "--reporter=junit",
    `--reporter-outfile=${report}`,
    `./${FIXTURE}`,
  ]);

  expect(result.code, result.stderr).toBe(0);
  expect(junitTitles(await readFile(report, "utf8"))).toEqual(EXPECTED_TITLES);
});

test.skipIf(node === null)(
  "the Node shim runs the fixture with the same titles",
  async () => {
    const result = await spawn([
      node!,
      "--test",
      "--test-reporter=tap",
      "--loader",
      "./scripts/node-bun-test-loader.mjs",
      FIXTURE,
    ]);

    expect(result.code, `${result.stdout}\n${result.stderr}`).toBe(0);
    expect(tapTitles(result.stdout)).toEqual(EXPECTED_TITLES);
  },
  60_000,
);
