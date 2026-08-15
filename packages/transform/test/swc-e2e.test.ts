// Real-pipeline smoke: run the fixture corpus through @swc/core with the
// built wasm plugin, execute the output against the runtime, and compare
// with the fixtures' expected values. This is the semantic half of the
// differential harness for the SWC side (the Rust suite checks structure).
//
// Skipped when the wasm artifact hasn't been built (`bun run build:swc`).

import { describe, test, expect } from "bun:test";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const WASM = join(import.meta.dir, "../../swc-plugin/dist/plugin.wasm");
const FIXTURES = join(import.meta.dir, "fixtures");
const wasmBuilt = existsSync(WASM);

async function swcTransform(source: string): Promise<string> {
  const swc = await import("@swc/core");
  const out = await swc.transform(source, {
    jsc: {
      parser: { syntax: "typescript" },
      target: "es2022",
      experimental: { plugins: [[WASM, {}]] },
    },
    isModule: true,
  });
  return out.code;
}

function loadFixture(dir: string, name: string): { source: string; expected: unknown } {
  const raw = readFileSync(join(FIXTURES, dir, name), "utf8");
  const header = raw.match(/^\/\/ expect: (.+)$/m);
  if (!header) throw new Error(`fixture ${name} is missing an "// expect:" header`);
  return { source: raw.replace(/^export /gm, ""), expected: JSON.parse(header[1]!) };
}

async function execute(transformed: string): Promise<unknown> {
  const { succeed, fail, sync, run } = await import("../../core/src");
  const fn = new Function(
    "succeed",
    "fail",
    "sync",
    "run",
    `return (async () => { ${transformed}; return await run(program); })()`,
  );
  return fn(succeed, fail, sync, run);
}

describe.skipIf(!wasmBuilt)("fixture corpus through the SWC wasm plugin", () => {
  for (const dir of ["shared", "swc-only"] as const) {
    for (const name of readdirSync(join(FIXTURES, dir)).sort()) {
      test(`${dir}/${name}`, async () => {
        const { source, expected } = loadFixture(dir, name);
        const compiled = await swcTransform(source);
        expect(compiled).not.toContain("eff((");
        expect(compiled).not.toContain("$(");
        expect(await execute(compiled)).toEqual(expected);
      });
    }
  }
});

if (!wasmBuilt) {
  test("swc wasm not built — corpus smoke skipped (run `bun run build:swc`)", () => {
    expect(wasmBuilt).toBe(false);
  });
}
