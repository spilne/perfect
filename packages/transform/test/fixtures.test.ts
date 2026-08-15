// Differential fixture corpus — every fixture in fixtures/shared/ must
// compile through the TS rewriter AND the SWC plugin (the Rust suite reads
// the same files; see crates/swc-plugin-perfect/src/tests.rs). The TS side
// executes the rewritten program against the real runtime and compares to
// the `// expect:` header. swc-only/ fixtures use shapes the rewriter
// rejects by design — asserted to throw here, executed in swc-e2e.test.ts.

import { describe, test, expect } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { rewriteEffBlocks, RewriteError } from "../src/rewrite";

const FIXTURES = join(import.meta.dir, "fixtures");

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

describe("shared fixtures through the TS rewriter", () => {
  for (const name of readdirSync(join(FIXTURES, "shared")).sort()) {
    test(name, async () => {
      const { source, expected } = loadFixture("shared", name);
      const transformed = rewriteEffBlocks(source);
      expect(transformed).not.toContain("eff((");
      expect(await execute(transformed)).toEqual(expected);
    });
  }
});

describe("swc-only fixtures are rejected by the rewriter (not miscompiled)", () => {
  test("if-else.ts throws RewriteError", () => {
    const { source } = loadFixture("swc-only", "if-else.ts");
    expect(() => rewriteEffBlocks(source)).toThrow(RewriteError);
  });

  test("expression-body.ts is left untouched (no block body to rewrite)", () => {
    const { source } = loadFixture("swc-only", "expression-body.ts");
    // the rewriter only handles block bodies; expression bodies pass through
    expect(rewriteEffBlocks(source)).toBe(source);
  });
});
