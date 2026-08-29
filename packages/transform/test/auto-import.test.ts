import { describe, test, expect } from "bun:test";
import { ensureCoreImports } from "../src/auto-import";
import { rewriteEffBlocks } from "../src/rewrite";

function process(source: string): string {
  return ensureCoreImports(source, rewriteEffBlocks(source));
}

describe("ensureCoreImports", () => {
  test("adds succeed + sync when the transform introduced them", () => {
    const out = process(`const p = eff(($) => { const x = $(a); log(x); return x; });`);
    expect(out).toStartWith(`import { succeed, sync } from "@spilne/perfect-core";`);
  });

  test("adds only the helpers actually used", () => {
    const out = process(`const p = eff(($) => { const x = $(a); return x; });`);
    expect(out).toStartWith(`import { succeed } from "@spilne/perfect-core";`);
    expect(out).not.toContain("sync");
  });

  test("does not duplicate existing imports", () => {
    const src = `import { eff, succeed, sync } from "@spilne/perfect-core";\nconst p = eff(($) => { const x = $(a); log(x); return x; });`;
    const out = process(src);
    expect(out.match(/@spilne\/perfect-core/g)?.length).toBe(1);
  });

  test("aliased imports do not count as bindings", () => {
    const src = `import { succeed as ok } from "@spilne/perfect-core";\nconst p = eff(($) => { const x = $(a); return x; });`;
    const out = process(src);
    expect(out).toStartWith(`import { succeed } from "@spilne/perfect-core";`);
  });

  test("untransformed source passes through untouched", () => {
    const src = `const x = 1;`;
    expect(process(src)).toBe(src);
  });
});
