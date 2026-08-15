// Regression tests for the rewriter hardening: string/comment safety,
// true nesting, multi-line binds, destructuring, and diagnostics-over-silence.

import { describe, test, expect } from "bun:test";
import { rewriteEffBlocks, RewriteError } from "../src/rewrite";

async function exec(source: string): Promise<any> {
  const transformed = rewriteEffBlocks(source);
  const { succeed, fail, sync, run } = await import("../../core/src");
  const fn = new Function(
    "succeed",
    "fail",
    "sync",
    "run",
    `return (async () => { ${transformed} })()`,
  );
  return fn(succeed, fail, sync, run);
}

describe("string/comment safety", () => {
  test("for-comprehension syntax inside a string literal is untouched", () => {
    const src = `const s = "for { x <- y } yield x";`;
    expect(rewriteEffBlocks(src)).toBe(src);
  });

  test("eff syntax inside a template literal is untouched", () => {
    const src = "const s = `eff(($) => { const x = $(e); return x })`;";
    expect(rewriteEffBlocks(src)).toBe(src);
  });

  test("for-comprehension inside a line comment is untouched", () => {
    const src = `// for { x <- y } yield x\nconst a = 1;`;
    expect(rewriteEffBlocks(src)).toBe(src);
  });

  test("for-comprehension inside a block comment is untouched", () => {
    const src = `/* for { x <- y } yield x */ const a = 1;`;
    expect(rewriteEffBlocks(src)).toBe(src);
  });

  test("comment lines inside an eff block are dropped, not miscompiled", async () => {
    const result = await exec(`
      const program = eff(($) => {
        // this comment must not swallow generated code
        const x = $(succeed(5));
        return x * 2;
      });
      return await run(program);
    `);
    expect(result).toBe(10);
  });
});

describe("nesting", () => {
  test("for-comprehension nested inside a bind expression", async () => {
    const result = await exec(`
      const program = for {
        x <- (for { a <- succeed(2) } yield a * 10)
        y <- succeed(1)
      } yield x + y
      return await run(program)
    `);
    expect(result).toBe(21);
  });

  test("eff block nested inside an eff bind", async () => {
    const result = await exec(`
      const inner = eff(($) => {
        const a = $(succeed(3));
        return a + 1;
      });
      const program = eff(($) => {
        const x = $(inner);
        const y = $(eff(($) => { return $(succeed(10)); }));
        return x + y;
      });
      return await run(program);
    `);
    expect(result).toBe(14);
  });
});

describe("previously-miscompiled shapes now supported", () => {
  test("multi-line bind expression", async () => {
    const result = await exec(`
      const program = for {
        x <- succeed(
          40
        )
        y <- succeed(2)
      } yield x + y
      return await run(program)
    `);
    expect(result).toBe(42);
  });

  test("destructuring bind in eff block", async () => {
    const result = await exec(`
      const program = eff(($) => {
        const { a, b } = $(succeed({ a: 1, b: 2 }));
        return a + b;
      });
      return await run(program);
    `);
    expect(result).toBe(3);
  });

  test("array destructuring bind in eff block", async () => {
    const result = await exec(`
      const program = eff(($) => {
        const [x, y] = $(succeed([7, 3]));
        return x - y;
      });
      return await run(program);
    `);
    expect(result).toBe(4);
  });

  test("newline between body close and eff close paren", async () => {
    const result = await exec(`
      const program = eff(($) => {
        const x = $(succeed(9));
        return x;
      }
      );
      return await run(program);
    `);
    expect(result).toBe(9);
  });

  test("bare return compiles to succeed(undefined)", () => {
    const out = rewriteEffBlocks(`const p = eff(($) => { $(a()); return; });`);
    expect(out).toContain("succeed(undefined)");
  });
});

describe("diagnostics over silence", () => {
  test("$ inside a larger expression throws", () => {
    expect(() => rewriteEffBlocks(`const p = eff(($) => { const x = $(a()) + $(b()); });`)).toThrow(
      RewriteError,
    );
  });

  test("$ inside a function call argument throws", () => {
    expect(() => rewriteEffBlocks(`const p = eff(($) => { const x = f($(e)); });`)).toThrow(
      RewriteError,
    );
  });

  test("$ inside an if statement throws with SWC-plugin guidance", () => {
    expect(() => rewriteEffBlocks(`const p = eff(($) => { if (c) { $(e); } });`)).toThrow(
      /SWC plugin/,
    );
  });

  test("$ embedded in a return expression throws", () => {
    expect(() => rewriteEffBlocks(`const p = eff(($) => { return $(a()) + 1; });`)).toThrow(
      RewriteError,
    );
  });

  test("nested $() throws instead of miscompiling", () => {
    expect(() => rewriteEffBlocks(`const p = eff(($) => { const x = $(f($(y))); });`)).toThrow(
      RewriteError,
    );
  });

  test("unsupported bind pattern in for-comprehension throws", () => {
    expect(() => rewriteEffBlocks(`const p = for { { a, b } <- getX() } yield a;`)).toThrow(
      RewriteError,
    );
  });

  test("guards still throw with guidance", () => {
    expect(() => rewriteEffBlocks(`const p = for { x <- getX()\n if (x > 1) } yield x;`)).toThrow(
      /guards/,
    );
  });

  test("plain statements without $ still sync-wrap fine", async () => {
    const result = await exec(`
      let log = 0;
      const program = eff(($) => {
        const x = $(succeed(2));
        log = x;
        return x + 1;
      });
      const r = await run(program);
      return r + log;
    `);
    expect(result).toBe(5);
  });
});
