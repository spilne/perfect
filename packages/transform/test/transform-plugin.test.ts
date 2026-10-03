import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTransformPlugin, usesComprehensions } from "../src/transform-plugin";

const folder = mkdtempSync(join(tmpdir(), "perfect-transform-"));
afterAll(() => rmSync(folder, { recursive: true, force: true }));

// Run the plugin's load hook on a file, like Bun would.
async function load(relativePath: string, source: string): Promise<string | undefined> {
  const path = join(folder, relativePath);
  mkdirSync(join(path, ".."), { recursive: true });
  writeFileSync(path, source);
  let onLoad!: (args: { path: string }) => Promise<{ contents: string } | undefined>;
  createTransformPlugin().setup({
    onLoad: (_options: unknown, callback: typeof onLoad) => {
      onLoad = callback;
    },
  } as never);
  return (await onLoad({ path }))?.contents;
}

const forOnly = `const r = for { a <- getA() } yield a + 1;\n`;

describe("the transform plugin", () => {
  test("rewrites a file that only uses for-comprehensions", async () => {
    const out = await load("app/only-for.ts", forOnly);
    expect(out).toContain(".map((a) => a + 1)");
    expect(out).not.toContain("<-");
  });

  test("leaves files in node_modules alone", async () => {
    expect(await load("node_modules/lib/index.ts", forOnly)).toBeUndefined();
  });

  test("still rewrites a file whose path mentions 'preload'", async () => {
    expect(await load("app/preload-data.ts", forOnly)).toContain(".map((a) => a + 1)");
  });

  test("skips files with neither syntax", async () => {
    expect(await load("app/plain.ts", "export const x = 1;\n")).toBeUndefined();
    expect(usesComprehensions("eff(($) => { return 1 })")).toBe(true);
    expect(usesComprehensions("const effort = 1")).toBe(false);
  });
});
