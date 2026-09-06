// Smoke test: every example file under examples/ is imported here.
//
// Executes the example assertions. Static typing is checked separately by
// `bun run typecheck:examples`; Bun's transpiler does not check types.

import { describe, test } from "bun:test";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";

const PACKAGES_DIR = join(import.meta.dir, "..", "..");

describe("examples execute without errors", () => {
  for (const pkg of readdirSync(PACKAGES_DIR).sort()) {
    const examplesDir = join(PACKAGES_DIR, pkg, "examples");
    if (!existsSync(examplesDir)) continue;
    for (const entry of readdirSync(examplesDir).sort()) {
      if (!entry.endsWith(".ts")) continue;
      if (entry.startsWith("_")) continue; // helpers, not standalone examples
      test(`${pkg}/${entry}`, async () => {
        // Import waits for the example's top-level await and assertions.
        await import(join(examplesDir, entry));
      });
    }
  }
});
