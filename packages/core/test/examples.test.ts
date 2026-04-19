// Smoke test: every example file under examples/ is imported here.
//
// If TypeScript can't compile any example, this test fails. If any example
// throws on top-level execution, this test fails. This is what guards the
// docs against drift — every snippet referenced in documentation/ comes from
// one of these files via the @embed mechanism.

import { describe, test } from "bun:test";
import { readdirSync } from "node:fs";
import { join } from "node:path";

const EXAMPLES_DIR = join(import.meta.dir, "..", "examples");

describe("examples compile and import without errors", () => {
  for (const entry of readdirSync(EXAMPLES_DIR).sort()) {
    if (!entry.endsWith(".ts")) continue;
    if (entry.startsWith("_")) continue; // helpers, not standalone examples
    test(entry, async () => {
      // Dynamic import: TS compiles + module body runs.
      // Examples should not throw during top-level evaluation.
      // (They may schedule async work via run() — that's fine, we don't await.)
      await import(join(EXAMPLES_DIR, entry));
    });
  }
});
