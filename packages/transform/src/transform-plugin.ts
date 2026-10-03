// The one Bun plugin behind every entry point (bun-plugin, plugin, preload).
//
// There used to be three copies, and they had drifted apart: one forgot to
// add the core imports the rewritten code needs, one only looked at files
// that used eff(($) => …) and so skipped files with only `for { … }`, and
// one skipped any file with "preload" anywhere in its path.

import type { BunPlugin } from "bun";
import { dirname } from "node:path";
import { rewriteEffBlocks } from "./rewrite.js";
import { ensureCoreImports } from "./auto-import.js";

// This package's own source folder. Its files mention the syntax in strings
// and comments, and must never be rewritten.
const OWN_FOLDER = dirname(import.meta.path);

/** True when a file might contain either comprehension syntax. */
export function usesComprehensions(source: string): boolean {
  return source.includes("<-") || /\beff\s*\(\s*\(\s*\$\s*\)/.test(source);
}

/** Rewrite comprehensions and add the core imports the result needs. */
export function transformSource(source: string): string {
  return ensureCoreImports(source, rewriteEffBlocks(source));
}

export function createTransformPlugin(name = "perfect-transform"): BunPlugin {
  return {
    name,
    setup(build) {
      build.onLoad({ filter: /\.(c|m)?tsx?$/ }, async (args) => {
        if (args.path.includes("/node_modules/") || args.path.startsWith(OWN_FOLDER)) {
          return undefined;
        }
        const source = await Bun.file(args.path).text();
        if (!usesComprehensions(source)) return undefined;
        return {
          contents: transformSource(source),
          loader: args.path.endsWith("x") ? "tsx" : "ts",
        };
      });
    },
  };
}
