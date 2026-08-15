import { plugin } from "bun";
import { rewriteEffBlocks } from "./rewrite";

// The Rust CLI transformer (crates/perfect-transform) is intentionally NOT
// used here: its output diverges from the TS rewriter (guard handling, yield
// desugaring), so silently preferring it when a local binary happens to be
// built would change program semantics. One transformer, one behavior.

plugin({
  name: "perfect-effect-transform",
  setup(build) {
    build.onLoad({ filter: /\.ts$/ }, async (args) => {
      const source = await Bun.file(args.path).text();

      // fast bail: if file doesn't use either syntax, skip
      if (!source.includes("<-") && !source.includes("eff(")) {
        return undefined;
      }

      return { contents: rewriteEffBlocks(source), loader: "ts" };
    });
  },
});
