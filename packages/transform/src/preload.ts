import { plugin } from "bun";
import { rewriteEffBlocks } from "./rewrite";
import { ensureCoreImports } from "./auto-import";

await plugin({
  name: "perfect-for-comprehension",
  setup(build) {
    build.onLoad({ filter: /\.ts$/ }, async (args) => {
      // don't transform ourselves or node_modules
      if (args.path.includes("node_modules") || args.path.includes("preload")) {
        return undefined;
      }
      const source = await Bun.file(args.path).text();
      if (!source.includes("<-") && !source.includes("eff(($)")) {
        return undefined;
      }
      const transformed = ensureCoreImports(source, rewriteEffBlocks(source));
      return { contents: transformed, loader: "ts" };
    });
  },
});

export {};
