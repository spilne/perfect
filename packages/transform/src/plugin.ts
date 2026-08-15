import { plugin } from "bun";
import { rewriteEffBlocks } from "./rewrite";
import { ensureCoreImports } from "./auto-import";

plugin({
  name: "spilne-eff-transform",
  setup(build) {
    build.onLoad({ filter: /\.ts$/ }, async (args) => {
      const source = await Bun.file(args.path).text();

      // only transform files that use eff($)
      if (!source.includes("eff(($)") && !source.includes("eff( ($)")) {
        return undefined;
      }

      const transformed = ensureCoreImports(source, rewriteEffBlocks(source));
      return {
        contents: transformed,
        loader: "ts",
      };
    });
  },
});
