import { plugin } from "bun"
import { rewriteEffBlocks } from "./rewrite"

const RUST_BINARY = new URL(
  "../../crates/perfect-transform/target/release/perfect-transform",
  import.meta.url,
).pathname

const useRust = await Bun.file(RUST_BINARY).exists()

async function transformWithRust(source: string): Promise<string> {
  const proc = Bun.spawn([RUST_BINARY, "-"], {
    stdin: new Blob([source]),
    stdout: "pipe",
  })
  return await new Response(proc.stdout).text()
}

plugin({
  name: "perfect-effect-transform",
  setup(build) {
    build.onLoad({ filter: /\.ts$/ }, async (args) => {
      const source = await Bun.file(args.path).text()

      // fast bail: if file doesn't use either syntax, skip
      if (!source.includes("<-") && !source.includes("eff(")) {
        return undefined
      }

      const transformed = useRust
        ? await transformWithRust(source)
        : rewriteEffBlocks(source)

      return { contents: transformed, loader: "ts" }
    })
  },
})
