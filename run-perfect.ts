#!/usr/bin/env bun
// Runner that transforms for-comprehension syntax then executes the file.
// Usage: bun run-perfect.ts demo.ts

import { rewriteEffBlocks } from "./packages/transform/src/rewrite"

const file = process.argv[2]
if (!file) {
  console.error("Usage: bun run-perfect.ts <file.ts>")
  process.exit(1)
}

const source = await Bun.file(file).text()
const transformed = rewriteEffBlocks(source)

// write to a temp file and run it
const tmp = `${file}.~transformed.ts`
await Bun.write(tmp, transformed)

try {
  const proc = Bun.spawn(["bun", tmp], {
    stdout: "inherit",
    stderr: "inherit",
    env: process.env,
  })
  const code = await proc.exited
  process.exit(code)
} finally {
  await Bun.file(tmp).exists() && (await Bun.$`rm ${tmp}`)
}
