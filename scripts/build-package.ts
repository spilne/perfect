import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { spawnSync } from "node:child_process";

const packageDir = process.argv[2] ?? ".";

const distDir = join(packageDir, "dist");
const srcDir = join(packageDir, "src");

await rm(distDir, { recursive: true, force: true });
await mkdir(distDir, { recursive: true });

const transpiler = new Bun.Transpiler({
  loader: "ts",
  target: "bun",
});

for (const file of await listTypeScriptFiles(srcDir)) {
  const source = await readFile(file, "utf8");
  const output = transpiler.transformSync(source);
  const outFile = join(distDir, relative(srcDir, file).replace(/\.ts$/, ".js"));
  await mkdir(dirname(outFile), { recursive: true });
  await writeFile(outFile, output);
}

const tsc = spawnSync("bunx", ["tsc", "-p", "tsconfig.build.json"], {
  cwd: packageDir,
  stdio: "inherit",
});

if (tsc.status !== 0) {
  process.exit(tsc.status ?? 1);
}

async function listTypeScriptFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listTypeScriptFiles(path)));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".ts") && !entry.name.endsWith(".d.ts")) {
      files.push(path);
    }
  }

  return files;
}
