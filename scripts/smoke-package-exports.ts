import { access, readFile } from "node:fs/promises";
import { join } from "node:path";

const packages = [
  {
    dir: "packages/core",
    imports: ["dist/index.js", "dist/stream/index.js", "dist/worker/index.js", "dist/syntax/index.js"],
  },
  { dir: "packages/http", imports: ["dist/index.js"] },
  { dir: "packages/http-otel", imports: ["dist/index.js"] },
  { dir: "packages/transform", imports: ["dist/rewrite.js", "dist/bun-plugin.js", "dist/plugin.js"] },
];

for (const pkg of packages) {
  const packageJsonPath = join(pkg.dir, "package.json");
  const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8")) as {
    name: string;
    main?: string;
    types?: string;
    exports?: Record<string, string | { default?: string; types?: string }>;
  };

  await assertFile(pkg.dir, packageJson.main, `${packageJson.name} main`);
  await assertFile(pkg.dir, packageJson.types, `${packageJson.name} types`);

  for (const [subpath, target] of Object.entries(packageJson.exports ?? {})) {
    if (typeof target === "string") {
      await assertFile(pkg.dir, target, `${packageJson.name}${subpath} export`);
      continue;
    }

    await assertFile(pkg.dir, target.default, `${packageJson.name}${subpath} default export`);
    await assertFile(pkg.dir, target.types, `${packageJson.name}${subpath} types export`);
  }

  for (const entry of pkg.imports) {
    await import(new URL(`../${pkg.dir}/${entry}`, import.meta.url).href);
  }
}

async function assertFile(packageDir: string, path: string | undefined, label: string): Promise<void> {
  if (!path) {
    throw new Error(`${label} is missing`);
  }

  const normalized = path.replace(/^\.\//, "");
  if (normalized.includes("*")) return;

  await access(join(packageDir, normalized));
}
