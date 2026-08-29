import { access, readFile } from "node:fs/promises";
import { join } from "node:path";

const packages = [
  {
    dir: "packages/core",
    imports: [
      "dist/stream/index.js",
      "dist/retry/index.js",
      "dist/index.js",
      "dist/worker/index.js",
      "dist/syntax/index.js",
    ],
  },
  { dir: "packages/http", imports: ["dist/index.js"] },
  { dir: "packages/http-otel", imports: ["dist/index.js"] },
  { dir: "packages/kafka", imports: ["dist/index.js"] },
  { dir: "packages/kafka-kafkajs", imports: ["dist/index.js"] },
  { dir: "packages/kafka-platformatic", imports: ["dist/index.js"] },
  { dir: "packages/otel", imports: ["dist/index.js"] },
  { dir: "packages/postgres", imports: ["dist/index.js", "dist/pgmq/index.js"] },
  { dir: "packages/redis", imports: ["dist/index.js"] },
  { dir: "packages/topology", imports: ["dist/index.js"] },
  {
    dir: "packages/transform",
    imports: ["dist/rewrite.js", "dist/bun-plugin.js", "dist/plugin.js"],
  },
];

// @spilne/perfect-swc-plugin ships a wasm artifact built by the Rust job — verify its
// declared entrypoint exists when it has been built (the TS-only CI job runs
// without a Rust toolchain, so absence is tolerated with a warning).
{
  const wasmPath = "packages/swc-plugin/dist/plugin.wasm";
  try {
    await access(wasmPath);
    const { size } = await import("node:fs").then((fs) => fs.statSync(wasmPath));
    if (size < 100_000)
      throw new Error(`@spilne/perfect-swc-plugin: ${wasmPath} suspiciously small (${size} bytes)`);
    console.log(`ok  @spilne/perfect-swc-plugin wasm artifact (${(size / 1024).toFixed(0)} KiB)`);
  } catch (e) {
    if ((e as { code?: string }).code === "ENOENT") {
      console.log(
        "warn @spilne/perfect-swc-plugin wasm not built (run `bun run build:swc`) — skipping",
      );
    } else {
      throw e;
    }
  }
}

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
    const imported = await import(new URL(`../${pkg.dir}/${entry}`, import.meta.url).href);
    if (pkg.dir === "packages/core" && entry === "dist/stream/index.js") {
      const values = await imported.Stream.of(1, 2, 3)
        .map((value: number) => value * 2)
        .toArray()
        .run();
      if (values.join(",") !== "2,4,6")
        throw new Error("@spilne/perfect-core/stream runtime smoke failed");
    }
  }
}

async function assertFile(
  packageDir: string,
  path: string | undefined,
  label: string,
): Promise<void> {
  if (!path) {
    throw new Error(`${label} is missing`);
  }

  const normalized = path.replace(/^\.\//, "");
  if (normalized.includes("*")) return;

  await access(join(packageDir, normalized));
}
