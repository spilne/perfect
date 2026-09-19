import { spawnSync } from "node:child_process";
import { access, mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

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
    // These entrypoints import "bun" itself, so they are Bun-only by construction
    // and are exempt from the Node ESM resolution check below.
    bunOnly: ["dist/bun-plugin.js", "dist/plugin.js"],
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

  await assertExplicitExtensions(pkg.dir, packageJson.name);
}

await assertNodeEsmResolves();
await assertConsumerTypechecks();

// Bun's resolver and `"moduleResolution": "bundler"` both accept an extensionless
// relative specifier, so `export { x } from "./y"` in dist/ looks fine from inside
// this repo. It is not: every package declares `"type": "module"`, so Node ESM and
// any consumer on `"moduleResolution": "nodenext"` need the real `./y.js`. When the
// consumer also has `skipLibCheck: true` (the common default) the resulting TS2834s
// are swallowed and every imported symbol silently degrades to an error type, which
// surfaces as nonsense like "Expected 0 arguments, but got 1" in *their* code.
async function assertExplicitExtensions(packageDir: string, packageName: string): Promise<void> {
  const specifier =
    /(?:\bfrom\s*|\bimport\s*|\bimport\s*\(\s*|\brequire\s*\(\s*|\bdeclare\s+module\s*)(["'])(\.\.?\/[^"']*)\1/g;
  const distDir = join(packageDir, "dist");
  const offenders: string[] = [];

  for (const file of await listFiles(distDir)) {
    if (!file.endsWith(".js") && !file.endsWith(".d.ts")) continue;
    const source = await readFile(file, "utf8");
    for (const match of source.matchAll(specifier)) {
      const path = match[2]!;
      if (/\.(js|mjs|cjs|json|wasm|node)$/.test(path)) continue;
      offenders.push(`${file}: ${path}`);
    }
  }

  if (offenders.length > 0) {
    throw new Error(
      `${packageName}: ${offenders.length} relative specifier(s) in dist/ have no file extension, ` +
        `so they do not resolve under Node ESM or "moduleResolution": "nodenext". ` +
        `Add the explicit ".js" extension in src/ (tsconfig.build.json runs on "module": "nodenext", ` +
        `which reports these as TS2835 at build time).\n  ` +
        offenders.slice(0, 10).join("\n  "),
    );
  }
}

// The loop above imports each entrypoint through Bun, whose resolver is looser than
// Node's. Re-run the same imports on real Node so a regression cannot hide behind it.
async function assertNodeEsmResolves(): Promise<void> {
  const entries = packages.flatMap((pkg) =>
    pkg.imports
      .filter((entry) => !(pkg as { bunOnly?: string[] }).bunOnly?.includes(entry))
      .map((entry) => resolve(pkg.dir, entry).replaceAll("\\", "/")),
  );
  const program = entries.map((entry) => `await import(${JSON.stringify(entry)});`).join("\n");

  const result = spawnSync("node", ["--input-type=module", "--eval", program], {
    encoding: "utf8",
  });
  if (result.error && (result.error as { code?: string }).code === "ENOENT") {
    console.log("warn node not on PATH — skipping the Node ESM resolution check");
    return;
  }
  if (result.status !== 0) {
    throw new Error(`Node ESM cannot resolve a published entrypoint:\n${result.stderr.trim()}`);
  }
  console.log(`ok  ${entries.length} entrypoints resolve under Node ESM`);
}

// The strongest guard: pack @spilne/perfect-core exactly as `npm publish` would, install
// the tarball into a throwaway consumer, and typecheck that consumer the way a real
// downstream project does — "moduleResolution": "nodenext", declaration emit on, and
// skipLibCheck *off* so broken published types are reported instead of swallowed.
// Catches both packaging regressions: unresolvable relative specifiers (TS2834) and
// public types that the barrel forgets to export (TS2742).
async function assertConsumerTypechecks(): Promise<void> {
  const fixtureDir = "node_modules/.cache/perfect-consumer-smoke";
  const installDir = join(fixtureDir, "node_modules/@spilne/perfect-core");
  await rm(fixtureDir, { recursive: true, force: true });
  await mkdir(join(fixtureDir, "src"), { recursive: true });
  await mkdir(dirname(installDir), { recursive: true });

  const pack = spawnSync(
    "bun",
    ["pm", "pack", "--quiet", "--filename", resolve(fixtureDir, "core.tgz")],
    {
      cwd: "packages/core",
      encoding: "utf8",
    },
  );
  if (pack.status !== 0) throw new Error(`bun pm pack failed:\n${pack.stderr}`);

  const untar = spawnSync("tar", ["-xzf", "core.tgz"], { cwd: fixtureDir, encoding: "utf8" });
  if (untar.status !== 0) throw new Error(`tar failed:\n${untar.stderr}`);
  await rename(join(fixtureDir, "package"), installDir);

  await writeFile(
    join(fixtureDir, "package.json"),
    JSON.stringify({ name: "perfect-consumer-smoke", private: true, type: "module" }, null, 2),
  );
  await writeFile(
    join(fixtureDir, "tsconfig.json"),
    JSON.stringify(
      {
        compilerOptions: {
          target: "ESNext",
          lib: ["ESNext", "DOM"],
          module: "nodenext",
          moduleResolution: "nodenext",
          strict: true,
          declaration: true,
          composite: true,
          skipLibCheck: false,
          types: [],
          rootDir: "src",
          outDir: "dist",
        },
        include: ["src"],
      },
      null,
      2,
    ),
  );
  await writeFile(
    join(fixtureDir, "src/index.ts"),
    [
      `import { succeed, TaggedError } from "@spilne/perfect-core";`,
      `import { Stream } from "@spilne/perfect-core/stream";`,
      `import { RetryPolicy } from "@spilne/perfect-core/retry";`,
      ``,
      `// Subclassing the TaggedError factory forces the consumer's own declaration`,
      `// emit to name TaggedErrorClass / TaggedErrorInstance — TS2742 unless the`,
      `// barrel exports them.`,
      `export class Boom extends TaggedError("Boom")<{ message: string }>() {}`,
      ``,
      `export const boom = new Boom({ message: "kaboom" });`,
      `export const tag: "Boom" = boom._tag;`,
      `export const message: string = boom.message;`,
      ``,
      `export const one = succeed(1).run();`,
      `export const doubled = Stream.of(1, 2, 3).map((n) => n * 2);`,
      `export const backoff = RetryPolicy.exponential(10);`,
      ``,
    ].join("\n"),
  );

  const tsc = spawnSync(resolve("node_modules/.bin/tsc"), ["-p", "tsconfig.json"], {
    cwd: fixtureDir,
    encoding: "utf8",
  });
  if (tsc.status !== 0) {
    throw new Error(
      `a consumer on "moduleResolution": "nodenext" cannot use the packed @spilne/perfect-core:\n` +
        `${tsc.stdout}${tsc.stderr}`.trim(),
    );
  }
  await rm(fixtureDir, { recursive: true, force: true });
  console.log(`ok  packed @spilne/perfect-core typechecks from a nodenext consumer`);
}

async function listFiles(dir: string): Promise<string[]> {
  const out: string[] = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...(await listFiles(path)));
    else if (entry.isFile()) out.push(path);
  }
  return out;
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
