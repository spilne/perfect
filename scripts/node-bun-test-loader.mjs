import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const BUN_TEST_SHIM = new URL("./node-bun-test-shim.mjs", import.meta.url).href;
const BUN_TRANSPILER = new URL("./node-bun-transpile-ts.mjs", import.meta.url);
const PACKAGES_DIR = path.resolve(fileURLToPath(new URL("../packages", import.meta.url)));

const toFileUrl = (absolutePath) => pathToFileURL(absolutePath).href;
const hasTypeScriptExtension = (requestPath) =>
  requestPath.endsWith(".ts") || requestPath.endsWith(".tsx");

const resolveLocalImport = (candidatePath) => {
  if (hasTypeScriptExtension(candidatePath) && existsSync(candidatePath)) {
    return candidatePath;
  }

  if (existsSync(`${candidatePath}.ts`)) {
    return `${candidatePath}.ts`;
  }

  if (existsSync(`${candidatePath}.tsx`)) {
    return `${candidatePath}.tsx`;
  }

  if (existsSync(candidatePath) && existsSync(path.join(candidatePath, "index.ts"))) {
    return path.join(candidatePath, "index.ts");
  }

  if (existsSync(candidatePath) && existsSync(path.join(candidatePath, "index.tsx"))) {
    return path.join(candidatePath, "index.tsx");
  }

  return null;
};

const resolvePackageImport = (specifier) => {
  const match = specifier.match(/^@spilne\/perfect-([^/]+)(\/.*)?$/);
  if (!match) {
    return null;
  }

  const packageName = match[1];
  const subPath = (match[2] ?? "").replace(/^\//, "");
  const basePath = path.join(PACKAGES_DIR, packageName, "src");

  if (!existsSync(basePath)) {
    return null;
  }

  const candidateBase =
    subPath === "" ? path.join(basePath, "index.ts") : path.join(basePath, subPath);

  if (hasTypeScriptExtension(candidateBase) && existsSync(candidateBase)) {
    return toFileUrl(candidateBase);
  }

  if (existsSync(`${candidateBase}.ts`)) {
    return toFileUrl(`${candidateBase}.ts`);
  }

  if (existsSync(path.join(candidateBase, "index.ts"))) {
    return toFileUrl(path.join(candidateBase, "index.ts"));
  }

  return null;
};

export async function resolve(specifier, context, nextResolve) {
  if (specifier === "bun:test") {
    return {
      url: BUN_TEST_SHIM,
      shortCircuit: true,
    };
  }

  if (context.parentURL && (specifier.startsWith(".") || specifier.startsWith("/"))) {
    try {
      const candidatePath = fileURLToPath(new URL(specifier, context.parentURL));
      const localImport = resolveLocalImport(candidatePath);
      if (localImport !== null) {
        return {
          url: pathToFileURL(localImport).href,
          shortCircuit: true,
        };
      }
    } catch {
      // noop: let Node resolve continue for special cases.
    }
  }

  const packageImport = resolvePackageImport(specifier);
  if (packageImport !== null) {
    return {
      url: packageImport,
      shortCircuit: true,
    };
  }

  return nextResolve(specifier, context);
}

export async function getFormat(url, context, nextGetFormat) {
  if (url.startsWith("file:") && (url.endsWith(".ts") || url.endsWith(".tsx"))) {
    return { format: "module", shortCircuit: true };
  }

  return nextGetFormat(url, context);
}

export async function load(url, context, nextLoad) {
  if (url.startsWith("file:") && (url.endsWith(".ts") || url.endsWith(".tsx"))) {
    const source = await readFile(new URL(url));
    const result = spawnSync("bun", [fileURLToPath(BUN_TRANSPILER)], {
      input: source,
      encoding: "utf8",
    });

    if (result.status !== 0) {
      const message = [result.stderr, result.error].filter(Boolean).join("\n");
      throw new Error(`bun transpiler failed for ${url}: ${message}`);
    }

    const injectImportMetaDir = result.stdout.includes("import.meta.dir")
      ? 'const __nodeImportMetaDir = decodeURIComponent(new URL(".", import.meta.url).pathname);\n'
      : "";
    const withImportMetaDir = result.stdout.includes("import.meta.dir")
      ? result.stdout.replaceAll("import.meta.dir", "__nodeImportMetaDir")
      : result.stdout;
    const requireSource = withImportMetaDir.replaceAll(/\brequire\s*\(/g, "__nodeRequire(");

    return {
      format: "module",
      shortCircuit: true,
      source: withImportMetaDir.includes("require(")
        ? `import { createRequire } from "node:module";
const __nodeRequire = globalThis.require ?? createRequire(import.meta.url);
${injectImportMetaDir}${requireSource}`
        : `${injectImportMetaDir}${withImportMetaDir}`,
    };
  }

  return nextLoad(url, context);
}
