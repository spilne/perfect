// Embed-extractor for the documentation/ folder.
//
// Markdown files reference TS regions like:
//
//   <!-- @embed packages/core/examples/01-hello.ts#region-name -->
//   ```ts
//   (anything in here gets overwritten on every build)
//   ```
//   <!-- @end -->
//
// Regions are marked in TS files with comment delimiters:
//
//   // >>> example: region-name
//   const program = succeed(42);
//   // <<< example
//
// What gets rendered:
//   1. The imports that the snippet body actually uses, rewritten so
//      `from "../src"` becomes `from "@spilne/perfect-core"` and the internal
//      `_assert` helper is skipped (it's a test utility, not user-facing).
//   2. The snippet body itself, with `assertEq(actual, expected)` rewritten to
//      `console.log(actual); // → expected` so readers can copy-paste-and-run.
//
// The source TS files stay self-verifying via assertEq (test/examples.test.ts
// imports them all and any wrong assertion throws), but the docs show the
// runnable form a user would actually write.
//
// Run:
//   bun documentation/build.ts          # rewrite all .md in documentation/
//   bun documentation/build.ts --check  # exit 1 if anything would change

import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";

const ROOT = join(import.meta.dir, "..");
const DOC_DIR = join(ROOT, "documentation");
const CHECK_MODE = process.argv.includes("--check");

const INTERNAL_IMPORT_RE = /^\.\.\/src(\/.*)?$/;

// The package an embedded example belongs to — internal `../src` imports
// rewrite to it (packages/http/examples → @spilne/perfect-http, etc.).
function packageNameFor(file: string): string {
  const m = file.match(/packages\/([\w-]+)\//);
  return `@spilne/perfect-${m ? m[1] : "core"}`;
}
const SKIP_IMPORT_SOURCES = new Set(["./_assert", "../_assert"]);

const EMBED_RE =
  /<!-- @embed (?<file>[^#\s]+)#(?<region>[^\s]+) -->\n```[a-z]*\n[\s\S]*?\n```\n<!-- @end -->/g;

function regionRe(name: string): RegExp {
  // Match `// >>> example: name` ... `// <<< example`
  // Captures everything between; trims surrounding blank lines later.
  return new RegExp(
    `^[ \\t]*//[ \\t]*>>>[ \\t]*example:[ \\t]*${name}[ \\t]*$\\n([\\s\\S]*?)^[ \\t]*//[ \\t]*<<<[ \\t]*example[ \\t]*$`,
    "m",
  );
}

function* walkMarkdown(dir: string): Generator<string> {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) {
      yield* walkMarkdown(path);
    } else if (entry.endsWith(".md")) {
      yield path;
    }
  }
}

interface ParsedImport {
  /** Raw import source (e.g. `"../src"`). */
  source: string;
  /** `default` binding name, if any (`import X from "..."`). */
  defaultName?: string;
  /** Namespace binding name, if any (`import * as X from "..."`). */
  namespaceName?: string;
  /** Named bindings — preserves `type` modifier per item. */
  named: Array<{ name: string; alias?: string; isType: boolean }>;
}

function parseImports(src: string): ParsedImport[] {
  // Captures consecutive top-of-file imports. Supports single- and multi-line
  // forms. Stops at the first non-import, non-blank, non-comment line.
  const out: ParsedImport[] = [];
  const lines = src.split("\n");
  let i = 0;
  let buf = "";

  const flush = () => {
    if (!buf.trim()) {
      buf = "";
      return;
    }
    const parsed = parseSingleImport(buf);
    if (parsed) out.push(parsed);
    buf = "";
  };

  while (i < lines.length) {
    const line = lines[i]!;
    const trimmed = line.trim();
    if (!buf && (trimmed === "" || trimmed.startsWith("//"))) {
      i++;
      continue;
    }
    if (!buf && !trimmed.startsWith("import")) break;
    buf += (buf ? "\n" : "") + line;
    if (buf.includes(";")) {
      flush();
    }
    i++;
  }
  flush();
  return out;
}

function parseSingleImport(stmt: string): ParsedImport | null {
  const sourceMatch = stmt.match(/from\s+["']([^"']+)["']/);
  if (!sourceMatch) return null;
  const source = sourceMatch[1]!;
  const bindings = stmt
    .replace(/^\s*import\s+/, "")
    .replace(/\s*from\s+["'][^"']+["']\s*;?\s*$/, "");

  const out: ParsedImport = { source, named: [] };
  let rest = bindings;

  const namespaceMatch = rest.match(/\*\s+as\s+(\w+)/);
  if (namespaceMatch) {
    out.namespaceName = namespaceMatch[1];
    rest = rest.replace(namespaceMatch[0], "");
  }

  const braceMatch = rest.match(/\{([\s\S]*)\}/);
  if (braceMatch) {
    const inner = braceMatch[1]!;
    for (const part of inner.split(",")) {
      const p = part.trim();
      if (!p) continue;
      const m = p.match(/^(type\s+)?(\w+)(?:\s+as\s+(\w+))?$/);
      if (!m) continue;
      out.named.push({ name: m[2]!, alias: m[3], isType: !!m[1] });
    }
    rest = rest.replace(braceMatch[0], "");
  }

  const defaultMatch = rest.match(/^\s*(\w+)\s*,?/);
  if (defaultMatch && defaultMatch[1]) {
    out.defaultName = defaultMatch[1];
  }

  return out;
}

function identifiersIn(code: string): Set<string> {
  // Strip strings and block/line comments, then collect identifier-like tokens.
  // Skip property/method accesses (preceded by `.`) so e.g. `x.runSync()`
  // doesn't mark `runSync` as a used import.
  const stripped = code
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/[^\n]*/g, "")
    .replace(/(["'`])(?:\\.|(?!\1)[\s\S])*?\1/g, "");
  const set = new Set<string>();
  const idRe = /(?<!\.)\b[A-Za-z_$][A-Za-z0-9_$]*\b/g;
  let m: RegExpExecArray | null;
  while ((m = idRe.exec(stripped)) !== null) set.add(m[0]);
  return set;
}

function renderImports(imports: ParsedImport[], body: string, packageName: string): string {
  const used = identifiersIn(body);
  const lines: string[] = [];

  for (const imp of imports) {
    if (SKIP_IMPORT_SOURCES.has(imp.source)) continue;

    const source = INTERNAL_IMPORT_RE.test(imp.source)
      ? packageName + (imp.source.replace(INTERNAL_IMPORT_RE, "$1") || "")
      : imp.source;

    const namedKept = imp.named.filter((n) => used.has(n.alias ?? n.name));
    const defaultKept = imp.defaultName && used.has(imp.defaultName) ? imp.defaultName : undefined;
    const namespaceKept =
      imp.namespaceName && used.has(imp.namespaceName) ? imp.namespaceName : undefined;

    if (!namedKept.length && !defaultKept && !namespaceKept) continue;

    const parts: string[] = [];
    if (defaultKept) parts.push(defaultKept);
    if (namespaceKept) parts.push(`* as ${namespaceKept}`);
    if (namedKept.length) {
      const inner = namedKept
        .map((n) => `${n.isType ? "type " : ""}${n.name}${n.alias ? ` as ${n.alias}` : ""}`)
        .join(", ");
      parts.push(`{ ${inner} }`);
    }
    lines.push(`import ${parts.join(", ")} from "${source}";`);
  }
  return lines.join("\n");
}

/**
 * Split `assertEq(actual, expected)` at the top-level comma.
 * Returns `[actual, expected]` if found, else `null`.
 * Walks paren/bracket/brace depth and skips quoted strings.
 */
function splitArgs(argsSource: string): [string, string] | null {
  let depth = 0;
  let inString: string | null = null;
  for (let i = 0; i < argsSource.length; i++) {
    const ch = argsSource[i]!;
    if (inString) {
      if (ch === "\\") {
        i++;
        continue;
      }
      if (ch === inString) inString = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") {
      inString = ch;
      continue;
    }
    if (ch === "(" || ch === "[" || ch === "{") depth++;
    else if (ch === ")" || ch === "]" || ch === "}") depth--;
    else if (ch === "," && depth === 0) {
      return [argsSource.slice(0, i).trim(), argsSource.slice(i + 1).trim()];
    }
  }
  return null;
}

function rewriteAssertEq(code: string): string {
  const out: string[] = [];
  for (const line of code.split("\n")) {
    const open = line.match(/^(\s*)assertEq\(/);
    if (!open) {
      out.push(line);
      continue;
    }
    const indent = open[1]!;
    const start = open[0].length;
    // Walk to the matching close paren so we can keep any trailing comment.
    let depth = 1;
    let inString: string | null = null;
    let i = start;
    for (; i < line.length && depth > 0; i++) {
      const ch = line[i]!;
      if (inString) {
        if (ch === "\\") {
          i++;
          continue;
        }
        if (ch === inString) inString = null;
        continue;
      }
      if (ch === '"' || ch === "'" || ch === "`") inString = ch;
      else if (ch === "(") depth++;
      else if (ch === ")") depth--;
    }
    if (depth !== 0) {
      out.push(line);
      continue;
    }
    const inner = line.slice(start, i - 1);
    const trailing = line.slice(i);
    const trailingComment = trailing.match(/\/\/.*$/)?.[0] ?? "";
    const split = splitArgs(inner);
    if (!split) {
      out.push(line);
      continue;
    }
    const [actual, expected] = split;
    // Trim a trailing third arg (assertion message) — assertEq's optional msg.
    const expectedClean = (() => {
      const s = splitArgs(expected);
      return s ? s[0] : expected;
    })();
    if (trailingComment) out.push(`${indent}${trailingComment}`);
    out.push(`${indent}console.log(${actual}); // → ${expectedClean}`);
  }
  return out.join("\n");
}

function extractRegion(file: string, region: string): string {
  const absPath = join(ROOT, file);
  const src = readFileSync(absPath, "utf8");
  const match = src.match(regionRe(region));
  if (!match) {
    throw new Error(`region '${region}' not found in ${file}`);
  }
  // Trim leading/trailing blank lines, normalize indent.
  const rawBody = match[1]!.replace(/^\n+|\n+$/g, "");
  const lines = rawBody.split("\n");
  const indents = lines
    .filter((l) => l.trim().length > 0)
    .map((l) => l.match(/^[ \t]*/)![0].length);
  const minIndent = indents.length === 0 ? 0 : Math.min(...indents);
  const body = lines.map((l) => l.slice(minIndent)).join("\n");

  const imports = parseImports(src);
  const importBlock = renderImports(imports, body, packageNameFor(file));
  const renderedBody = rewriteAssertEq(body);

  return importBlock ? `${importBlock}\n\n${renderedBody}` : renderedBody;
}

let changed = 0;
let errors = 0;

function verifyPackageCoverage(): void {
  const packagesDir = join(ROOT, "packages");
  const coverageFile = join(DOC_DIR, "19-packages.md");
  const coverage = readFileSync(coverageFile, "utf8");
  const start = coverage.indexOf("<!-- package-coverage:start -->");
  const end = coverage.indexOf("<!-- package-coverage:end -->");

  if (start === -1 || end === -1 || end <= start) {
    errors++;
    console.error("✗ documentation/19-packages.md: package coverage markers are missing");
    return;
  }

  const documented = new Set(
    [...coverage.slice(start, end).matchAll(/`(@spilne\/perfect-[\w-]+)`/g)].map(
      (match) => match[1]!,
    ),
  );
  const discovered = new Set<string>();

  for (const entry of readdirSync(packagesDir)) {
    const dir = join(packagesDir, entry);
    const manifest = join(dir, "package.json");
    if (!statSync(dir).isDirectory() || !existsSync(manifest)) continue;

    const parsed = JSON.parse(readFileSync(manifest, "utf8")) as { name?: string };
    if (!parsed.name) continue;
    discovered.add(parsed.name);

    if (!existsSync(join(dir, "README.md"))) {
      errors++;
      console.error(`✗ packages/${entry}: ${parsed.name} has no README.md`);
    }
    if (!documented.has(parsed.name)) {
      errors++;
      console.error(`✗ documentation/19-packages.md: ${parsed.name} is not listed`);
    }
  }

  for (const packageName of documented) {
    if (!discovered.has(packageName)) {
      errors++;
      console.error(`✗ documentation/19-packages.md: ${packageName} is not a workspace package`);
    }
  }
}

verifyPackageCoverage();

for (const md of walkMarkdown(DOC_DIR)) {
  const original = readFileSync(md, "utf8");
  const rewritten = original.replace(EMBED_RE, (_match, ...args) => {
    const groups = args[args.length - 1] as { file: string; region: string };
    try {
      const code = extractRegion(groups.file, groups.region);
      return `<!-- @embed ${groups.file}#${groups.region} -->\n\`\`\`ts\n${code}\n\`\`\`\n<!-- @end -->`;
    } catch (e) {
      errors++;
      console.error(`✗ ${relative(ROOT, md)}: ${(e as Error).message}`);
      return _match;
    }
  });

  if (rewritten !== original) {
    if (CHECK_MODE) {
      console.error(`✗ ${relative(ROOT, md)} is out of sync`);
      changed++;
    } else {
      writeFileSync(md, rewritten);
      console.log(`✓ ${relative(ROOT, md)}`);
      changed++;
    }
  }
}

if (errors > 0) {
  console.error(`\n${errors} error(s)`);
  process.exit(1);
}

if (CHECK_MODE && changed > 0) {
  console.error(`\n${changed} file(s) out of sync. Run \`bun documentation/build.ts\` and commit.`);
  process.exit(1);
}

console.log(`\n${changed} file(s) ${CHECK_MODE ? "would be " : ""}updated.`);
