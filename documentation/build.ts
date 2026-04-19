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
// Run:
//   bun documentation/build.ts          # rewrite all .md in documentation/
//   bun documentation/build.ts --check  # exit 1 if anything would change
//
// The check mode is what CI runs — if a contributor edited an example but
// forgot to rerun the embed, the build fails.

import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";

const ROOT = join(import.meta.dir, "..");
const DOC_DIR = join(ROOT, "documentation");
const CHECK_MODE = process.argv.includes("--check");

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

function extractRegion(file: string, region: string): string {
  const absPath = join(ROOT, file);
  const src = readFileSync(absPath, "utf8");
  const match = src.match(regionRe(region));
  if (!match) {
    throw new Error(`region '${region}' not found in ${file}`);
  }
  // Trim leading/trailing blank lines, normalize indent.
  const body = match[1]!.replace(/^\n+|\n+$/g, "");
  const lines = body.split("\n");
  // Compute common leading whitespace and strip it.
  const indents = lines
    .filter((l) => l.trim().length > 0)
    .map((l) => l.match(/^[ \t]*/)![0].length);
  const minIndent = indents.length === 0 ? 0 : Math.min(...indents);
  return lines.map((l) => l.slice(minIndent)).join("\n");
}

let changed = 0;
let errors = 0;

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
  console.error(
    `\n${changed} file(s) out of sync. Run \`bun documentation/build.ts\` and commit.`,
  );
  process.exit(1);
}

console.log(`\n${changed} file(s) ${CHECK_MODE ? "would be " : ""}updated.`);
