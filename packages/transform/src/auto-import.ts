// Desugared output references bare `succeed` / `sync` identifiers. When the
// module doesn't already bind them from @spilne/perfect-core, prepend an import so
// the transformed file compiles without the user knowing about the
// implementation detail.
//
// Heuristic, not a scope analysis: a local `const succeed = …` shadowing the
// name would defeat it — in that case import the helpers explicitly.

const HELPERS = ["succeed", "sync"] as const;

function importedLocalNames(source: string): Set<string> {
  const names = new Set<string>();
  const importRe = /import\s*(?:type\s+)?\{([^}]*)\}\s*from\s*["']@spilne\/perfect-core["']/g;
  let m: RegExpExecArray | null;
  while ((m = importRe.exec(source)) !== null) {
    for (const entry of m[1]!.split(",")) {
      const parts = entry.trim().split(/\s+as\s+/);
      const local = (parts[1] ?? parts[0])?.trim();
      if (local) names.add(local);
    }
  }
  return names;
}

export function ensureCoreImports(original: string, transformed: string): string {
  if (transformed === original) return transformed;
  const bound = importedLocalNames(original);
  const missing = HELPERS.filter(
    (name) => !bound.has(name) && new RegExp(`\\b${name}\\(`).test(transformed),
  );
  if (missing.length === 0) return transformed;
  return `import { ${missing.join(", ")} } from "@spilne/perfect-core";\n${transformed}`;
}
