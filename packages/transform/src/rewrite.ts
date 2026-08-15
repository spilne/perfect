// Spilne for-comprehension desugarer
//
// Supports two syntaxes:
//
// 1. eff($) style — Eff-specific, ergonomic sugar (sync statements, $(e), succeed-wrap on return):
//    eff(($) => { const x = $(e); console.log(x); return x })
//
// 2. Scala for-comprehension style — MONAD-GENERIC. Desugars to plain .flatMap/.map only.
//    Works on anything with those methods: Eff, Array, Promise, Option, Stream, custom monads.
//    for { x <- getX() } yield x * 2    →    getX().map((x) => x * 2)
//    for { a <- e1; b <- e2 } yield a+b →    e1.flatMap((a) => e2.map((b) => a + b))
//
//    Guards (`if cond`) are NOT supported — they'd require type-specific filter/fail. Use
//    .filter() directly on the monad, or switch to eff($) for Eff-specific flow control.
//
// Design notes:
// - All scanning happens against a MASK of the source (string literals and
//   comments blanked to spaces, same length) so code-looking text inside
//   strings is never rewritten. Original text is sliced for output.
// - Regions are rewritten one at a time with a full restart between splices —
//   no offset bookkeeping. Nested regions are handled by recursively
//   rewriting a region's body text before parsing it.
// - Anything involving `$` that the rewriter cannot compile THROWS a
//   RewriteError instead of silently emitting code with a dangling `$`.

export class RewriteError extends Error {
  constructor(message: string, statement?: string) {
    super(statement ? `${message}\n  in statement: ${statement}` : message);
    this.name = "RewriteError";
  }
}

const MAX_PASSES = 200;

// ── Main entry ─────────────────────────────────────────────────────

export function rewriteEffBlocks(source: string): string {
  let result = source;
  for (let pass = 0; pass < MAX_PASSES; pass++) {
    const afterFor = rewriteOneForComprehension(result);
    if (afterFor !== null) {
      result = afterFor;
      continue;
    }
    const afterDollar = rewriteOneDollarBlock(result);
    if (afterDollar !== null) {
      result = afterDollar;
      continue;
    }
    return result;
  }
  throw new RewriteError("rewrite did not converge — nesting too deep or internal bug");
}

// ── Masking ────────────────────────────────────────────────────────
//
// Same-length copy of the source with string-literal and comment contents
// replaced by spaces. Template-literal interpolations are blanked too —
// a comprehension inside `${…}` is not supported (documented limitation).
// Regex literals are not recognized (the `/` division ambiguity needs a
// parser); a `for {` inside a regex would be misdetected — vanishingly rare.

function maskCode(source: string): string {
  const out = source.split("");
  let i = 0;
  while (i < source.length) {
    const ch = source[i]!;
    if (ch === "/" && source[i + 1] === "/") {
      while (i < source.length && source[i] !== "\n") {
        out[i] = " ";
        i++;
      }
      continue;
    }
    if (ch === "/" && source[i + 1] === "*") {
      out[i] = " ";
      out[i + 1] = " ";
      i += 2;
      while (i < source.length && !(source[i] === "*" && source[i + 1] === "/")) {
        out[i] = " ";
        i++;
      }
      if (i < source.length) {
        out[i] = " ";
        out[i + 1] = " ";
        i += 2;
      }
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") {
      const end = skipString(source, i);
      for (let j = i; j < end && j < source.length; j++) out[j] = " ";
      i = end;
      continue;
    }
    i++;
  }
  return out.join("");
}

function skipString(source: string, start: number): number {
  const quote = source[start];
  let i = start + 1;
  while (i < source.length) {
    if (source[i] === "\\") {
      i += 2;
      continue;
    }
    if (source[i] === quote) return i + 1;
    if (quote === "`" && source[i] === "$" && source[i + 1] === "{") {
      i = findMatchingBraceRaw(source, i + 1);
      if (i === -1) return source.length;
      i++;
      continue;
    }
    i++;
  }
  return i;
}

// Brace matching over raw text (used only inside template interpolations).
function findMatchingBraceRaw(source: string, openPos: number): number {
  let depth = 1;
  let i = openPos + 1;
  while (i < source.length) {
    const ch = source[i];
    if (ch === '"' || ch === "'" || ch === "`") {
      i = skipString(source, i);
      continue;
    }
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) return i;
    }
    i++;
  }
  return -1;
}

// Brace matching over the MASK — strings/comments are already blanked.
function findMatchingBrace(mask: string, openPos: number): number {
  let depth = 1;
  let i = openPos + 1;
  while (i < mask.length) {
    const ch = mask[i];
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) return i;
    }
    i++;
  }
  return -1;
}

function lineOf(source: string, index: number): number {
  return source.slice(0, index).split("\n").length;
}

// ── For-comprehension: for { x <- e } yield expr ───────────────────

type ForStmt =
  | { kind: "bind"; varName: string; expr: string }
  | { kind: "bind_discard"; expr: string }
  | { kind: "let"; varName: string; expr: string };

// Rewrite the FIRST for-comprehension region found; null if none.
function rewriteOneForComprehension(source: string): string | null {
  const mask = maskCode(source);
  const pattern = /\bfor\s*\{/g;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(mask)) !== null) {
    const before = mask.slice(Math.max(0, match.index - 1), match.index);
    if (before === "." || before === "_") continue; // method/property like x.for

    const braceStart = match.index + match[0].length - 1;
    const braceEnd = findMatchingBrace(mask, braceStart);
    if (braceEnd === -1) continue;

    const afterBrace = mask.slice(braceEnd + 1).match(/^\s*yield\s+/);
    if (!afterBrace) continue; // not our for-comprehension

    const yieldStart = braceEnd + 1 + afterBrace[0].length;
    const yieldExpr = extractExpression(source, mask, yieldStart);
    if (!yieldExpr) continue;

    const fullEnd = yieldStart + yieldExpr.length;

    // nested regions inside the body desugar first
    const body = rewriteEffBlocks(source.slice(braceStart + 1, braceEnd));
    const stmts = parseForStatements(body, lineOf(source, match.index));
    if (stmts.length === 0) continue;

    const desugared = desugarForStatements(stmts, rewriteEffBlocks(yieldExpr.trim()));
    if (!desugared) continue;

    return source.slice(0, match.index) + desugared + source.slice(fullEnd);
  }

  return null;
}

function parseForStatements(body: string, blockLine: number): ForStmt[] {
  const stmts: ForStmt[] = [];
  const lines = splitStatements(body.trim());

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // x <- expr  (bind; multi-line RHS allowed)
    const bindMatch = trimmed.match(/^(\w+)\s*<-\s*([\s\S]+)$/);
    if (bindMatch) {
      const varName = bindMatch[1]!;
      const expr = bindMatch[2]!.trim();
      if (varName === "_") {
        stmts.push({ kind: "bind_discard", expr });
      } else {
        stmts.push({ kind: "bind", varName, expr });
      }
      continue;
    }

    // guards are inherently type-specific — refuse loudly
    if (/^if\s+/.test(trimmed)) {
      throw new RewriteError(
        `for-comprehension guards (if) are not supported (line ~${blockLine}): "${trimmed}".\n` +
          `Use .filter() on the monad directly, or switch to eff(($) => {}) for Eff-specific flow control.`,
      );
    }

    // val x = expr  (pure let binding)
    const letMatch = trimmed.match(/^(?:val|let|const)\s+(\w+)\s*=\s*([\s\S]+)$/);
    if (letMatch) {
      stmts.push({ kind: "let", varName: letMatch[1]!, expr: letMatch[2]!.trim() });
      continue;
    }

    // a line mentioning <- that didn't parse is a bind we can't compile —
    // dropping it would corrupt the program
    if (maskCode(trimmed).includes("<-")) {
      throw new RewriteError(
        `unsupported bind pattern in for-comprehension (line ~${blockLine}) — only "name <- expr" binds are supported`,
        trimmed,
      );
    }

    throw new RewriteError(
      `unsupported statement in for-comprehension (line ~${blockLine}) — expected "x <- e", "val x = e", or nothing`,
      trimmed,
    );
  }

  return stmts;
}

function desugarForStatements(stmts: ForStmt[], yieldExpr: string): string | null {
  if (stmts.length === 0) return null;

  let lastEffectfulIndex = -1;
  for (let i = stmts.length - 1; i >= 0; i--) {
    const k = stmts[i]!.kind;
    if (k === "bind" || k === "bind_discard") {
      lastEffectfulIndex = i;
      break;
    }
  }
  if (lastEffectfulIndex === -1) return null;

  // After the last effectful bind: only pure lets + yield expr.
  function desugarTail(index: number): string {
    if (index >= stmts.length) return yieldExpr;
    const stmt = stmts[index]!;
    if (stmt.kind === "let") {
      return `((${stmt.varName}) => ${desugarTail(index + 1)})(${stmt.expr})`;
    }
    return yieldExpr;
  }

  function desugar(index: number): string {
    const stmt = stmts[index]!;

    switch (stmt.kind) {
      case "bind": {
        if (index === lastEffectfulIndex) {
          return `(${stmt.expr}).map((${stmt.varName}) => ${desugarTail(index + 1)})`;
        }
        return `(${stmt.expr}).flatMap((${stmt.varName}) => ${desugar(index + 1)})`;
      }
      case "bind_discard": {
        if (index === lastEffectfulIndex) {
          return `(${stmt.expr}).map(() => ${desugarTail(index + 1)})`;
        }
        return `(${stmt.expr}).flatMap(() => ${desugar(index + 1)})`;
      }
      case "let": {
        return `((${stmt.varName}) => ${desugar(index + 1)})(${stmt.expr})`;
      }
    }
  }

  return desugar(0);
}

// Expression boundary scan runs on the mask; text is sliced from the source.
function extractExpression(source: string, mask: string, start: number): string | null {
  let i = start;
  let depth = 0;

  while (i < mask.length) {
    const ch = mask[i]!;

    if (ch === "(" || ch === "[" || ch === "{") {
      depth++;
      i++;
      continue;
    }
    if (ch === ")" || ch === "]" || ch === "}") {
      if (depth === 0) break;
      depth--;
      i++;
      continue;
    }

    if (depth === 0 && (ch === "\n" || ch === ";")) break;

    i++;
  }

  const expr = source.slice(start, i).trim();
  return expr || null;
}

// ── Dollar-bind: eff(($) => { const x = $(e); return x }) ─────────

type DollarStmt =
  | { kind: "bind"; pattern: string; expr: string }
  | { kind: "let"; pattern: string; expr: string }
  | { kind: "bind_discard"; expr: string }
  | { kind: "return"; expr: string }
  | { kind: "raw"; code: string };

// Rewrite the FIRST eff(($) => { … }) region found; null if none.
function rewriteOneDollarBlock(source: string): string | null {
  const mask = maskCode(source);
  const pattern = /\beff\s*\(\s*\(\s*\$\s*\)\s*=>\s*\{/g;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(mask)) !== null) {
    const blockStart = match.index + match[0].length;
    const blockEnd = findMatchingBrace(mask, blockStart - 1);
    if (blockEnd === -1) continue;

    // the eff(...) call must close right after the body — skip whitespace
    // to the `)` instead of assuming `})` adjacency
    let closeParen = blockEnd + 1;
    while (closeParen < mask.length && /\s/.test(mask[closeParen]!)) closeParen++;
    if (mask[closeParen] !== ")") {
      throw new RewriteError(
        `expected ")" closing eff(($) => { … }) near line ${lineOf(source, blockEnd)} — ` +
          `eff takes exactly one arrow argument`,
      );
    }
    const fullEnd = closeParen + 1;

    // nested regions inside the body desugar first
    const body = rewriteEffBlocks(source.slice(blockStart, blockEnd));
    const stmts = parseDollarStatements(body, lineOf(source, match.index));
    if (stmts.length === 0) continue;

    const desugared = desugarDollarStatements(stmts);
    if (!desugared) continue;

    return source.slice(0, match.index) + desugared + source.slice(fullEnd);
  }

  return null;
}

function isBalanced(fragment: string): boolean {
  const mask = maskCode(fragment);
  let depth = 0;
  for (const ch of mask) {
    if (ch === "(" || ch === "[" || ch === "{") depth++;
    else if (ch === ")" || ch === "]" || ch === "}") {
      depth--;
      if (depth < 0) return false;
    }
  }
  return depth === 0;
}

// Standalone `$` outside identifiers (template interpolations are masked out).
function mentionsDollar(fragment: string): boolean {
  return /(^|[^\w$])\$($|[^\w${])/.test(maskCode(fragment));
}

function parseDollarStatements(body: string, blockLine: number): DollarStmt[] {
  const stmts: DollarStmt[] = [];
  const lines = splitStatements(body.trim());

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // const/let/var <pattern> = $(expr) — pattern may be an identifier,
    // a destructuring pattern, or carry a type annotation; multi-line ok
    const bindMatch = trimmed.match(
      /^(?:const|let|var)\s+([\s\S]+?)\s*=\s*\$\(([\s\S]+)\)\s*;?\s*$/,
    );
    if (bindMatch && isBalanced(bindMatch[2]!) && !mentionsDollar(bindMatch[2]!)) {
      stmts.push({ kind: "bind", pattern: bindMatch[1]!.trim(), expr: bindMatch[2]!.trim() });
      continue;
    }

    // bare $(expr) — discard the value
    const discardMatch = trimmed.match(/^\$\(([\s\S]+)\)\s*;?\s*$/);
    if (discardMatch && isBalanced(discardMatch[1]!) && !mentionsDollar(discardMatch[1]!)) {
      stmts.push({ kind: "bind_discard", expr: discardMatch[1]!.trim() });
      continue;
    }

    // bare `return` / `return;`
    if (/^return\s*;?\s*$/.test(trimmed)) {
      stmts.push({ kind: "return", expr: "undefined" });
      continue;
    }

    const returnMatch = trimmed.match(/^return\s+([\s\S]+?)\s*;?\s*$/);
    if (returnMatch) {
      const expr = returnMatch[1]!;
      const inner = expr.match(/^\$\(([\s\S]+)\)$/);
      if (inner && isBalanced(inner[1]!) && !mentionsDollar(inner[1]!)) {
        stmts.push({ kind: "return", expr });
        continue;
      }
      if (mentionsDollar(expr)) {
        throw new RewriteError(
          `unsupported $ usage in return (line ~${blockLine}) — only "return $(expr)" or plain "return expr" are compiled. ` +
            `Bind first: const x = $(expr); return f(x)`,
          trimmed,
        );
      }
      stmts.push({ kind: "return", expr });
      continue;
    }

    // pure declaration (no $) — must NOT be sync-wrapped as a statement:
    // the binding would be scoped inside the sync arrow, invisible to the
    // rest of the block. Compile to sync(() => expr).flatMap((name) => rest).
    const letMatch = trimmed.match(/^(?:const|let|var)\s+([\s\S]+?)\s*=\s*([\s\S]+?)\s*;?\s*$/);
    if (letMatch && !mentionsDollar(letMatch[2]!)) {
      const declMask = maskCode(letMatch[2]!);
      let depth = 0;
      let topComma = false;
      for (const ch of declMask) {
        if (ch === "(" || ch === "[" || ch === "{") depth++;
        else if (ch === ")" || ch === "]" || ch === "}") depth--;
        else if (ch === "," && depth === 0) topComma = true;
      }
      if (topComma) {
        throw new RewriteError(
          `multi-declarator statements are not supported inside eff(($) => …) (line ~${blockLine}) — split into separate const declarations`,
          trimmed,
        );
      }
      stmts.push({ kind: "let", pattern: letMatch[1]!.trim(), expr: letMatch[2]!.trim() });
      continue;
    }

    // any other statement mentioning $ would be silently miscompiled —
    // refuse with guidance instead
    if (mentionsDollar(trimmed)) {
      throw new RewriteError(
        `unsupported $ usage in eff(($) => …) block (line ~${blockLine}) — $ is only compiled in ` +
          `"const x = $(expr)", bare "$(expr)", and "return $(expr)". ` +
          `For $ inside expressions or control flow, bind first (const x = $(e)) or use the ` +
          `SWC plugin (@perfect/swc-plugin), which supports if/else.`,
        trimmed,
      );
    }

    stmts.push({ kind: "raw", code: trimmed });
  }

  return stmts;
}

function desugarDollarStatements(stmts: DollarStmt[]): string | null {
  if (stmts.length === 0) return null;

  function desugar(index: number): string {
    if (index >= stmts.length) return "succeed(undefined)";
    const stmt = stmts[index]!;
    const isLast = index === stmts.length - 1;

    switch (stmt.kind) {
      case "bind":
        return `${stmt.expr}.flatMap((${stmt.pattern}) => ${desugar(index + 1)})`;
      case "let":
        return `sync(() => (${stmt.expr})).flatMap((${stmt.pattern}) => ${desugar(index + 1)})`;
      case "bind_discard":
        return isLast ? stmt.expr : `${stmt.expr}.flatMap(() => ${desugar(index + 1)})`;
      case "return": {
        const inner = stmt.expr.match(/^\$\(([\s\S]+)\)$/);
        return inner ? inner[1]! : `succeed(${stmt.expr})`;
      }
      case "raw":
        return `sync(() => { ${stmt.code} }).flatMap(() => ${desugar(index + 1)})`;
    }
  }

  return desugar(0);
}

// ── Statement splitting (mask-aware) ───────────────────────────────

// Line comments must not survive into statements — desugared output is
// single-line, so a trailing `// …` would swallow the generated code.
function stripLineComments(fragment: string): string {
  let out = "";
  let i = 0;
  while (i < fragment.length) {
    const ch = fragment[i]!;
    if (ch === '"' || ch === "'" || ch === "`") {
      const end = skipString(fragment, i);
      out += fragment.slice(i, end);
      i = end;
      continue;
    }
    if (ch === "/" && fragment[i + 1] === "/") {
      while (i < fragment.length && fragment[i] !== "\n") i++;
      continue;
    }
    out += ch;
    i++;
  }
  return out;
}

function splitStatements(body: string): string[] {
  const mask = maskCode(body);
  const stmts: string[] = [];
  let start = 0;
  let depth = 0;

  for (let i = 0; i < mask.length; i++) {
    const ch = mask[i]!;
    if (ch === "(" || ch === "[" || ch === "{") depth++;
    else if (ch === ")" || ch === "]" || ch === "}") depth--;
    else if (depth === 0 && (ch === "\n" || ch === ";")) {
      const stmt = stripLineComments(body.slice(start, i)).trim();
      if (stmt) stmts.push(stmt);
      start = i + 1;
    }
  }
  const last = stripLineComments(body.slice(start)).trim();
  if (last) stmts.push(last);
  return stmts;
}
