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

// ── Main entry ─────────────────────────────────────────────────────

export function rewriteEffBlocks(source: string): string {
  let result = source;
  result = rewriteForComprehensions(result);
  result = rewriteDollarBlocks(result);
  return result;
}

// ── For-comprehension: for { x <- e; if p } yield expr ─────────────

type ForStmt =
  | { kind: "bind"; varName: string; expr: string }
  | { kind: "bind_discard"; expr: string }
  | { kind: "let"; varName: string; expr: string };

function rewriteForComprehensions(source: string): string {
  let result = source;
  let offset = 0;

  // match: for { ... } yield expr
  // but NOT: for ( — that's a JS for-loop
  const pattern = /\bfor\s*\{/g;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(source)) !== null) {
    // check it's not inside a string or comment
    const before = source.slice(Math.max(0, match.index - 1), match.index);
    if (before === "." || before === "_") continue; // method call like x.for or _for

    const braceStart = match.index + match[0].length - 1;
    const braceEnd = findMatchingBrace(source, braceStart);
    if (braceEnd === -1) continue;

    // look for `yield` after the closing brace
    const afterBrace = source.slice(braceEnd + 1).match(/^\s*yield\s+/);
    if (!afterBrace) continue; // not our for-comprehension

    const yieldStart = braceEnd + 1 + afterBrace[0].length;
    // find the yield expression — goes until newline, semicolon, or closing paren/brace at depth 0
    const yieldExpr = extractExpression(source, yieldStart);
    if (!yieldExpr) continue;

    const fullEnd = yieldStart + yieldExpr.length;

    // parse the body between { }
    const body = source.slice(braceStart + 1, braceEnd);
    const stmts = parseForStatements(body);
    if (stmts.length === 0) continue;

    const desugared = desugarForStatements(stmts, yieldExpr.trim());
    if (!desugared) continue;

    result = result.slice(0, match.index + offset) + desugared + result.slice(fullEnd + offset);

    offset += desugared.length - (fullEnd - match.index);
  }

  return result;
}

function parseForStatements(body: string): ForStmt[] {
  const stmts: ForStmt[] = [];
  const lines = splitStatements(body.trim());

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // x <- expr  (bind)
    const bindMatch = trimmed.match(/^(\w+)\s*<-\s*(.+)$/);
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

    // if <cond>  is no longer supported — guards are inherently type-specific.
    // Silently skip so we don't silently mangle user code; the rewrite will abort below
    // if no bind statements are parsed.
    if (/^if\s+/.test(trimmed)) {
      throw new Error(
        `for-comprehension guards (if) are not supported: "${trimmed}".\n` +
          `Use .filter() on the monad directly, or switch to eff(($) => {}) for Eff-specific flow control.`,
      );
    }

    // val x = expr  (let binding — pure, not effectful)
    const letMatch = trimmed.match(/^(?:val|let|const)\s+(\w+)\s*=\s*(.+)$/);
    if (letMatch) {
      stmts.push({ kind: "let", varName: letMatch[1]!, expr: letMatch[2]!.trim() });
      continue;
    }
  }

  return stmts;
}

function desugarForStatements(stmts: ForStmt[], yieldExpr: string): string | null {
  if (stmts.length === 0) return null;

  // Find the last effectful step; `let` is pure JS and doesn't count.
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

function extractExpression(source: string, start: number): string | null {
  let i = start;
  let depth = 0;

  while (i < source.length) {
    const ch = source[i]!;

    if (ch === '"' || ch === "'" || ch === "`") {
      i = skipString(source, i);
      continue;
    }

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

function rewriteDollarBlocks(source: string): string {
  let result = source;
  let offset = 0;

  const pattern = /\beff\s*\(\s*\(\s*\$\s*\)\s*=>\s*\{/g;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(source)) !== null) {
    const blockStart = match.index + match[0].length;
    const blockEnd = findMatchingBrace(source, blockStart - 1);
    if (blockEnd === -1) continue;

    const body = source.slice(blockStart, blockEnd);
    const stmts = parseDollarStatements(body);

    if (stmts.length === 0) continue;

    const desugared = desugarDollarStatements(stmts);
    if (!desugared) continue;

    const fullStart = match.index;
    const fullEnd = blockEnd + 2; // })

    result = result.slice(0, fullStart + offset) + desugared + result.slice(fullEnd + offset);

    offset += desugared.length - (fullEnd - fullStart);
  }

  return result;
}

type DollarStmt =
  | { kind: "bind"; varName: string; expr: string }
  | { kind: "bind_discard"; expr: string }
  | { kind: "return"; expr: string }
  | { kind: "raw"; code: string };

function parseDollarStatements(body: string): DollarStmt[] {
  const stmts: DollarStmt[] = [];
  const lines = splitStatements(body.trim());

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const bindMatch = trimmed.match(/^(?:const|let|var)\s+(\w+)\s*=\s*\$\((.+)\)\s*;?\s*$/);
    if (bindMatch) {
      stmts.push({ kind: "bind", varName: bindMatch[1]!, expr: bindMatch[2]!.trim() });
      continue;
    }

    const discardMatch = trimmed.match(/^\$\((.+)\)\s*;?\s*$/);
    if (discardMatch) {
      stmts.push({ kind: "bind_discard", expr: discardMatch[1]!.trim() });
      continue;
    }

    const returnMatch = trimmed.match(/^return\s+(.+?)\s*;?\s*$/);
    if (returnMatch) {
      stmts.push({ kind: "return", expr: returnMatch[1]! });
      continue;
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
        return `${stmt.expr}.flatMap((${stmt.varName}) => ${desugar(index + 1)})`;
      case "bind_discard":
        return isLast ? stmt.expr : `${stmt.expr}.flatMap(() => ${desugar(index + 1)})`;
      case "return": {
        const inner = stmt.expr.match(/^\$\((.+)\)$/);
        return inner ? inner[1]! : `succeed(${stmt.expr})`;
      }
      case "raw":
        return `sync(() => { ${stmt.code} }).flatMap(() => ${desugar(index + 1)})`;
    }
  }

  return desugar(0);
}

// ── Shared utilities ───────────────────────────────────────────────

function findMatchingBrace(source: string, openPos: number): number {
  let depth = 1;
  let i = openPos + 1;
  while (i < source.length && depth > 0) {
    const ch = source[i];
    if (ch === "{") depth++;
    else if (ch === "}") depth--;
    else if (ch === '"' || ch === "'" || ch === "`") {
      i = skipString(source, i);
      continue;
    }
    // skip single-line comments
    else if (ch === "/" && source[i + 1] === "/") {
      while (i < source.length && source[i] !== "\n") i++;
      continue;
    }
    if (depth === 0) return i;
    i++;
  }
  return -1;
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
      i = findMatchingBrace(source, i + 1);
      if (i === -1) return source.length;
      i++;
      continue;
    }
    i++;
  }
  return i;
}

function splitStatements(body: string): string[] {
  const stmts: string[] = [];
  let current = "";
  let depth = 0;
  let i = 0;

  while (i < body.length) {
    const ch = body[i]!;

    if (ch === '"' || ch === "'" || ch === "`") {
      const end = skipString(body, i);
      current += body.slice(i, end);
      i = end;
      continue;
    }

    // skip single-line comments
    if (ch === "/" && body[i + 1] === "/") {
      while (i < body.length && body[i] !== "\n") i++;
      continue;
    }

    if (ch === "(" || ch === "[" || ch === "{") {
      depth++;
      current += ch;
      i++;
      continue;
    }
    if (ch === ")" || ch === "]" || ch === "}") {
      depth--;
      current += ch;
      i++;
      continue;
    }

    if (depth === 0 && (ch === "\n" || ch === ";")) {
      if (current.trim()) stmts.push(current.trim());
      current = "";
      i++;
      continue;
    }

    current += ch;
    i++;
  }

  if (current.trim()) stmts.push(current.trim());
  return stmts;
}
