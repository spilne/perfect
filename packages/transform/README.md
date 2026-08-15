# @perfect/transform

Source-text for-comprehension compiler for Perfect: a TypeScript rewriter
plus Bun plugins that wrap it. It desugars two syntaxes before the TS parser
sees the file — the `eff(($) => …)` sugar and the Scala-style
`for { x <- e } yield` comprehension. For bundler builds (Next.js, Vite) the
canonical `eff($)` compiler is `@perfect/swc-plugin` (AST-level, source
maps); this package covers Bun preload workflows and is the _only_ home of
the `for {} yield` syntax, which is not valid TypeScript and therefore can't
be handled by any AST plugin.

## Install

```bash
bun add @perfect/transform
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

Wire the preload in `bunfig.toml`:

```toml
preload = ["@perfect/transform/preload"]
```

Then both syntaxes work in any `.ts` file (node_modules excluded):

```ts
import { eff, succeed } from "@perfect/core";

// eff($) — Eff-specific; becomes a composed .flatMap chain
const program = eff(($) => {
  const a = $(succeed(21));
  const b = $(succeed(2));
  return a * b;
});

// for-comprehension — monad-generic; desugars to plain .flatMap/.map,
// so it works on Eff, Array, Promise, Stream, or any type with those methods
const doubled = for {
  a <- succeed(21)
  b <- succeed(2)
} yield a * b;
```

Missing `succeed` / `sync` imports are added automatically when the
desugared output needs them.

## Programmatic use

```ts
import { rewriteEffBlocks, RewriteError } from "@perfect/transform";

const output = rewriteEffBlocks(source);
// throws RewriteError instead of emitting code with a dangling `$`
```

## Entry points

| Import                          | What it does                                                 |
| ------------------------------- | ------------------------------------------------------------ |
| `@perfect/transform`            | `rewriteEffBlocks` / `RewriteError` — the pure rewriter      |
| `@perfect/transform/preload`    | Bun preload: both syntaxes + auto-import, skips node_modules |
| `@perfect/transform/plugin`     | Bun plugin: `eff($)` only + auto-import                      |
| `@perfect/transform/bun-plugin` | Bun plugin: both syntaxes, no auto-import                    |

## What it handles

- `eff(($) => { const x = $(e); … return x })` — binds, destructuring,
  `if`/`else` with `$()`, sync statements wrapped automatically
- `for { a <- e1; b <- e2 } yield a + b` →
  `e1.flatMap((a) => e2.map((b) => a + b))`
- Strings and comments are masked before scanning — code-looking text inside
  literals is never rewritten; nested comprehensions rewrite recursively

## Limitations

- Guards (`if cond` inside `for {}`) are not supported — they'd need a
  type-specific `filter`/`fail`. Use `.filter()` on the monad, or `eff($)`
- Comprehensions inside template-literal `${…}` interpolations are not
  transformed
- Source-text rewriting means no source maps — for `eff($)` with source maps
  use `@perfect/swc-plugin`

## Links

- Repo: https://github.com/spilne/perfect
- Pipeline design: [`docs/transform-pipeline.md`](../../docs/transform-pipeline.md)
- Syntax comparison: [`documentation/03-syntax.md`](../../documentation/03-syntax.md)
