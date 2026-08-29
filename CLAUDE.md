# CLAUDE.md

Project-level instructions for AI agents working on this codebase.

## Project Overview

**Perfect** — a TypeScript Effect runtime with cats-effect / ZIO-level ergonomics. Nx monorepo with Bun runtime. Linting via oxlint.

### Directory Layout

```
packages/
  core/        Eff<A,S> runtime, fibers, scheduler, stream, concurrency primitives
  transform/   For-comprehension compiler (TS rewriter + Bun plugin)
crates/
  swc-plugin-perfect/    SWC WASM plugin (for Next.js/Vite)
editors/
  zed/                   Zed editor extension (syntax highlighting)
docs/                    Design docs, comparisons, research
```

## Core Concepts

- `Eff<A, S>` — the effect type. `A` is the value, `S` is a flat union of effect tags.
- `Throws<E>` — typed error in the union. Removed by `.catch()` / `.catchTag()`.
- `Needs<D>` — typed dependency in the union. Removed by `provide()`.
- Fluent API: `.flatMap()`, `.map()`, `.catch()` — no `pipe()`.
- Two for-comprehension syntaxes: `eff(($) => { ... })` and `for { x <- e } yield x`.

## TypeScript Conventions

- Files: kebab-case (`stream-pipeline.ts`)
- Classes: PascalCase (`WorkerPool`)
- Interfaces: PascalCase, no `I` prefix
- Prefer objects for 2+ parameters
- No comments unless the WHY is non-obvious

## Testing

- Framework: `bun:test`
- Location: `test/` directory in each package
- Run: `bun test` in package dir, or `bun nx run @spilne/perfect-core:test`

## Commands

```bash
bun test                          # all tests
bun nx run @spilne/perfect-core:test     # core tests only
bun nx run @spilne/perfect-core:lint     # lint core
bun run bench                     # benchmarks
bun run build:swc                 # build SWC WASM plugin
```
