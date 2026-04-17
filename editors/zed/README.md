# Perfect Effect — Zed Extension

Syntax highlighting and language support for the Perfect effect runtime.

## Features

- Highlights `eff()`, `$()` as control/bind keywords
- Highlights effect constructors: `succeed`, `fail`, `sync`, `fork`, `forkDaemon`,
  `uninterruptible`, `interruptible`, `race`, `raceAll`, `timeout`, `timeoutFail`,
  `onExit`, `acquireRelease`, `scoped`, `retry`, etc.
- Highlights fluent methods: `.flatMap`, `.map`, `.catch`, `.catchTag`, `.orDie`,
  `.option`, `.tapError`, `.ensuring`, `.onExit`, `.timeout`, etc.
- Recognizes types: `Eff`, `Throws`, `Needs`, `Exit`, `Cause`, `Stream`, `Chunk`, `Pipe`, `Fiber`
- Recognizes capital-case namespaces: `Stream`, `Chunk`, `Queue`, `Deferred`,
  `Semaphore`, `Ref`, `Schedule`, `WorkerPool`, `Cause`, `Exit`, `Fiber`

Uses the built-in TypeScript tree-sitter grammar — no custom grammar needed for the
`eff(($) => …)` syntax since it is valid TypeScript.

## Installation (dev)

The easiest way: open the command palette in Zed and run
`zed: install dev extension`, then pick the `editors/zed` folder in this repo.

Alternatively, symlink this folder into Zed's extensions directory. The path
depends on your OS — the example below is for Linux:

```bash
# Linux
ln -s /path/to/perfect/editors/zed ~/.local/share/zed/extensions/installed/perfect-effect

# macOS
ln -s /path/to/perfect/editors/zed "$HOME/Library/Application Support/Zed/extensions/installed/perfect-effect"
```

Then restart Zed. The extension applies to `.ts` and `.tsx` files.

## Syntax support

### `eff(($) => …)` — works out of the box

This is valid TypeScript. Zed's TS parser + LSP see it as normal code. The SWC
WASM plugin (in `crates/swc-plugin-perfect/`) rewrites it to `.flatMap` chains at
build time.

### `for { x <- e } yield expr` — requires a source-text pre-processor

Zed's TypeScript parser will show errors for `for { <- }` because the syntax isn't
valid TS. To use it, wire up the source-text rewriter (in `packages/transform/`)
as a Bun preload:

```toml
# bunfig.toml
preload = ["./packages/transform/src/preload.ts"]
```

The rewriter runs before TS sees the file, so the compiler never encounters
`<-` or `yield`. The Zed LSP will still show errors inside `for { }` blocks
themselves — fixing that cleanly requires a custom tree-sitter grammar.

## Publishing to the Zed registry

1. Fork <https://github.com/zed-industries/extensions>.
2. Add a submodule pointing at this folder's git history.
3. Register the extension in `extensions.toml` at the repo root.
4. Open a PR. The Zed team reviews and merges.

Details: <https://zed.dev/docs/extensions/developing-extensions>.

## Roadmap

- [ ] Custom tree-sitter grammar for `for { <- } yield` (no red squiggles)
- [ ] Language server plugin: inline type hints showing `Eff<A, S>` types
- [ ] Go-to-definition through `$()` / `<-` binds to the underlying effect
