# Perfect Effect — VS Code Extension

Syntax highlighting for the Perfect effect runtime.

## Features

Grammar injection into `source.ts` and `source.tsx` — no file-extension change
needed. Works alongside the default TypeScript support.

- Highlights `eff(($) => …)`, `$(…)` bind, and for-comprehension binds (inside
  valid-TS blocks)
- Effect constructors: `succeed`, `fail`, `fork`, `forkDaemon`, `uninterruptible`,
  `race`, `raceAll`, `timeout`, `timeoutFail`, `onExit`, `acquireRelease`, etc.
- Fluent methods: `.flatMap`, `.catch`, `.orDie`, `.option`, `.tapError`, `.ensuring`,
  `.onExit`, `.timeout`, and the rest of the algebra
- Types: `Eff`, `Throws`, `Needs`, `Exit`, `Cause`, `Stream`, `Chunk`, `Pipe`, `Fiber`
- Namespaces: `Stream.`, `Queue.`, `Schedule.`, `Cause.`, `Exit.`, etc.

## Installation (dev)

```bash
# Package locally
npm install -g @vscode/vsce
cd editors/vscode
vsce package
# Then: "Extensions: Install from VSIX..." and pick the generated .vsix
```

Or drop the folder into `~/.vscode/extensions/`.

## Syntax support

### `eff(($) => …)` — works out of the box

Valid TypeScript. The VS Code TS server treats it as a normal function call. The
SWC WASM plugin (in `crates/swc-plugin-perfect/`) rewrites it to `.flatMap` chains
at build time.

### `for { x <- e } yield expr` — requires a source-text pre-processor

The `<-` / `yield` syntax is not valid TS — the TypeScript language server will
show errors inside these blocks. Wire up the source-text rewriter at build time:

```toml
# bunfig.toml
preload = ["./packages/transform/src/preload.ts"]
```

The rewriter runs before TS sees the file. Fixing the editor squiggles cleanly
requires either a TS Language Service Plugin (TODO) or a custom `.peff` file type.

## Publishing to the marketplace

1. Create a publisher: <https://marketplace.visualstudio.com/manage>
2. Get a Personal Access Token from dev.azure.com with `Marketplace.Manage` scope.
3. `vsce login <publisher>` then `vsce publish`.

Details: <https://code.visualstudio.com/api/working-with-extensions/publishing-extension>.

## Roadmap

- [ ] TypeScript Language Service Plugin: type hints through `$()` binds
- [ ] Custom file extension `.peff` with full `for { <- } yield` parser
- [ ] Go-to-definition through the desugared form
