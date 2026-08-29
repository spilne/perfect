# @spilne/perfect-swc-plugin

SWC WASM plugin that desugars Perfect's `eff(($) => ...)` syntax to `.flatMap`
chains at compile time, with source maps.

## Build

From the repo root:

```bash
bun run build:swc
```

The build compiles `crates/swc-plugin-perfect/` to WebAssembly and copies the
output to `dist/plugin.wasm` in this package.

## Usage

The WASM file lives at `packages/swc-plugin/dist/plugin.wasm` after `bun run build:swc`.

### Inside this repo (file path)

Point directly at the built file — no npm install needed:

```ts
// next.config.js / vite.config.ts / etc.
const PLUGIN = new URL("./packages/swc-plugin/dist/plugin.wasm", import.meta.url).pathname;
```

### After publishing or `npm link` (package import)

Once this package is on npm or linked into `node_modules/@spilne/perfect-swc-plugin`:

```js
// Next.js
import { createRequire } from "module";
const require = createRequire(import.meta.url);

export default {
  experimental: {
    swcPlugins: [[require.resolve("@spilne/perfect-swc-plugin"), {}]],
  },
};
```

```ts
// Vite
import swc from "unplugin-swc";
import { createRequire } from "module";
const require = createRequire(import.meta.url);

export default {
  plugins: [
    swc.vite({
      jsc: {
        experimental: {
          plugins: [[require.resolve("@spilne/perfect-swc-plugin"), {}]],
        },
      },
    }),
  ],
};
```

### Local link (between projects on your machine)

```bash
cd packages/swc-plugin && npm link
cd /path/to/your-other-project && npm link @spilne/perfect-swc-plugin
```

## What it handles

- `eff(($) => { const x = $(e); return x })` → `e.flatMap((x) => succeed(x))`
- Object & array destructuring in binds: `const { a, b } = $(e)`, `const [x, y] = $(e)`
- `if`/`else` branches containing `$()` become a ternary between two effect branches
- Expression-body shorthand: `eff(($) => $(e))` and `eff(($) => 42)`
- Non-`$` statements wrapped in `sync()` automatically
- Source maps preserved — spans flow from the driving statements to generated nodes

## What it does **not** handle

- `for { x <- e } yield expr` — this is source-text rewriting, not AST. Use
  `@spilne/perfect-transform` (the Bun plugin / preload) for that.

See `docs/transform-pipeline.md` at the repo root for the full picture.
