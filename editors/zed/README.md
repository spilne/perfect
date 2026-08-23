# Perfect Effect — Zed Extension

**Status: not publishable. Highlight queries only, not wired to any file type.**

The queries in `languages/perfect-typescript/` are groundwork. They are not
applied to your `.ts` files, and cannot be, for the reason below.

## Why this does not auto-apply

The extension used to declare `path_suffixes = ["ts", "tsx"]`, which made Zed
associate every TypeScript file in every project with the "Perfect TypeScript"
language. That sounds harmless and is not: a language config has no way to
attach a language server, so the association silently replaced the built-in
TypeScript language and took tsserver with it. No diagnostics, no completions,
no go-to-definition, no rename — in any `.ts` file, project-wide, for as long as
the extension stayed installed. Installing this would have broken editing for
anyone who tried it.

There is no extension-side fix:

- `LanguageConfig` has no `language_servers` field. Its only server-related
  fields, `scope_opt_in_language_servers` and `opt_into_language_servers`,
  govern scope overrides within a language, not which server runs.
- `extension.toml` **can** declare `[language_servers.*]`, but only for a server
  the extension itself provides via a Rust/WASM `lib`. This extension has no
  `lib`, and even with one there is no way to retarget Zed's built-in TypeScript
  server at a different language.
- `hidden = true` exists for languages that are "only for syntax highlighting
  via an injection into other languages" — the right shape for this case — but
  injection has to be declared by the *host* language, and we cannot edit the
  built-in TypeScript language's `injections.scm`.

Zed simply has no supported way for one extension to add highlight queries to a
language another extension owns.

## What you can do today

Select it by hand per buffer: Command Palette → `language selector: toggle` →
"Perfect TypeScript". You get the Perfect highlights and lose LSP in that
buffer. Explicit, reversible, and scoped to one file — as opposed to the silent
project-wide breakage the old configuration caused.

Most people should not bother. `eff(($) => …)` is valid TypeScript and already
highlights fine under the built-in language.

## What the queries cover

- `eff()` / `$()` as control and bind keywords
- Effect constructors: `succeed`, `fail`, `sync`, `fork`, `forkDaemon`,
  `uninterruptible`, `race`, `timeout`, `acquireRelease`, `scoped`, `retry`, …
- Fluent methods: `.flatMap`, `.map`, `.catch`, `.catchTag`, `.orDie`,
  `.option`, `.tapError`, `.ensuring`, `.timeout`, …
- Types: `Eff`, `Throws`, `Needs`, `Exit`, `Cause`, `Stream`, `Chunk`, `Pipe`,
  `Fiber`
- Namespaces: `Stream`, `Chunk`, `Queue`, `Deferred`, `Semaphore`, `Ref`,
  `Schedule`, `WorkerPool`, `Cause`, `Exit`, `Fiber`

`injections.scm` is currently a comment-only placeholder — it injects nothing.

## The two real paths forward

Either would make this shippable; both are tracked in the roadmap.

1. **TypeScript Language Service Plugin.** Makes `eff(($) => …)` type-check
   through the bind, which is the actual DX gap. It works through tsserver, so
   it keeps every editor feature instead of trading them away, and it benefits
   VS Code and every other editor at the same time — not just Zed.
2. **A custom tree-sitter grammar** that parses `for { x <- e } yield expr`.
   Only this removes the red squiggles on that syntax, since it is not valid
   TypeScript and no TS-grammar-based approach can accept it.

## Installing for development

```bash
# Command palette → "zed: install dev extension" → pick this folder
```

Nothing will change in your `.ts` files, by design. Use the language selector as
described above to see the highlights.

## Publishing

Do not publish until path 1 or 2 above lands. Publishing an extension that
disables TypeScript tooling on install is worse than shipping nothing.
