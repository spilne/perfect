# Getting Started

Perfect is a TypeScript effect runtime — like effect-ts or ZIO, but with a
flat union type, fluent API, and three syntactic styles for the same
underlying machinery. This guide takes you from zero to a working program in
five minutes.

## Install

```bash
bun add @perfect/core
```

Optional packages:

```bash
bun add @perfect/transform   # SWC plugin for the eff(($) => …) sugar
```

## Your first program

The smallest possible Eff:

<!-- @embed packages/core/examples/01-hello.ts#hello-sync -->
```ts
// runSync — for purely synchronous programs (no Async, no Sleep, no Fork).
const greet = succeed("hello, perfect");
assertEq(runSync(greet), "hello, perfect");
```
<!-- @end -->

`runSync` works for any program that doesn't suspend (no `sleep`, no `Fork`,
no `Async`). For everything else, use `run`, which returns a `Promise`.

## Three syntactic styles

Perfect supports three ways to write effect pipelines, all compiling to the
same fiber walk. Pick by readability — the perf table is in
[`bench/await-vs-flatmap-vs-dollar.md`](../packages/core/bench/await-vs-flatmap-vs-dollar.md).

### Generator (recommended — no build step)

<!-- @embed packages/core/examples/01-hello.ts#hello-generator -->
```ts
// The recommended syntax: write effects in generator form, use yield* to
// extract values. No build step required.
const program = eff(function* () {
  const a = yield* succeed(21);
  const b = yield* succeed(2);
  return a * b;
});

assertEq(await run(program), 42);
```
<!-- @end -->

### Composed `.flatMap` (fastest)

<!-- @embed packages/core/examples/01-hello.ts#hello-flatmap -->
```ts
// The composed-flatMap form — fastest, but reads bottom-up for long chains.
const composed = succeed(21)
  .flatMap((a) => succeed(a * 2))
  .map((b) => b + 0);

assertEq(runSync(composed), 42);
```
<!-- @end -->

### `eff($)` source syntax (cleanest, requires SWC plugin)

```ts
const program = eff(($) => {
  const a = $(succeed(21));
  const b = $(succeed(2));
  return a * b;
});
```

This form is rewritten at build time into the composed `.flatMap` chain. See
[`@perfect/transform`](../packages/transform/) for setup.

## Running

| Function | When to use |
|---|---|
| `runSync(eff)` | Sync only — throws if the effect suspends. |
| `run(eff)` | Returns `Promise<A>`, rejects with squashed cause on failure. |
| `runExit(eff)` | Returns `Promise<Exit<E, A>>` — never throws. |
| `runFiber(eff)` | Returns a `Fiber<A>` you can join, interrupt, race. |

## Next

- [Effects](./02-effects.md) — `succeed`, `fail`, `sync`, `async`, `Throws`, `Needs`
- [Syntax](./03-syntax.md) — generator, dollar, and flatMap compared
- [Services + Layers](./04-services-and-layers.md) — dependency injection done right
