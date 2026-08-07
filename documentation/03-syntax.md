# Syntax — three styles, same machinery

Perfect supports three ways to express effect pipelines. They all compile to
the same fiber walk; choose by readability and tradeoff. The full bench is in
[`bench/await-vs-flatmap-vs-dollar.md`](../packages/core/bench/await-vs-flatmap-vs-dollar.md).

| Style | Build step? | Per-step (ns) | Best for |
|---|---|---|---|
| Composed `.flatMap` | none | 14.5 | hot loops |
| `eff(function*)` (recommended) | none | 64 | most code |
| `eff(($) => ...)` (rewriter) | SWC plugin | 34 | when you want max readability + native speed |

## Composed `.flatMap`

The lowest-level surface — chained method calls.

<!-- @embed packages/core/examples/01-hello.ts#hello-flatmap -->
```ts
import { succeed } from "@perfect/core";

// The composed-flatMap form — fastest, but reads bottom-up for long chains.
const composed = succeed(21)
  .flatMap((a) => succeed(a * 2))
  .map((b) => b + 0);

console.log(composed.runSync()); // → 42
```
<!-- @end -->

Pros: fastest, no magic. Cons: nests for long chains, reads bottom-up.

## Generator — `eff(function*)`

The recommended default. Uses `yield*` to extract values from effects.

<!-- @embed packages/core/examples/03-generator-syntax.ts#gen-basic -->
```ts
import { eff, succeed, sync } from "@perfect/core";

// Use yield* to extract values from effects. Looks like async/await.
const program = eff(function* () {
  const a = yield* succeed(10);
  const b = yield* succeed(20);
  const c = yield* sync(() => a + b);
  return c * 2;
});

console.log(program.runSync()); // → 60
```
<!-- @end -->

`try/catch` works too — typed failures get routed back through `gen.throw`:

<!-- @embed packages/core/examples/03-generator-syntax.ts#gen-trycatch -->
```ts
import { eff, fail, type Eff, type Throws } from "@perfect/core";

// try/catch inside the generator catches typed failures (and defects).
const safe = eff(function* () {
  try {
    yield* fail("boom") as Eff<never, Throws<string>>;
    return "unreachable";
  } catch (e) {
    return `caught: ${e}`;
  }
});

console.log(await (safe as any).run()); // → "caught: boom"
```
<!-- @end -->

Pros: looks like async/await, no build step, native `try/catch`. Cons: ~4×
slower than `.flatMap` per step (still 2× faster than `await effect`).

## `eff($)` rewriter

The cleanest source syntax — but requires the SWC plugin / TS rewriter from
`@perfect/transform`.

```ts
import { eff, succeed, run } from "@perfect/core";

const program = eff(($) => {
  const a = $(succeed(21));
  const b = $(succeed(2));
  return a * b;
});
```

The rewriter transforms this at build time into a composed `.flatMap` chain,
so runtime cost matches `.flatMap`. Only the declaration form
`const x = $(e)` is recognized today.

## Mixing styles

All three produce `Eff<A, S>` values, so they compose freely:

```ts
const one = succeed(1).flatMap((a) => succeed(a + 1));
const two = eff(function* () { return (yield* one) * 10; });
runSync(two); // 20
```

## Pitfalls

- **Don't `await effect` in a hot loop** — every `await` pays a microtask.
  See the `await eff per step` row in the bench.
- **`yield*` not `yield`** — `yield effect` yields the effect to the driver,
  but you usually want the value, which requires `yield*`. (`yield*` calls
  the effect's `[Symbol.iterator]`, threads the value back through.)
- **`eff($)` without the plugin compiles to a runtime error** — the rewriter
  is mandatory. If you can't add it, use `eff(function*)`.

## Next

- [Services and Layers](./04-services-and-layers.md)
- [Error handling](./05-error-handling.md)
