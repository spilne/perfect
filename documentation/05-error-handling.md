# Error handling

Two kinds of errors flow through Perfect:

- **Typed failures** (`Throws<E>`) — expected, recoverable. You opt in by
  calling `fail(e)`.
- **Defects** (`Cause.Die`) — unexpected. Caused by `throw` inside a `sync`
  body, programming mistakes, OOM, etc. Not in the typed channel.

There's also `Cause.Interrupt` for cooperative cancellation.

## Typed failures with `.catch`

`.catch(handler)` removes `Throws<E>` from the type:

<!-- @embed packages/core/examples/06-error-handling.ts#catch-typed -->
```ts
import { succeed, fail, type Eff, type Throws } from "@perfect/core";

// .catch handles any typed failure, removing Throws<E> from the type.
const program: Eff<string, never> = (fail("nope") as Eff<never, Throws<string>>)
  .catch((e) => succeed(`recovered: ${e}`));

console.log(program.runSync()); // → "recovered: nope"
```
<!-- @end -->

## Tagged errors with `.catchTag`

When your error is a discriminated union, handle one variant at a time:

<!-- @embed packages/core/examples/06-error-handling.ts#catch-tag -->
```ts
import { succeed, fail, type Eff, type Throws } from "@perfect/core";

// .catchTag — handle one specific tagged error variant.
type Err = { _tag: "NotFound"; id: number } | { _tag: "Forbidden" };

const lookup = (id: number): Eff<string, Throws<Err>> =>
  id === 1 ? succeed("alice") : (fail({ _tag: "NotFound", id }) as Eff<never, Throws<Err>>);

const safe = lookup(99)
  .catchTag("NotFound", (e) => succeed(`(missing ${e.id})`))
  .catchTag("Forbidden", () => succeed("(no access)"));

console.log(safe.runSync()); // → "(missing 99)"
```
<!-- @end -->

After all tags are handled, the type is `Throws<never>` — equivalent to no
error.

## Full causes with `.catchAllCause`

If you need to see defects and interrupts too, use `.catchAllCause`:

<!-- @embed packages/core/examples/06-error-handling.ts#catch-cause -->
```ts
import { succeed, fail, type Eff, type Throws } from "@perfect/core";

// .catchAllCause — see the full Cause (Fail | Die | Interrupt | composites).
const wild = (fail("boom") as Eff<never, Throws<string>>).catchAllCause((cause) =>
  succeed(`cause: ${cause._tag}`),
);

console.log(wild.runSync()); // → "cause: Fail"
```
<!-- @end -->

`Cause` is one of:

| | |
|---|---|
| `Cause.Fail` | typed failure (`fail(e)`) |
| `Cause.Die` | defect (uncaught throw, `die(e)`) |
| `Cause.Interrupt` | fiber was cancelled |
| `Cause.Both` | parallel branches both failed |
| `Cause.Then` | sequential failure: error then finalizer error |

## Observe without handling

`.tapError(f)` runs a side-effect on failure but re-fails:

<!-- @embed packages/core/examples/06-error-handling.ts#tap-error -->
```ts
import { succeed, fail, sync, type Eff, type Throws } from "@perfect/core";

// .tapError — observe a typed failure without handling it (re-fails).
let observedError: string | null = null;
const observed = (fail("bad") as Eff<never, Throws<string>>)
  .tapError((e) => sync(() => { observedError = e; }) as any)
  .catch(() => succeed("ok"));

console.log(observed.runSync()); // → "ok"
console.log(observedError); // → "bad"
```
<!-- @end -->

## Fallback with `.orElse`

<!-- @embed packages/core/examples/06-error-handling.ts#orelse -->
```ts
import { succeed, fail, type Eff, type Throws } from "@perfect/core";

// .orElse — if this effect fails, run another.
const fallback = (fail("first") as Eff<never, Throws<string>>).orElse(() => succeed("second"));
console.log(await fallback.run()); // → "second"
```
<!-- @end -->

## Defects vs failures — when to use `fail` vs `throw`

| Use `fail(e)` when… | Use `throw` (defect) when… |
|---|---|
| The error is a normal outcome (network down, not found) | The error indicates a bug |
| You want callers to handle it via `.catch` | You want it to surface as a crash |
| You want `retry` to retry it | You don't want `retry` to retry it |

`retry` only retries `Throws<E>` failures by default. Defects don't retry —
use `retryAllCause` if you really want to.

## API summary

| | |
|---|---|
| `.catch(f)` | handle any typed failure |
| `.catchTag(tag, f)` | handle one discriminated variant |
| `.catchAllCause(f)` | handle the full Cause |
| `.orElse(() => alt)` | run an alternative on any typed failure |
| `.tapError(f)` | observe failure, re-fail |
| `.option()` | turn `Eff<A, Throws<E>>` into `Eff<A | undefined, never>` |
| `.either()` | turn `Eff<A, Throws<E>>` into `Eff<Either<E, A>, never>` |
| `.mapError(f)` | transform the error type |

## Pitfalls

- **`throw` in a `sync` body becomes a defect.** It's not catchable with
  `.catch`. Use `fail()` for expected errors.
- **Squashed errors lose structure.** When `run()` rejects, the rejection is
  a single value (the squashed cause). Use `runExit()` if you need the
  full `Cause` tree.

## Next

- [Concurrency](./06-concurrency.md)
- [Retry and schedule](./08-retry-and-schedule.md)
