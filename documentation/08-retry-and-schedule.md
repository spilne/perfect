# Retry and Schedule

Retry transient failures with controlled backoff and jitter. Use the fluent
`.retry(policy)` method on any effect; pass either an inline config or a
`RetryPolicy` builder for richer behavior.

## Inline config

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-config -->
```ts
import { eff, fail, type Eff, type Throws } from "@perfect/core";

// Inline config form — quickest setup. Fluent .retry() method.
let calls = 0;
const flaky: Eff<string, Throws<string>> = eff(function* () {
  calls++;
  if (calls < 3) yield* fail("still failing") as Eff<never, Throws<string>>;
  return "ok";
});

console.log(await flaky.retry({ times: 5, delay: 5 }).run()); // → "ok"
console.log(calls); // → 3
```
<!-- @end -->

:::

::: syntax chainable
<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-config-flat -->
```ts
import { succeed, fail, sync, type Eff, type Throws } from "@perfect/core";

// Same retry, chainable form — sync() + .flatMap, no generator.
let callsFlat = 0;
const flakyFlat: Eff<string, Throws<string>> = sync(() => ++callsFlat).flatMap((c) =>
  c < 3 ? (fail("still failing") as Eff<never, Throws<string>>) : succeed("ok"),
);

console.log(await flakyFlat.retry({ times: 5, delay: 5 }).run()); // → "ok"
console.log(callsFlat); // → 3
```
<!-- @end -->
:::

::::

The config form takes:

| field | default | what |
|---|---|---|
| `times` | required | max retry count |
| `delay` | 0 | base delay in ms |
| `backoff` | `"fixed"` | `"fixed"` or `"exponential"` |
| `maxDelay` | none | cap exponential growth |
| `when` | always | predicate `(error: E) => boolean` |

## Fluent RetryPolicy

For anything beyond trivial:

<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-policy-fluent -->
```ts
import { eff, fail, RetryPolicy, type Eff, type Throws } from "@perfect/core";

// Fluent builder — composable, expressive.
calls = 0;
const policy = RetryPolicy.exponential({ initial: 5, factor: 2 })
  .withMaxRetries(4)
  .withFullJitter()
  .whenError((e: string) => e !== "fatal");

const flaky2: Eff<string, Throws<string>> = eff(function* () {
  calls++;
  if (calls < 3) yield* fail("transient") as Eff<never, Throws<string>>;
  return "recovered";
});

console.log(await flaky2.retry(policy).run()); // → "recovered"
console.log(calls); // → 3
```
<!-- @end -->

### Builders

| | |
|---|---|
| `RetryPolicy.recurs(n)` | retry up to n times, no delay |
| `RetryPolicy.spaced(ms)` | fixed delay between retries |
| `RetryPolicy.exponential({ initial, factor })` | exponential backoff |
| `RetryPolicy.fibonacci({ initial })` | fibonacci sequence delays |

### Modifiers (chainable)

| | |
|---|---|
| `.withMaxRetries(n)` | cap retry count |
| `.withMaxDelay(ms)` | cap per-retry delay |
| `.withFullJitter()` | randomize each delay in `[0, computed]` |
| `.withEqualJitter()` | randomize in `[computed/2, computed]` |
| `.whenError(p)` | predicate on the typed error |
| `.whenCause(p)` | predicate on the full Cause |
| `.onRetry(f)` | callback before each retry attempt |

## Defects don't retry by default

Only `Throws<E>` failures are retried — defects (`throw` inside `sync`)
aren't, so a real bug doesn't loop forever.

<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-on-cause-only -->
```ts
import { succeed, sync, RetryPolicy } from "@perfect/core";

// Don't retry defects (real bugs) or interrupts — only typed failures.
const probablyABug = sync(() => {
  throw new Error("this is a defect, not a typed failure");
}) as any;

const failed = await probablyABug
  .retry(RetryPolicy.recurs(3))
  .catchAllCause((c: any) => succeed(`gave up: cause=${c._tag}`))
  .run();
// no retries — defects don't retry
console.log(failed); // → "gave up: cause=Die"
```
<!-- @end -->

If you really want to retry defects, use `retryAllCause(eff, policy)`.

## Schedule (for repetition, not retry)

`Schedule` is the underlying recurrence pattern. `RetryPolicy` is built on
it. You can also use it directly with `repeat(eff, schedule)` — useful for
periodic jobs:

```ts
import { repeat, Schedule } from "@perfect/core";

const heartbeat = sync(() => console.log("alive"));
await run(repeat(heartbeat, Schedule.spaced(1000)));
```

## Pitfalls

- **`retry` only catches typed failures.** If you `throw` inside `sync`, it
  won't retry. Use `fail()` or `retryAllCause()`.
- **No jitter = thundering herd.** Always add `.withFullJitter()` for retries
  against shared infrastructure.
- **Don't retry forever.** Cap with `.withMaxRetries(n)` or use a finite
  policy like `recurs`.

## Next

- [Streams](./09-streams.md)
- [Testing](./10-testing.md)
