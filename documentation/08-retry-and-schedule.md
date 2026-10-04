# Retry and Schedule

Retry transient failures with controlled backoff and jitter. Use the fluent
`.retry(policy)` method on any effect; pass either an inline config or a
`RetryPolicy` builder for richer behavior.

## Inline config

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-config -->

```ts
import { eff, fail, type Eff, type Throws } from "@spilne/perfect-core";

// Inline config form — quickest setup. Fluent .retry() method.
let calls = 0;
const flaky: Eff<string, Throws<string>> = eff(function* () {
  calls++;
  if (calls < 3) yield* fail("still failing") as Eff<never, Throws<string>>;
  return "ok";
});

console.log(await flaky.retry({ times: 5, delay: 5 }).orDie().run()); // → "ok"
console.log(calls); // → 3
```

<!-- @end -->

:::

::: syntax chainable
<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-config-flat -->

```ts
import { succeed, fail, sync, type Eff, type Throws } from "@spilne/perfect-core";

// Same retry, chainable form — sync() + .flatMap, no generator.
let callsFlat = 0;
const flakyFlat: Eff<string, Throws<string>> = sync(() => ++callsFlat).flatMap((c) =>
  c < 3 ? (fail("still failing") as Eff<never, Throws<string>>) : succeed("ok"),
);

console.log(await flakyFlat.retry({ times: 5, delay: 5 }).orDie().run()); // → "ok"
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
| `maxDelay` | `30_000` | no single wait is longer than this |
| `when` | always | predicate `(error: E) => boolean` |
| `jitter` | `false` | make each wait a random 0.5x–1.5x of its delay, so many clients don't retry at the same moment |
| `timeBudgetMs` | none | give up once this much wall-clock time has passed in total (waiting plus the attempts themselves) |

## Fluent RetryPolicy

For anything beyond trivial:

<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-policy-fluent -->

```ts
import { eff, fail, RetryPolicy, type Eff, type Throws } from "@spilne/perfect-core";

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

console.log(await flaky2.retry(policy).orDie().run()); // → "recovered"
console.log(calls); // → 3
```

<!-- @end -->

### Builders

| API / concept | Behavior |
|---|---|
| `RetryPolicy.recurs(n)` | retry up to n times, no delay |
| `RetryPolicy.spaced(ms)` / `.constant(ms)` | fixed delay between retries |
| `RetryPolicy.exponential({ initial, factor? })` / `.exponential(initial, factor?)` | exponential backoff (`factor` defaults to 2) |
| `RetryPolicy.fibonacci(initial)` | fibonacci sequence delays |
| `RetryPolicy.linear(initial)` | linearly increasing delays |
| `RetryPolicy.forever` / `.none` | retry without a limit / never retry |
| `RetryPolicy.fromSchedule(schedule)` | adapt a custom `Schedule` |

### Modifiers (chainable)

| API / concept | Behavior |
|---|---|
| `.withMaxRetries(n)` | cap retry count |
| `.withMaxDelay(ms)` | cap per-retry delay |
| `.withTimeBudget(ms)` | stop once the waits between retries add up to `ms` (time spent in the attempts doesn't count) |
| `.withWallClockBudget(ms)` | stop once `ms` of real time has passed since the first attempt, counting the attempts too. An attempt already running is not cut short; add `timeout()` for that |
| `.withFullJitter()` | randomize each delay in `[0, computed]` |
| `.withEqualJitter()` | randomize in `[computed/2, computed]` |
| `.withJitter(min, max)` | multiply delays by a custom random range |
| `.whenError(p)` | predicate on the typed error |
| `.whenCause(p)` | predicate on the full Cause |
| `.onRetry(f)` | callback before each retry attempt |
| `.and(policy)` / `.or(policy)` | intersect or union retry schedules |

## Defects don't retry by default

Only `Throws<E>` failures are retried — defects (`throw` inside `sync`)
aren't, so a real bug doesn't loop forever.

<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-on-cause-only -->

```ts
import { succeed, sync, RetryPolicy } from "@spilne/perfect-core";

// Don't retry defects (real bugs) or interrupts — only typed failures.
const probablyABug = sync(() => {
  throw new Error("this is a defect, not a typed failure");
});

const failed = await probablyABug
  .retry(RetryPolicy.recurs(3))
  .catchAllCause((c) => succeed(`gave up: cause=${c._tag}`))
  .orDie()
  .run();
// no retries — defects don't retry
console.log(failed); // → "gave up: cause=Die"
```

<!-- @end -->

If you really want to retry defects, opt in with `.whenCause(...)` or use
`.retryAllBy(...)`.

## Retry everything (typed failures + defects)

Use `.retryAllBy(...)` when you want retry decisions to inspect both typed
errors and defects from thrown exceptions.

<!-- @embed packages/core/examples/09-retry-schedule.ts#retry-all-by -->

```ts
import { eff, fail, RetryDecision, type Eff, type Throws } from "@spilne/perfect-core";

// Retry typed failures and defects with per-outcome logic.
let unstable = 0;
const mayFail: Eff<string, Throws<string>> = eff(function* () {
  unstable++;
  if (unstable < 2) {
    throw new Error("ephemeral network issue");
  }
  if (unstable < 4) {
    yield* fail("transient typed bridge issue") as Eff<never, Throws<string>>;
  }
  return "ok";
});

const recovered = await mayFail
  .retryAllBy({
    maxRetries: 4,
    handle: (attempt) => {
      if (attempt._tag === "success") return RetryDecision.stop();
      return RetryDecision.retry();
    },
  })
  .orDie()
  .run();

console.log(recovered); // → "ok"
```

<!-- @end -->

## Schedule (for repetition, not retry)

`Schedule` is the underlying recurrence pattern. `RetryPolicy` is built on
it. You can also use it directly with `repeat(eff, schedule)` — useful for
periodic jobs:

```ts
import { repeat, run, Schedule, sync } from "@spilne/perfect-core";

const heartbeat = sync(() => console.log("alive"));
await run(repeat(heartbeat, Schedule.spaced(1000)));
```

| Schedule | Delays |
|---|---|
| `Schedule.forever` / `Schedule.once` | no delay, without end / just once more |
| `Schedule.recurs(n)` | no delay, `n` times |
| `Schedule.spaced(ms)` / `Schedule.fixed(ms)` | the same delay every time |
| `Schedule.exponential(base, factor = 2)` | `base`, `base * 2`, `base * 4`, … |
| `Schedule.linear(base)` | `base`, `2 * base`, `3 * base`, … |
| `Schedule.fibonacci(base)` | `base`, `base`, `2 * base`, `3 * base`, `5 * base`, … |
| `Schedule.jittered(s, min?, max?)` | `s` with each delay multiplied by a random factor |
| `Schedule.intersect(a, b)` / `Schedule.union(a, b)` | continue while both / either continue |

`retryWith(eff, schedule, { while?, onRetry? })` retries on a schedule. Unlike
`retry`, it also retries defects (a `throw` inside `sync`); `while` gets the
typed error or the thrown value and decides. It never retries an interrupt.
`retryAllCause(eff, { shouldRetry, times, ... })` is the same idea with the
config style of `retry`, deciding from the whole `Cause`.

## Polling until something is ready

`repeatUntil` runs an effect again and again until its result passes a check.
It fails with a typed `RepeatTimeoutError` (with `reason`, `attempts`,
`elapsedMs` and the `lastResult`) if it runs out of attempts or time:

```ts
import { repeatUntil } from "@spilne/perfect-core";

const ready = repeatUntil(checkJobStatus, {
  until: (status) => status === "done",
  intervalMs: 500,        // wait between checks (default 1000)
  maxDurationMs: 60_000,  // give up after a minute
});
```

`repeatUntilWithBackoff` does the same, but the wait starts at
`initialIntervalMs` (default 100) and doubles up to `maxIntervalMs`
(default 30 000).

## Pitfalls

- **`retry` only catches typed failures.** If you `throw` inside `sync`, it
  won't retry. Use `fail()` or `.retryAllBy(...)`.
- **No jitter = thundering herd.** Always add `.withFullJitter()` for retries
  against shared infrastructure.
- **Don't retry forever.** Cap with `.withMaxRetries(n)` or use a finite
  policy like `recurs`.

## Next

- [Streams](./09-streams.md)
- [Testing](./10-testing.md)
