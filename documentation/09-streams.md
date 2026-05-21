# Streams

Lazy, fused, effect-typed sequences. Adjacent pure operators (`map` /
`filter` / `tap`) compile to a single chunk walk for performance.

## Build a stream

| | |
|---|---|
| `Stream.of(...vals)` | from explicit values |
| `Stream.fromArray(arr)` | from a fixed array |
| `Stream.fromIterable(iter)` | from any iterable |
| `Stream.fromEffect(eff)` | one element produced by an effect |
| `Stream.range(start, end, step?)` | numeric range |
| `Stream.iterate(seed, f)` | infinite — `seed, f(seed), f(f(seed)), …` |
| `Stream.unfold(seed, f)` | finite — `f` returns `null` to stop |
| `Stream.fromQueue(q)` | bridge from a Queue |
| `Stream.fromCallback(register)` | bridge from a callback API |
| `Stream.fromEventEmitter(emitter, event)` | EventEmitter bridge |
| `Stream.tick(ms)` | a `void` every `ms` |

## Transform

| | |
|---|---|
| `.map(f)` | element-wise transform (fused) |
| `.filter(p)` | keep elements matching predicate (fused) |
| `.filterMap(f)` | map + filter — keep `f(a)` if not undefined (fused) |
| `.tap(f)` | side-effect per element (fused) |
| `.scan(zero, f)` | running fold |
| `.take(n)` | first n elements (short-circuits) |
| `.takeWhile(p)` | until predicate fails |
| `.drop(n)` | skip first n |
| `.flatMap(f)` | flatten one stream per element |
| `.mapEffect(f)` | map with an effect |

## Run

| | |
|---|---|
| `.toArray()` | collect into `A[]` |
| `.drain()` | run for side effects, return `void` |
| `.forEach(f)` | apply effect per element |

## Examples

### Collect after transform

<!-- @embed packages/core/examples/10-streams.ts#stream-collect -->
```ts
import { Stream } from "@perfect/core";

// Build a stream from an array, transform, collect.
const collected = await Stream.fromArray([1, 2, 3, 4, 5])
  .map((x) => x * 10)
  .filter((x) => x > 20)
  .toArray().run();

console.log(collected); // → [30, 40, 50]
```
<!-- @end -->

### Side-effect per element

<!-- @embed packages/core/examples/10-streams.ts#stream-foreach -->
```ts
import { Stream, succeed } from "@perfect/core";

// forEach — apply an effect per element, return when stream exhausts.
const seen: number[] = [];
await Stream.range(1, 4).forEach((n) => {
  seen.push(n);
  return succeed(undefined);
}).run();
console.log(seen); // → [1, 2, 3]
```
<!-- @end -->

### Lazy infinite + take

<!-- @embed packages/core/examples/10-streams.ts#stream-mapchunks -->
```ts
import { Stream } from "@perfect/core";

// take(n) — short-circuit after n elements (lazy: never produces beyond).
const first3 = await Stream.iterate(0, (n) => n + 1).take(3).toArray().run();
console.log(first3); // → [0, 1, 2]
```
<!-- @end -->

### A fuller pipeline

<!-- @embed packages/core/examples/11-stream-pipeline.ts#pipeline-etl -->
```ts
import { Stream } from "@perfect/core";

// A small ETL: parse, filter, enrich, accumulate.
type Row = { city: string; population: number };
const rawCsv = [
  "tokyo,37000000",
  "delhi,32000000",
  "shanghai,28000000",
  "saopaulo,22000000",
  "mexicocity,22000000",
];

const kept: string[] = [];
const top3RunningTotals = await Stream.fromArray(rawCsv)
  .map((line) => {
    const [city, n] = line.split(",");
    return { city, population: Number(n) } as Row;
  })
  .filter((r) => r.population >= 25_000_000) // pure filter
  .tap((r) => { kept.push(r.city); }) // side effect, fused
  .take(3) // short-circuit
  .scan(0, (acc, r) => acc + r.population) // running total (includes seed)
  .toArray().run();

console.log(kept); // → ["tokyo", "delhi", "shanghai"]
console.log(top3RunningTotals); // → [0, 37_000_000, 69_000_000, 97_000_000]
```
<!-- @end -->

## Retry

`stream.retry(policy)` rebuilds the stream from scratch on failure. Use
`Stream.suspend(() => ...)` if your source has per-attempt state:

```ts
const flaky = Stream.suspend(() =>
  Stream.fromEffect(maybeFail()),
);
const robust = flaky.retry(RetryPolicy.recurs(3));
```

## Pitfalls

- **Streams are lazy.** Building a 10M-element stream costs nothing until
  you run it.
- **`forEach` doesn't collect.** If you need both side effects AND a
  result, use `tap` + `toArray`.
- **Fusion stops at non-fusible ops.** `mapEffect`, `flatMap`, and `take`
  break the fused walk; expect a perf cliff if you mix them tight.

## Next

- [Testing](./10-testing.md)
- [Comparison](./comparison.md)
