# Streams

Lazy, fused, effect-typed sequences. Adjacent pure operators (`map` /
`filter` / `tap`) compile to a single chunk walk for performance.

## Build a stream

|                                           |                                                           |
| ----------------------------------------- | --------------------------------------------------------- |
| `Stream.of(...vals)`                      | from explicit values                                      |
| `Stream.fromArray(arr)`                   | from a fixed array                                        |
| `Stream.fromIterable(iter)`               | from any iterable                                         |
| `Stream.fromAsyncIterable(iter, onError)` | lazy async-iterator bridge with typed failures            |
| `Stream.fromEffect(eff)`                  | one element produced by an effect                         |
| `Stream.range(start, end, step?)`         | numeric range                                             |
| `Stream.iterate(seed, f)`                 | infinite — `seed, f(seed), f(f(seed)), …`                 |
| `Stream.unfold(seed, f)`                  | finite — `f` returns `null` to stop                       |
| `Stream.fromQueue(q)`                     | bridge from a Queue                                       |
| `Stream.fromCallback(register)`           | bridge from a callback API                                |
| `Stream.fromEventEmitter(emitter, event)` | EventEmitter bridge                                       |
| `Stream.async(register)`                  | effectful callback registration                           |
| `Stream.asyncChunks(register)`            | effectful callback registration preserving emitted chunks |
| `Stream.bracket(acquire, release)`         | one resource with guaranteed release                      |
| `Stream.retryFrom(factory, policy)`        | reacquire and retry a whole source                         |
| `Stream.repeatN(factory, n)`               | reacquire and concatenate a source `n` times               |
| `Stream.repeatForever(factory)`            | reacquire a whole source until downstream stops            |
| `Stream.mergeAll(...streams)`              | concurrently merge any number of streams                   |
| `Stream.tick(ms)`                         | a `void` every `ms`                                       |

`Stream.fromQueue` treats `QueueClosed` as normal stream completion and
preserves every other queue backend effect. A `RedisQueue<A>`, for example,
becomes `Stream<A, Throws<RedisError>>` rather than losing its error type.

## Transform

|                  |                                                     |
| ---------------- | --------------------------------------------------- |
| `.map(f)`        | element-wise transform (fused)                      |
| `.filter(p)`     | keep elements matching predicate (fused)            |
| `.filterMap(f)`  | map + filter — keep `f(a)` if not undefined (fused) |
| `.tap(f)`        | side-effect per element (fused)                     |
| `.scan(zero, f)` | running fold, including the initial value           |
| `.take(n)`       | first n elements (short-circuits)                   |
| `.takeWhile(p)`  | until predicate fails                               |
| `.takeUntil(s)`  | until another stream emits                          |
| `.drop(n)`       | skip first n                                        |
| `.dropWhile(p)`  | skip the matching prefix                            |
| `.flatMap(f)`    | flatten one stream per element                      |
| `.evalMap(f)`    | map with an effect                                  |
| `.evalFilter(f)` | filter with an effectful predicate                  |
| `.tapEffect(f)`  | effectful action while retaining the element        |
| `.tapEffectFork(f)` | detached, fire-and-forget effect per element      |
| `.pauseWhen(ref, pollMs?)` | pause delivery while a shared boolean Ref is true |
| `.through(pipe)` | run a `Pipe<A, B>` stream-to-stream transformer     |

## Stateful, concurrent, and reactive operators

The effect tag union is preserved through every operator. For example,
combining `Stream<A, Throws<E1>>` with `Stream<B, Throws<E2>>` produces a
stream whose effect type contains both errors.

| Operator | Semantics |
| --- | --- |
| `.mapAccumulate(initial, f)` | thread local state and emit one value per input |
| `.statefulMap(initial, f)` | local-state shorthand for `mapAccumulate` |
| `.statefulMap({ stateBackend, keyBy, process })` | use a pluggable keyed `StateBackend` |
| `.changes(compare?)` / `.dedupe(key?)` / `.distinctBy(key)` | suppress repeated or previously seen values |
| `.grouped(n)` / `.sliding(n, step?)` | fixed batches and sliding windows |
| `.parEvalMap(n, f)` | bounded parallel evaluation in input order |
| `.parEvalMapUnordered(n, f)` | bounded parallel evaluation in completion order |
| `.merge(other)` | concurrently emit from either stream |
| `.broadcastThrough(...branches)` | pull once, fan out to every branch, and merge their outputs |
| `.observe(branch)` | run a reliable side branch while retaining source values |
| `.switchMap(f)` | latest inner stream wins; the previous inner is canceled and finalized |
| `.exhaustMap(f)` | ignore new outer values while an inner stream is active |
| `.combineLatest(other)` | emit when either initialized side changes |
| `.withLatest(other)` | emit only for the main stream, paired with the latest side value |

`switchMap`, `exhaustMap`, `combineLatest`, and `withLatest` run on Perfect
fibers. Downstream cancellation interrupts their driver fibers and runs all
source/inner finalizers; no callback or timer escapes structured concurrency.

### Single-pass fan-out

`broadcastThrough` is for parallel consumers that must share one upstream
subscription:

```ts
const routed = events.broadcastThrough(
  (stream) => stream.groupWithin(100, 5_000).tapEffect(writeAnalytics),
  (stream) => stream.filter(isAnomaly).tapEffect(sendAlert),
  (stream) => stream.grouped(1_000).tapEffect(writeArchive),
);

await routed.drain().run();
```

The source is acquired, pulled, and finalized once. Each active branch sees
every source item through its own one-chunk bounded queue; the slowest branch
therefore backpressures upstream. Branch outputs are merged in arrival order,
so their relative order is intentionally nondeterministic. A branch may end
early without blocking the others. Failure or downstream cancellation
interrupts sibling branches and runs every finalizer exactly once. The return
type contains the union of every branch output and effect type.

`observe` uses the same single-pass machinery but retains only source values:

```ts
const enriched = events
  .observe((stream) =>
    stream.groupWithin(100, 5_000).tapEffect(writeAnalytics),
  )
  .parEvalMap(16, enrich);
```

The observer is backpressured and reliable. Its failures remain typed, and
completion waits for its finalizer. It is not fire-and-forget telemetry; use an
explicit bounded queue with a chosen overflow policy when dropping telemetry is
acceptable.

`tapEffectFork` is deliberately detached: it neither waits for fork completion
nor adds fork failures to the stream error type. Prefer `observe` for reliable
work and reserve the forked form for best-effort telemetry.

## Time and buffering

| Operator | Semantics |
| --- | --- |
| `.groupWithin(maxSize, ms)` | close a group on size or time |
| `.debounce(ms)` | emit after an inactivity gap |
| `.sample(ms)` | emit the latest dirty value on each sampling boundary |
| `.audit(ms)` | emit the latest value after a non-resetting window |
| `.throttle(ms)` / `.metered(ms)` | pace delivery to at most one value per interval |
| `.spaced(ms)` | delay every value, including the first |
| `.timeout(ms)` | fail with typed `StreamTimeoutError` when a producing pull is too slow |
| `.deadline(ms)` / `.timeoutTotal(ms)` | fail with typed `StreamDeadlineError` when total runtime expires |
| `.interruptAfter(ms)` | end normally after the duration |
| `.interruptOn(signal)` | end when an `AbortSignal` fires |
| `.takeUntil(signalStream)` | end when another stream emits; propagate its failure |
| `.buffer(capacity)` | prefetch through a bounded queue with backpressure |

All timing goes through the `Clock` service, so these operators are
deterministic under `TestClock`.

## Error handling

Stream error operators mirror the `Eff` error algebra and preserve non-error
requirements such as `Needs<Service>`:

| Operator | Semantics |
| --- | --- |
| `.catch(f)` | recover every typed error with another stream |
| `.catchTag(tag, f)` | recover one tagged error and retain the others |
| `.catchSome(f)` | recover only when `f` returns a stream |
| `.catchAllCause(f)` | recover typed failures, defects, or interruption |
| `.mapError(f)` | transform typed errors |
| `.tapError(f)` / `.tapErrorCause(f)` | observe typed errors or the full Cause |
| `.tapAnyError(f)` | observe every typed failure and defect without consuming it |
| `.trapError(...classes)` | move matching defects into the typed error channel |
| `.either()` / `.attempt()` | emit `Right` values and a terminal `Left` typed error |
| `.exit()` / `.attemptCause()` | emit `Exit.Success` values or a terminal full `Cause` |

Recovery retains values emitted before failure and finalizes both the failed
source and the recovery stream.

## Run

|                  |                                       |
| ---------------- | ------------------------------------- |
| `.toArray()`     | collect into `A[]`                    |
| `.drain()`       | run for side effects, return `void`   |
| `.forEach(f)`    | apply effect per element              |
| `.head()`        | first element, or `undefined`         |
| `.collectFirst(p)` | first matching element, or `undefined` |
| `.collectWhile(p)` | matching prefix as an array          |
| `.last()`        | last element, or `undefined`          |
| `.count()`       | count emitted elements                |
| `.runSink(sink)` | run a reusable terminal postprocessor |

## Pipes vs sinks

`Pipe` and `Sink` solve different problems:

|                 |                                                           |
| --------------- | --------------------------------------------------------- |
| `Pipe<I, O, S>` | stream-to-stream transformation: `Stream<I> -> Stream<O>` |
| `Sink<A, B, S>` | terminal postprocessor: `Stream<A> -> Eff<B, S>`          |

Use a pipe when more streaming should happen after the operation. Use a sink
when you want one final value or side effect.

```ts
import { Stream, Pipes, Sinks } from "@perfect/core";

const words = await Stream.fromArray(["a\nb", "\nc"])
  .through(Pipes.lines)
  .runSink(Sinks.collectAll())
  .run();

console.log(words); // → ["a", "b", "c"]
```

Built-in sinks:

|                             |                                                |
| --------------------------- | ---------------------------------------------- |
| `Sinks.collectAll<A>()`     | collect all elements into `A[]`                |
| `Sinks.collectN<A>(n)`      | collect up to `n` elements, then stop          |
| `Sinks.drain<A>()`          | consume and discard                            |
| `Sinks.drainWith(eff)`      | drain, then return another effect's result     |
| `Sinks.forEach(f)`          | effectful action per element                   |
| `Sinks.forEachWhile(f)`     | run effectful predicate until it returns false |
| `Sinks.fold(zero, f)`       | fold to one value                              |
| `Sinks.foldEffect(zero, f)` | effectful fold                                 |
| `Sinks.fromEffect(eff)`     | ignore input and return an effect              |
| `Sinks.head<A>()`           | first element                                  |
| `Sinks.last<A>()`           | last element                                   |
| `Sinks.count<A>()`          | element count                                  |

Sinks are composable values:

```ts
const sink = Sinks.fold(0, (acc: number, n: number) => acc + n)
  .contramap((s: string) => s.length)
  .map((n) => `total:${n}`);

const result = await Stream.of("a", "bb").runSink(sink).run();
console.log(result); // → "total:3"
```

## Format pipes

`Pipes.csv` accepts arbitrary text chunks and maintains parser state across
chunk boundaries, including quoted separators, escaped quotes, CRLF, and
quoted newlines. Passing it directly to `through` emits arrays; call it with
`header: true` to emit records:

```ts
const rows = csvText.through(
  Pipes.csv({ header: true, separator: "," }),
);
```

`Pipes.base64Encode` maps `Uint8Array` to base64 strings and
`Pipes.base64Decode` restores `Uint8Array`. `base64EncodeText` and
`base64DecodeText` are the UTF-8 string conveniences. Each input chunk is one
independent base64 value, preserving message boundaries.

## Examples

### Collect after transform

<!-- @embed packages/core/examples/10-streams.ts#stream-collect -->

```ts
import { Stream } from "@perfect/core";

// Build a stream from an array, transform, collect.
const collected = await Stream.fromArray([1, 2, 3, 4, 5])
  .map((x) => x * 10)
  .filter((x) => x > 20)
  .toArray()
  .run();

console.log(collected); // → [30, 40, 50]
```

<!-- @end -->

### Side-effect per element

<!-- @embed packages/core/examples/10-streams.ts#stream-foreach -->

```ts
import { Stream, succeed } from "@perfect/core";

// forEach — apply an effect per element, return when stream exhausts.
const seen: number[] = [];
await Stream.range(1, 4)
  .forEach((n) => {
    seen.push(n);
    return succeed(undefined);
  })
  .run();
console.log(seen); // → [1, 2, 3]
```

<!-- @end -->

### Lazy infinite + take

<!-- @embed packages/core/examples/10-streams.ts#stream-mapchunks -->

```ts
import { Stream } from "@perfect/core";

// take(n) — short-circuit after n elements (lazy: never produces beyond).
const first3 = await Stream.iterate(0, (n) => n + 1)
  .take(3)
  .toArray()
  .run();
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
  .tap((r) => {
    kept.push(r.city);
  }) // side effect, fused
  .take(3) // short-circuit
  .scan(0, (acc, r) => acc + r.population) // running total (includes seed)
  .toArray()
  .run();

console.log(kept); // → ["tokyo", "delhi", "shanghai"]
console.log(top3RunningTotals); // → [0, 37_000_000, 69_000_000, 97_000_000]
```

<!-- @end -->

## Retry

`stream.retry(policy)` retries a failed pull. The retry budget resets after a
chunk is emitted, so a later failed pull receives a fresh budget. It does not
reacquire a source that already emitted data.

Use `Stream.retryFrom` when retry must finalize and reconstruct the whole
source:

```ts
const robust = Stream.retryFrom(
  () => kafkaTopic.subscribe(),
  RetryPolicy.exponential({ initial: 100, factor: 2 }).withMaxRetries(5),
);
```

Values emitted before failure stay emitted. A restarted source can therefore
produce duplicates unless it resumes from a durable offset; consumers should
remain idempotent or deduplicate by record identity.

## Resource safety

Streams are lazy, so resources attached to a stream are released by terminal
operators. `onFinalize(finalizer)` runs exactly once when the terminal effect
finishes, fails, or stops early:

```ts
import { Stream, sync } from "@perfect/core";

let finalized = 0;

await Stream.fromArray([1, 2, 3])
  .onFinalize(
    sync(() => {
      finalized++;
    }),
  )
  .take(1)
  .drain()
  .run();

console.log(finalized); // → 1
```

Push-source bridges use the same mechanism. `Stream.fromCallback`,
`Stream.fromEventEmitter`, `Stream.async`, and `Stream.asyncChunks` unregister
their waiter/listeners when the consumer short-circuits with `take`, `head`,
`runSink(Sinks.head())`, or any other terminal operation that stops before
natural source completion. `asyncChunks` retains every emitted `Chunk` as one
stream step, which lets batch-oriented drivers avoid per-element scheduling.

`Stream.fromAsyncIterable` acquires its iterator lazily, maps both synchronous
iterator acquisition failures and rejected pulls through `onError`, pulls one
item at a time, and calls `iterator.return()` when downstream stops early.

Parallel stream operators preserve failures. A failed upstream pull or failed
`parEvalMap` mapper produces a failed stream pull with the original `Cause`
rather than silently ending the stream.

## Pitfalls

- **Streams are lazy.** Building a 10M-element stream costs nothing until
  you run it.
- **`Pipe` is not terminal.** If you need a final value, use a terminal
  operator or `runSink`.
- **`forEach` doesn't collect.** If you need both side effects AND a result,
  use `tap` + `toArray`, or write a custom `Sink`.
- **Fusion stops at non-fusible ops.** `mapEffect`, `flatMap`, and `take`
  break the fused walk; expect a perf cliff if you mix them tight.

## Next

- [Testing](./10-testing.md)
- [Comparison](./comparison.md)
