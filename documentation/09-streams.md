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
| `Stream.unfold(seed, f)`                  | `f` returns the next value, or `null` to stop (see below) |
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
| `Stream.succeed(a)` / `Stream.empty()`    | one value / no values                                     |
| `Stream.fail(e)`                          | a stream that fails with a typed error                    |
| `Stream.fromChunk(chunk)`                 | the values of one `Chunk`                                 |
| `Stream.unfoldEffect(seed, f)`            | like `unfold`, but `f` returns an effect                  |
| `Stream.repeat(eff)`                      | run `eff` again for every value, forever                  |
| `Stream.repeatValue(a)`                   | the same value, forever                                   |
| `Stream.suspend(() => stream)`            | build the stream fresh each time it runs                  |

`Stream.unfold` makes values a batch at a time, so `f` can be called a few
more times than the number of values you take. Keep `f` free of side effects
and use `unfoldEffect` when it has some.

`Stream.suspend` is how a stream gets its own state for each run. Anything
created inside the function (a counter, a buffer) is new every time the
stream is run, instead of being shared by every run of the same stream value.

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
| `.collect(f)`    | map and filter in one go: return `undefined` to drop |
| `.unNone()`      | drop `null` and `undefined` values                  |
| `.scanEffect(zero, f)` | running total, where `f` returns an effect    |
| `.mapChunks(f)` / `.rechunk(n)` | work on whole chunks / regroup into chunks of `n` |

Combining streams:

| Operator | Semantics |
| --- | --- |
| `.concat(other)` | all of this stream, then all of `other` |
| `.zip(other)` / `.zipWith(other, f)` | pair values up one by one; ends when the shorter stream ends |
| `.zipWithIndex()` | `[value, index]` pairs |
| `.zipWithPrevious()` | `[previous, value]` pairs (`previous` is `undefined` for the first) |
| `.interleave(other)` | one from this stream, one from `other`, and so on; stops when either one ends |
| `.orElse(() => other)` | if this stream fails, carry on with `other` |

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
| `.parJoin(n)` | flatten a stream of streams, running at most `n` inner streams at once |
| `.parJoinUnbounded()` | flatten a stream of streams, opening each inner stream as it arrives (no memory bound) |
| `.broadcastThrough(...branches)` | pull once, fan out to every branch, and merge their outputs |
| `.observe(branch)` | run a reliable side branch while retaining source values |
| `.switchMap(f)` | latest inner stream wins; the previous inner is canceled and finalized before the next one starts |
| `.exhaustMap(f)` | ignore new outer values while an inner stream is active |
| `.combineLatest(other)` | emit when either initialized side changes |
| `.withLatest(other)` | emit only for the main stream, paired with the latest side value |

`merge`, `parJoin`, `switchMap`, `exhaustMap`, `parEvalMap`, `combineLatest`,
`withLatest`, `broadcastThrough`, `observe`, and `takeUntil` run background
fibers, as do `groupWithin`, `debounce`, `sample`, `audit`, and `buffer`.

- **The fibers belong to the stream**, not to whichever fiber pulls it. They
  start on the first pull.
- **They are always cleaned up.** When the stream completes, fails, stops
  early, or its consumer is interrupted, the stream's finalizer interrupts
  them and waits for them to finish before it releases the sources.
- **Failures while stopping are not lost.** If something fails while they
  stop (say an inner stream's finalizer), the stream fails with it. The same
  goes for `switchMap` closing the inner stream it switches away from; the
  next inner stream then does not start.
- **A source interrupted on its own** (not because the stream is stopping)
  fails the stream with that interruption, so the consumer isn't left
  waiting.

No callback or timer escapes structured concurrency.

Because the finalizer owns these fibers, consume the stream with a terminal
operator such as `toArray`, `drain`, `forEach`, or `runSink`. Pulling `step` by
hand without running the stream's finalizer leaves them running. One stream
value also shares one finalizer, so do not consume it from two fibers at the
same time: whichever consumer finishes first stops the other one's fibers.

This matters when a pull runs on a short-lived fiber. `timeout`, `deadline`,
`interruptAfter`, `interruptOn`, and `takeUntil` race every pull against a timer
or signal. Composing them with these operators is safe:

<!-- @embed packages/core/examples/10-streams.ts#stream-merge-interrupt-after -->

```ts
import { Stream } from "@spilne/perfect-core";

// merge's background fibers belong to the stream, so pulls racing a timer
// (interruptAfter, timeout, takeUntil, …) don't stop them.
const ticks = await Stream.tick(10)
  .take(3)
  .merge(Stream.tick(15).take(2))
  .interruptAfter(1_000)
  .toArray()
  .run();
console.log(ticks.length); // → 5
```

<!-- @end -->

### Concurrent flattening

`parJoin(n)` flattens a `Stream<Stream<A, S2>, S>` into `Stream<A, S | S2>`,
running up to `n` inner streams concurrently. Map to streams first for a
bounded concurrent `flatMap`:

<!-- @embed packages/core/examples/10-streams.ts#stream-par-join -->

```ts
import { Stream } from "@spilne/perfect-core";

// parJoin(n) — run up to n inner streams at once and interleave their output.
const pages = await Stream.fromArray(["a", "b", "c"])
  .map((shard) => Stream.range(1, 3).map((page) => `${shard}${page}`))
  .parJoin(2)
  .toArray()
  .run();
console.log([...pages].sort()); // → ["a1", "a2", "b1", "b2", "c1", "c2"]
```

<!-- @end -->

Chunks are emitted in arrival order, so elements from different inner streams
interleave while each inner stream keeps its own order. The outer stream is
pulled only when a slot is free. `parJoinUnbounded()` never holds the outer
stream back, which suits long-lived inner streams that arrive over time:

```ts
const runs = registrations
  .map((job) => Stream.tick(job.everyMs).map(() => job.id))
  .parJoinUnbounded();
```

The result completes once the outer stream and every opened inner stream have
completed. Output passes through a bounded queue of 16 chunks, and each open
inner stream holds at most one more pending chunk. With `parJoin(n)` a slow
consumer therefore backpressures the inner streams and, through them, the
outer stream, so memory stays bounded. `parJoinUnbounded()` never stops pulling
the outer stream, even while the consumer is slow. Every inner stream it opens
stays alive as a fiber until its output is consumed, so memory grows with the
number of inner streams the outer stream emits. Use `parJoin(n)` unless that
number is bounded.

When the outer stream or any inner stream fails (including an inner
finalizer), all the others are interrupted right away. Then:

- The consumer first gets the chunks already queued (at most 16), then the
  failure, then any failures from the teardown (another inner stream failing
  at the same time, or a finalizer failing while its stream is interrupted).
- If downstream stops before it reaches the failure, the failure is dropped,
  like any other element it didn't pull (the same as `merge`). Finalizers that
  fail while the stream is being stopped still fail it.
- Every inner stream taken from the outer stream is finalized, even one that
  never started.
- The outer finalizer runs last, so inner streams can use resources the outer
  stream acquired.

Under `retry`, `parJoin` follows the rules for operators with background fibers
described in [Retry](#retry): an interrupted pull resumes the same join, a
failure that reached the consumer fails again, and a stream that is run again
starts a new join.

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

`timeout`, `deadline`, `interruptAfter`, `interruptOn` and `takeUntil` race
each pull against a timer or signal. When the pull is cut, they wait for it to
finish its cleanup before they fail or end the stream (see
[Structured teardown](./06-concurrency.md#structured-teardown)). A failure in
that cleanup is not swallowed:

- `timeout` and `deadline` fail with their error joined to the cleanup failure.
  So does `takeUntil` when its signal stream fails.
- `interruptAfter`, `interruptOn` and `takeUntil` fail with the cleanup failure
  instead of ending normally.

A linear source cleans up inside the cut pull, so slow cleanup there delays
the timeout. A pull of an operator with background fibers only stops waiting on
the operator's queue, so it is cut at once. Its fibers keep their state and are
cleaned up when the stream is finalized, where a failure joins the outcome the
same way.

A value that arrives at the same instant as a timer is not lost. That covers
a `debounce` window closing, a `groupWithin` deadline and a `sample` or
`audit` boundary, which wait on internal queues, and a `Stream.fromQueue` pull
cut by `timeout` and pulled again by `retry`. A queue take that loses its race
against the timer gives its value back (see
[Handoff to waiting fibers](./06-concurrency.md#handoff-to-waiting-fibers)).

## Error handling

Stream error operators mirror the `Eff` error algebra and preserve non-error
requirements such as `Needs<Service>`:

| Operator | Semantics |
| --- | --- |
| `.catch(f)` | recover every typed error with another stream |
| `.catchTag(tag, f)` | recover one tagged error and retain the others |
| `.catchSome(f)` | recover only when `f` returns a stream |
| `.catchAllCause(f)` | recover typed failures, defects, or an interrupt that failed an inner fiber; an interrupted consumer does not recover |
| `.mapError(f)` | transform typed errors |
| `.tapError(f)` / `.tapErrorCause(f)` | observe typed errors or the full Cause |
| `.tapAnyError(f)` | observe every typed failure and defect without consuming it |
| `.trapError(...classes)` | move matching defects into the typed error channel |
| `.either()` / `.attempt()` | emit `Right` values and a terminal `Left` typed error |
| `.exit()` / `.attemptCause()` | emit `Exit.Success` values or a terminal full `Cause` |
| `.rethrow()` | inverse of the two above: emit the values, fail at the first `Left` / `Failure` |
| `.orDie()` | turn typed errors into defects before a runner boundary |

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
| `.toAsyncIterable()` | consume with `for await` from Promise-based code |

### Consume with `for await`

`toAsyncIterable()` hands a stream to Promise-based code, such as an SDK
contract that returns `AsyncIterable<Chunk>` or a subscriber loop:

<!-- @embed packages/core/examples/10-streams.ts#stream-async-iterable -->

```ts
import { Stream, sync } from "@spilne/perfect-core";

// toAsyncIterable — consume with `for await`; leaving the loop finalizes the stream.
let finalized = 0;
const numbers = Stream.range(1, 1_000_000).onFinalize(
  sync(() => {
    finalized++;
  }),
);

const firstThree: number[] = [];
for await (const n of numbers.toAsyncIterable()) {
  firstThree.push(n);
  if (firstThree.length === 3) break;
}
console.log(firstThree); // → [1, 2, 3]
console.log(finalized); // → 1
```

<!-- @end -->

- **Pull-based.** Nothing runs until the first `next()`. Each iterator runs the
  stream on one fiber and pulls the next chunk only after the consumer has taken
  every element of the current one. Concurrent `next()` calls are served in call
  order.
- **Early exit is safe.** Once `next()` has been called, `break`, `return()`, or
  a throw in the loop body stops the stream and runs its finalizers. The loop
  continues only after the finalizers have run and the fibers the stream
  started, such as `merge` drivers or `parEvalMap` workers, have stopped.
  Calling `return()` while a `next()` is pending interrupts that pull.
- **Close it yourself.** The iterator runs on its own root fiber, so
  interrupting an enclosing fiber does not stop it. End it with `for await` or
  `return()`.
- **Failures reject.** Like `run()`, the method type-checks only when every
  error and service requirement is handled. Use `.orDie()` to let typed errors
  surface in the loop. A failure rejects `next()` with `Cause.squash(cause)`
  after finalizers have run.
- **Reuse sequentially, not concurrently.** Every `[Symbol.asyncIterator]()`
  call runs the stream again. Start another iterator only after the previous
  one has finished: operators such as `Stream.bracket`, `Stream.suspend`, and
  `catch` keep per-run state on the stream value, so overlapping iterators can
  leak resources. A single-pass source, such as `fromAsyncIterable` over a
  generator, yields only what is left the second time, often nothing, without
  an error.

`Stream` does not implement `Symbol.asyncIterator` itself. TypeScript ignores a
method's `this` constraint in `for await` and `AsyncIterable` assignments, so
streams with missing services or unhandled errors would get past the check.

## Pipes vs sinks

`Pipe` and `Sink` solve different problems:

|                 |                                                           |
| --------------- | --------------------------------------------------------- |
| `Pipe<I, O, S>` | stream-to-stream transformation: `Stream<I> -> Stream<O>` |
| `Sink<A, B, S>` | terminal postprocessor: `Stream<A> -> Eff<B, S>`          |

Use a pipe when more streaming should happen after the operation. Use a sink
when you want one final value or side effect.

```ts
import { Stream, Pipes, Sinks } from "@spilne/perfect-core";

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

Sinks are composable values. `contramap` changes what goes in, `map`
changes the result, and `flatMap` runs an effect with the result:

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

The other text pipes split the input into lines first, so they also work on
text that arrives in pieces:

| Pipe | Turns text into |
| --- | --- |
| `Pipes.lines` | one string per line (`\n` or `\r\n`) |
| `Pipes.tsv` | `string[]` per line, split on tabs; double quotes work like in CSV |
| `Pipes.ssv` | `string[]` per line, split on runs of spaces (log files, CLI output) |
| `Pipes.fixedWidth([{ name, start, end }])` | a record per line, cut at fixed columns (trimmed unless `trim: false`) |
| `Pipes.regex(/(?<name>...)/)` | a record of the named groups per matching line; other lines are dropped |
| `Pipes.jsonl` | one parsed value per line. **Lines that aren't valid JSON are dropped silently**; use `lines` + `parseAs` if you need to know |
| `Pipes.xml` | SAX-style events: `open`, `close`, `selfClose`, `text` (entities like `&amp;` are not decoded) |

For bytes and validation:

| Pipe | What it does |
| --- | --- |
| `Pipes.utf8Decode` / `Pipes.utf8Encode` | bytes ↔ text; a character split across two chunks is joined correctly |
| `Pipes.lengthPrefixed({ headerBytes?, littleEndian?, maxFrameBytes? })` | cut a byte stream into messages that each start with their length (4-byte big-endian by default). Set `maxFrameBytes` so a corrupt header fails with `FrameTooLargeError` instead of waiting for gigabytes |
| `Pipes.binaryDecode(decode)` | decode each message, e.g. after `lengthPrefixed` |
| `Pipes.parseAs(schema)` | validate each value; the stream fails with `SchemaParseError` on the first bad one |
| `Pipes.parseAsLenient(schema)` | validate each value and drop the bad ones |

`schema` is anything with a `safeParse(data)` method, so Zod, Valibot and
ArkType schemas work as they are.

There are also pipe versions of common operators (`Pipes.take`, `drop`,
`filter`, `mapPipe`, `grouped`, `scan`) for building reusable pipes.

## Examples

### Collect after transform

<!-- @embed packages/core/examples/10-streams.ts#stream-collect -->

```ts
import { Stream } from "@spilne/perfect-core";

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
import { Stream, succeed } from "@spilne/perfect-core";

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
import { Stream } from "@spilne/perfect-core";

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
import { Stream } from "@spilne/perfect-core";

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

Operators with background fibers (see
[Stateful, concurrent, and reactive operators](#stateful-concurrent-and-reactive-operators))
behave differently under `retry`, because the work that fails runs in one of
their fibers, not in the pull:

- **Interrupted pulls resume.** When `timeout` or `deadline` interrupts a pull
  and `retry` runs it again, the pull resumes against the same fibers. No
  second set starts and no source is acquired again. Nothing waiting for the
  pull is lost: an element handed to the pull as it is cut goes back to the
  operator's queue, and pull state that spans several waits (a claimed
  `parEvalMap` slot, an open `groupWithin` batch, a pending `debounce` value)
  carries over. The resumed stream emits the same elements as an uninterrupted
  one, except that `debounce`, `sample` and `audit` restart their window timer
  in the retried pull, so a window can close later and see a newer value.
- **Delivered failures stay.** Once a failure has reached the consumer, every
  retried pull fails again with the same cause, whether or not it was the first
  pull. A failed element is not skipped and a failed input is not restarted.
  This holds for a cause that contains an interruption too, such as a source
  that interrupted itself: only a pull whose own fiber was interrupted counts
  as cut and resumes. Linear operators such as `evalMap` run a failed pull
  again instead.
- **Retry where the work runs.** To retry a failing input or mapper, retry it
  before it reaches the operator: `input.retry(policy).merge(other)` or
  `.parEvalMap(n, (a) => f(a).retry(policy))`. To restart the sources, use
  `Stream.retryFrom` or run the stream again. A stream that is run again, for
  example as a `catch` fallback or in `concat`, starts fresh.

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
import { Stream, sync } from "@spilne/perfect-core";

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
A value emitted to a pull that is interrupted before it runs goes back to the
head of the buffer for the next pull.

### When the consumer is slower than the producer

Values wait in a buffer until the consumer pulls them. The buffer holds 1024
items by default (for `asyncChunks`, 1024 chunks). You can change both the
size and what happens when it is full:

```ts
Stream.async<Message, never>(
  (emit, close) => sync(() => subscribe(async (msg) => {
    // emit() returns a Promise when the buffer is full. Awaiting it makes the
    // producer wait for the consumer instead of piling up messages.
    await emit(msg);
  })),
  { bufferSize: 256, overflow: "backpressure" },
);
```

| `overflow` | when the buffer is full |
| --- | --- |
| `"backpressure"` (default) | keep the value; `emit()` returns a Promise that resolves once there is room |
| `"dropNewest"` | throw the new value away |
| `"dropOldest"` | throw the oldest buffered value away |

Nothing is ever dropped unless you ask for it. If the producer can't wait
(an event emitter, a `LISTEN` callback), values keep buffering, so pick a
drop mode when memory matters more than every value.

`Stream.fromAsyncIterable` acquires its iterator lazily, maps both synchronous
iterator acquisition failures and rejected pulls through `onError`, pulls one
item at a time, and calls `iterator.return()` when downstream stops early.

Parallel stream operators preserve failures. A failed upstream pull or failed
`parEvalMap` mapper produces a failed stream pull with the original `Cause`
rather than silently ending the stream.

## Pitfalls

- **Pulling is lazy; input construction may not be.** `Stream.range` builds
  chunks on demand, while `Stream.fromArray(buildLargeArray())` builds its
  array immediately. `fromIterable` reads a generator or other iterable
  lazily, a small batch at a time, so `take(1)` reads one value. Other
  sources can still produce a full chunk before `take(1)` stops them.
- **`Pipe` is not terminal.** If you need a final value, use a terminal
  operator or `runSink`.
- **`forEach` doesn't collect.** If you need both side effects AND a result,
  use `tap` + `toArray`, or write a custom `Sink`.
- **Close manual iterators.** An iterator from `toAsyncIterable()` that is
  neither exhausted nor closed with `return()` keeps its stream parked and its
  resources open. `for await` closes it for you.
- **Counts and durations are validated when the operator is built.**
  `parEvalMap`, `parEvalMapUnordered`, `buffer`, `groupWithin`'s `maxSize`,
  and `parJoin`'s `maxOpen` take a positive integer or `Infinity`. `grouped`
  and `sliding` take a positive integer. Durations (`Stream.tick`,
  `debounce`, `groupWithin`'s `timeoutMs`, `sample`, `audit`,
  `throttle`/`metered`, `spaced`, `timeout`, `deadline`, `interruptAfter`, and
  `pauseWhen`) take a finite, non-negative number of milliseconds; `sample`,
  `audit`, and `pauseWhen` wait at least 1 ms. Any other value throws
  `RangeError` instead of being rounded or firing immediately.
- **Fusion stops at non-fusible ops.** `evalMap`, `flatMap`, and `take`
  break a fused chain; benchmark the actual pipeline if throughput matters.

## Next

- [Testing](./10-testing.md)
- [Comparison](./comparison.md)
