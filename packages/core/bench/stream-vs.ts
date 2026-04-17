// Stream throughput: Perfect vs effect-ts Stream vs RxJS vs Array vs async iterators.
//
// Run: bun packages/core/bench/stream-vs.ts

import { group, bench, run } from "mitata";
import { Stream as PStream, run as pRun, succeed as pSucceed } from "../src";
import { Stream as EStream, Effect, Chunk as EChunk } from "effect";
import {
  from as rxFrom,
  map as rxMap,
  filter as rxFilter,
  take as rxTake,
  mergeMap as rxMergeMap,
  scan as rxScan,
  lastValueFrom,
  toArray as rxToArray,
} from "rxjs";

const N = 10_000;
const items = Array.from({ length: N }, (_, i) => i);

// ── 1. map × N — pure transformation throughput ────────────────────

group(`map × ${N}`, () => {
  bench("Array.map (baseline)", () => items.map((x) => x * 2));

  bench("perfect Stream.map", async () =>
    pRun(
      PStream.fromArray(items)
        .map((x) => x * 2)
        .runCollect(),
    ));

  bench("effect Stream.map", async () =>
    Effect.runPromise(
      EStream.fromIterable(items).pipe(
        EStream.map((x) => x * 2),
        EStream.runCollect,
      ),
    ));

  bench("RxJS map", async () =>
    lastValueFrom(
      rxFrom(items).pipe(
        rxMap((x) => x * 2),
        rxToArray(),
      ),
    ));

  bench("for…of (baseline)", () => {
    const out: number[] = new Array(items.length);
    for (let i = 0; i < items.length; i++) out[i] = items[i]! * 2;
    return out;
  });
});

// ── 2a. chained pure ops — where fusion should really win ──────────

group(`chained map × 5 over ${N}`, () => {
  bench("Array chain", () =>
    items
      .map((x) => x + 1)
      .map((x) => x * 2)
      .map((x) => x - 1)
      .map((x) => (x / 3) | 0)
      .map((x) => x + 100));
  bench("perfect Stream chain", async () =>
    pRun(
      PStream.fromArray(items)
        .map((x) => x + 1)
        .map((x) => x * 2)
        .map((x) => x - 1)
        .map((x) => (x / 3) | 0)
        .map((x) => x + 100)
        .runCollect(),
    ));
  bench("effect Stream chain", async () =>
    Effect.runPromise(
      EStream.fromIterable(items).pipe(
        EStream.map((x) => x + 1),
        EStream.map((x) => x * 2),
        EStream.map((x) => x - 1),
        EStream.map((x) => (x / 3) | 0),
        EStream.map((x) => x + 100),
        EStream.runCollect,
      ),
    ));
});

// ── 2. map + filter × N — fusion potential ─────────────────────────

group(`map + filter × ${N}`, () => {
  bench("Array.map.filter (baseline)", () => items.map((x) => x * 2).filter((x) => x % 3 === 0));

  bench("perfect Stream.map.filter", async () =>
    pRun(
      PStream.fromArray(items)
        .map((x) => x * 2)
        .filter((x) => x % 3 === 0)
        .runCollect(),
    ));

  bench("effect Stream.map.filter", async () =>
    Effect.runPromise(
      EStream.fromIterable(items).pipe(
        EStream.map((x) => x * 2),
        EStream.filter((x) => x % 3 === 0),
        EStream.runCollect,
      ),
    ));

  bench("RxJS map.filter", async () =>
    lastValueFrom(
      rxFrom(items).pipe(
        rxMap((x) => x * 2),
        rxFilter((x) => x % 3 === 0),
        rxToArray(),
      ),
    ));
});

// ── 3. take(K) from large source — early termination ───────────────

const K = 100;
group(`take(${K}) of ${N}`, () => {
  bench("Array.slice", () => items.slice(0, K));

  bench("perfect Stream.take", async () => pRun(PStream.fromArray(items).take(K).runCollect()));

  bench("effect Stream.take", async () =>
    Effect.runPromise(EStream.fromIterable(items).pipe(EStream.take(K), EStream.runCollect)));

  bench("RxJS take", async () => lastValueFrom(rxFrom(items).pipe(rxTake(K), rxToArray())));
});

// ── 4. runFold (sum) × N ───────────────────────────────────────────

group(`fold/sum × ${N}`, () => {
  bench("Array.reduce (baseline)", () => items.reduce((a, b) => a + b, 0));

  bench("perfect Stream.runFold", async () =>
    pRun(PStream.fromArray(items).runFold(0, (a, b) => a + b)));

  bench("effect Stream.runFold", async () =>
    Effect.runPromise(EStream.fromIterable(items).pipe(EStream.runFold(0, (a, b) => a + b))));

  bench("RxJS scan + last", async () =>
    lastValueFrom(rxFrom(items).pipe(rxScan((a: number, b: number) => a + b, 0))));
});

// ── 5. range(0, N) construction + collect ──────────────────────────

group(`range(0, ${N}) → collect`, () => {
  bench("Array.from", () => Array.from({ length: N }, (_, i) => i));

  bench("perfect Stream.range", async () => pRun(PStream.range(0, N).runCollect()));

  bench("effect Stream.range", async () =>
    Effect.runPromise(EStream.range(0, N - 1).pipe(EStream.runCollect)));
});

// ── 6. flatMap small inner × N ─────────────────────────────────────

const MID = 1_000;
const innerN = 3;
const midItems = Array.from({ length: MID }, (_, i) => i);
group(`flatMap (× ${MID} of ${innerN})`, () => {
  bench("Array.flatMap", () => midItems.flatMap((x) => [x, x + 1, x + 2]));

  bench("perfect Stream.flatMap", async () =>
    pRun(
      PStream.fromArray(midItems)
        .flatMap((x) => PStream.fromArray([x, x + 1, x + 2]))
        .runCollect(),
    ));

  bench("effect Stream.flatMap", async () =>
    Effect.runPromise(
      EStream.fromIterable(midItems).pipe(
        EStream.flatMap((x) => EStream.fromIterable([x, x + 1, x + 2])),
        EStream.runCollect,
      ),
    ));

  bench("RxJS mergeMap", async () =>
    lastValueFrom(
      rxFrom(midItems).pipe(
        rxMergeMap((x) => rxFrom([x, x + 1, x + 2])),
        rxToArray(),
      ),
    ));
});

// ── 7. effectful map: mapEffect of pure succeed ────────────────────

group(`mapEffect(succeed) × 1000`, () => {
  const ME = 1_000;
  const meItems = Array.from({ length: ME }, (_, i) => i);

  bench("perfect Stream.mapEffect", async () =>
    pRun(
      PStream.fromArray(meItems)
        .evalMap((x: number) => pSucceed(x * 2))
        .runCollect(),
    ));

  bench("effect Stream.mapEffect", async () =>
    Effect.runPromise(
      EStream.fromIterable(meItems).pipe(
        EStream.mapEffect((x) => Effect.succeed(x * 2)),
        EStream.runCollect,
      ),
    ));
});

// ── 8. async iterator baseline ─────────────────────────────────────

async function asyncIterMap(src: number[]): Promise<number[]> {
  const out: number[] = [];
  async function* gen(): AsyncGenerator<number> {
    for (const x of src) yield x;
  }
  for await (const v of gen()) out.push(v * 2);
  return out;
}

async function asyncIterMapFilter(src: number[]): Promise<number[]> {
  const out: number[] = [];
  async function* gen(): AsyncGenerator<number> {
    for (const x of src) yield x;
  }
  for await (const v of gen()) {
    const mapped = v * 2;
    if (mapped % 3 === 0) out.push(mapped);
  }
  return out;
}

async function asyncIterTake(src: number[], k: number): Promise<number[]> {
  const out: number[] = [];
  async function* gen(): AsyncGenerator<number> {
    for (const x of src) yield x;
  }
  let taken = 0;
  for await (const v of gen()) {
    if (taken >= k) break;
    out.push(v);
    taken++;
  }
  return out;
}

async function asyncIterFold(src: number[]): Promise<number> {
  async function* gen(): AsyncGenerator<number> {
    for (const x of src) yield x;
  }
  let acc = 0;
  for await (const v of gen()) acc += v;
  return acc;
}

group(`async iterator vs stream (${N} items)`, () => {
  bench("async-iter map", async () => asyncIterMap(items));
  bench("perfect Stream.map", async () =>
    pRun(
      PStream.fromArray(items)
        .map((x) => x * 2)
        .runCollect(),
    ));

  bench("async-iter map+filter", async () => asyncIterMapFilter(items));
  bench("perfect map+filter", async () =>
    pRun(
      PStream.fromArray(items)
        .map((x) => x * 2)
        .filter((x) => x % 3 === 0)
        .runCollect(),
    ));

  bench("async-iter take(100)", async () => asyncIterTake(items, 100));
  bench("perfect Stream.take", async () => pRun(PStream.fromArray(items).take(100).runCollect()));

  bench("async-iter fold", async () => asyncIterFold(items));
  bench("perfect Stream.runFold", async () =>
    pRun(PStream.fromArray(items).runFold(0, (a, b) => a + b)));
});

// ── 9. backpressure: slow consumer, fast producer ──────────────────
//
// Pull-based (Perfect, async iterators) — producer naturally waits for consumer.
// Push-based (RxJS default) — producer would swamp the consumer if not for
// mitata averaging; we include RxJS anyway to see the end-to-end latency.
//
// We don't measure buffer size directly (hard to do fairly across models),
// but the end-to-end time reveals whether the consumer's pacing dominates.

const BP_N = 50;
const SLOW_US = 100; // 100µs per item consumer delay
function slowWork(_: number): number {
  const t = Bun.nanoseconds();
  while (Bun.nanoseconds() - t < SLOW_US * 1000) {
    /* spin */
  }
  return _;
}

group(`slow consumer × ${BP_N} (${SLOW_US}µs/item)`, () => {
  const src = Array.from({ length: BP_N }, (_, i) => i);

  bench("async-iter slow consumer", async () => {
    async function* gen() {
      for (const x of src) yield x;
    }
    let sum = 0;
    for await (const v of gen()) sum += slowWork(v);
    return sum;
  });

  bench("perfect Stream slow consumer", async () =>
    pRun(
      PStream.fromArray(src)
        .map(slowWork)
        .runFold(0, (a, b) => a + b),
    ));

  bench("RxJS slow consumer", async () =>
    lastValueFrom(
      rxFrom(src).pipe(
        rxMap(slowWork),
        rxScan((a: number, b: number) => a + b, 0),
      ),
    ));
});

await run();
