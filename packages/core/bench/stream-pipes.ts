// .through() composition overhead vs direct Stream ops.
//
// Question: does wrapping a transformation in a `Pipe` and applying via
// `.through(pipe)` cost more than calling the raw method inline?
//
// Also: does stacking N `.through` calls scale linearly, or accrue
// overhead per layer?
//
// Run: bun packages/core/bench/stream-pipes.ts

import { group, bench, run as mitataRun } from "mitata";
import { Stream, Pipes, run, type Pipe } from "../src";

const N = 10_000;
const arr = Array.from({ length: N }, (_, i) => i);

// ── Single transformation: .map direct vs through(pipe) ───────────

group(`single transform × ${N}`, () => {
  const addOne: Pipe<number, number> = (s) => s.map((x) => x + 1);

  bench("direct .map (baseline)", async () => {
    await run(Stream.fromArray(arr).map((x) => x + 1).runDrain());
  });

  bench(".through(pipe) wrapping .map", async () => {
    await run(Stream.fromArray(arr).through(addOne).runDrain());
  });
});

// ── Chain of 3 transforms: inline vs pipe chain ───────────────────

group(`3 transforms chained × ${N}`, () => {
  bench("direct .map × 3 (baseline)", async () => {
    await run(
      Stream.fromArray(arr)
        .map((x) => x + 1)
        .map((x) => x * 2)
        .map((x) => x - 3)
        .runDrain(),
    );
  });

  bench(".through × 3 pipes (each wraps .map)", async () => {
    const p1: Pipe<number, number> = (s) => s.map((x) => x + 1);
    const p2: Pipe<number, number> = (s) => s.map((x) => x * 2);
    const p3: Pipe<number, number> = (s) => s.map((x) => x - 3);
    await run(Stream.fromArray(arr).through(p1).through(p2).through(p3).runDrain());
  });
});

// ── Realistic: Uint8Array → utf8Decode → lines ────────────────────

const LINE_COUNT = 1000;
const LINE = "the quick brown fox jumps over the lazy dog";
const textBody = Array(LINE_COUNT).fill(LINE).join("\n");
const bytesBody = new TextEncoder().encode(textBody);
// Split into ~32 chunks to exercise boundary handling
const chunks: Uint8Array[] = [];
const CHUNKS = 32;
const chunkSize = Math.ceil(bytesBody.length / CHUNKS);
for (let i = 0; i < bytesBody.length; i += chunkSize) {
  chunks.push(bytesBody.slice(i, i + chunkSize));
}

group(`utf8Decode + lines: ${LINE_COUNT} lines in ${CHUNKS} Uint8Array chunks`, () => {
  bench("through(utf8Decode).through(lines) — pipe chain", async () => {
    const lines = await run(
      Stream.fromArray(chunks).through(Pipes.utf8Decode).through(Pipes.lines).runCollect(),
    );
    if (lines.length !== LINE_COUNT) throw new Error(`${lines.length} vs ${LINE_COUNT}`);
  });

  bench("inline string build + split (non-stream baseline)", () => {
    let s = "";
    const dec = new TextDecoder();
    for (const c of chunks) s += dec.decode(c, { stream: true });
    s += dec.decode();
    const split = s.split("\n");
    // Strip trailing \r to match Pipes.lines semantics
    for (let i = 0; i < split.length; i++) {
      if (split[i]!.endsWith("\r")) split[i] = split[i]!.slice(0, -1);
    }
    if (split.length !== LINE_COUNT) throw new Error(`${split.length} vs ${LINE_COUNT}`);
  });
});

// ── Deep pipe stack: overhead scaling ─────────────────────────────

group(`stacking .through N times (identity pipes) × ${N}`, () => {
  const identity: Pipe<number, number> = (s) => s.map((x) => x);

  const run1 = (s: Stream<number>) => s.through(identity).runDrain();
  const run4 = (s: Stream<number>) =>
    s.through(identity).through(identity).through(identity).through(identity).runDrain();
  const run16 = (s: Stream<number>) => {
    let out: Stream<number> = s;
    for (let i = 0; i < 16; i++) out = out.through(identity);
    return out.runDrain();
  };

  bench("1 through", async () => {
    await run(run1(Stream.fromArray(arr)));
  });
  bench("4 through", async () => {
    await run(run4(Stream.fromArray(arr)));
  });
  bench("16 through", async () => {
    await run(run16(Stream.fromArray(arr)));
  });
});

await mitataRun();
