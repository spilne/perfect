import { describe, expect, test } from "bun:test";
import { Pipes, Stream, run, runExit } from "../src";

function frame(payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(4 + payload.length);
  new DataView(out.buffer).setUint32(0, payload.length);
  out.set(payload, 4);
  return out;
}

describe("pipes start fresh on every run", () => {
  test("lines", async () => {
    const eff = Stream.of("a\nb", "c").through(Pipes.lines).toArray();
    expect(await run(eff)).toEqual(["a", "bc"]);
    expect(await run(eff)).toEqual(["a", "bc"]);
  });

  test("csv with a header", async () => {
    const eff = Stream.of("name\nann\n")
      .through(Pipes.csv({ header: true }))
      .toArray();
    expect(await run(eff)).toEqual([{ name: "ann" }]);
    expect(await run(eff)).toEqual([{ name: "ann" }]);
  });

  test("lengthPrefixed", async () => {
    const bytes = frame(new Uint8Array([1, 2]));
    const eff = Stream.of(bytes.subarray(0, 3), bytes.subarray(3))
      .through(Pipes.lengthPrefixed())
      .toArray();
    expect((await run(eff)).map((m) => [...m])).toEqual([[1, 2]]);
    expect((await run(eff)).map((m) => [...m])).toEqual([[1, 2]]);
  });
});

// Sizes are picked so the old, quadratic code takes seconds and the linear
// code a few milliseconds; the 1 s limit sits far from both, so a busy
// machine doesn't make these flaky.
describe("pipes stay linear on long inputs", () => {
  test("a line split over many chunks", async () => {
    const pieces = Array.from({ length: 100_000 }, () => "x");
    const started = performance.now();
    const out = await run(
      Stream.fromArray([...pieces, "\n"])
        .through(Pipes.lines)
        .toArray(),
    );
    expect(out[0]).toHaveLength(100_000);
    expect(performance.now() - started).toBeLessThan(1_000);
  });

  test("many frames in one chunk, and one big frame over many chunks", async () => {
    const frames = Array.from({ length: 100_000 }, (_, i) => frame(new Uint8Array([i & 0xff])));
    const joined = new Uint8Array(frames.reduce((n, f) => n + f.length, 0));
    let offset = 0;
    for (const f of frames) {
      joined.set(f, offset);
      offset += f.length;
    }
    let started = performance.now();
    expect(await run(Stream.of(joined).through(Pipes.lengthPrefixed()).count())).toBe(100_000);
    expect(performance.now() - started).toBeLessThan(1_000);

    const big = frame(new Uint8Array(200_000).fill(7));
    const slices = Array.from({ length: Math.ceil(big.length / 10) }, (_, i) =>
      big.subarray(i * 10, i * 10 + 10),
    );
    started = performance.now();
    const [message] = await run(Stream.fromArray(slices).through(Pipes.lengthPrefixed()).toArray());
    expect(message!.length).toBe(200_000);
    expect(performance.now() - started).toBeLessThan(1_000);
  });
});

describe("lengthPrefixed maxFrameBytes", () => {
  test("a header bigger than the limit fails the stream", async () => {
    const exit = await runExit(
      Stream.of(frame(new Uint8Array(100)))
        .through(Pipes.lengthPrefixed({ maxFrameBytes: 10 }))
        .drain(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const error = (exit.cause as { error?: { _tag: string; frameBytes: number } }).error;
      expect(error?._tag).toBe("FrameTooLargeError");
      expect(error?.frameBytes).toBe(100);
    }
  });

  test("frames within the limit pass", async () => {
    const out = await run(
      Stream.of(frame(new Uint8Array([5])))
        .through(Pipes.lengthPrefixed({ maxFrameBytes: 10 }))
        .toArray()
        .orDie(),
    );
    expect(out.map((m) => [...m])).toEqual([[5]]);
  });
});
