import { describe, test, expect } from "bun:test";
import { Stream } from "@perfect/core/stream";
import type { Streamable, Acknowledgeable, Sinkable, Codec } from "@perfect/core/connect";
import { StreamTopology, analyzeTopology } from "../src";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function mockSource<T>(): Streamable<T> & Acknowledgeable<T> {
  const codec: Codec<T> = { encode: (v) => v, decode: (v) => v as T };
  return {
    codec,
    subscribe: () => Stream.fromIterable([]),
    subscribeAck: () => Stream.fromIterable([]),
  };
}

function mockSink<T>(): Sinkable<T> {
  return {
    publish: async () => {},
    codec: { encode: (v) => v, decode: (v) => v as T },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("analyzeTopology", () => {
  test("detects keyBy → stateful op without shuffle", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const warnings = analyzeTopology(topology.compiled);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.type).toBe("missing-shuffle");
    expect(warnings[0]!.message).toContain("aggregate");
    expect(warnings[0]!.message).toContain("shuffle");
  });

  test("no warning when shuffle is present", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .shuffle()
      .tumbling(60_000)
      .count()
      .to(mockSink());

    const warnings = analyzeTopology(topology.compiled);

    expect(warnings).toHaveLength(0);
  });

  test("no warning for keyBy → stateless → sink (no stateful op)", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .map((e) => e.v)
      .to(mockSink());

    const warnings = analyzeTopology(topology.compiled);

    expect(warnings).toHaveLength(0);
  });

  test("detects keyBy → process without shuffle", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; v: number }>())
      .keyBy((e) => e.userId)
      .process<{ total: number }, number>({
        init: () => ({ total: 0 }),
        process: (state, value) => ({
          state: { total: state.total + value.v },
          emit: state.total + value.v,
        }),
      })
      .to(mockSink());

    const warnings = analyzeTopology(topology.compiled);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.type).toBe("missing-shuffle");
    expect(warnings[0]!.message).toContain("process");
  });

  test("detects keyBy → dedupe without shuffle", () => {
    const topology = StreamTopology.source(mockSource<{ userId: string; id: string }>())
      .keyBy((e) => e.userId)
      .dedupe((e) => e.id)
      .to(mockSink());

    const warnings = analyzeTopology(topology.compiled);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.message).toContain("dedupe");
  });
});
