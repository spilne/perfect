import { describe, test, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import type { Streamable, Acknowledgeable, Sinkable, Codec } from "@spilne/perfect-core/connect";
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
    publish: () => succeed(undefined),
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

describe("analyzeTopology with joins", () => {
  type Click = { user: string };
  type User = { user: string; name: string };
  const users = (shuffled: boolean) => {
    const keyed = StreamTopology.source(mockSource<User>()).keyBy((u) => u.user);
    return shuffled ? keyed.shuffle() : keyed;
  };
  const clicks = (shuffled: boolean) => {
    const keyed = StreamTopology.source(mockSource<Click>()).keyBy((c) => c.user);
    return shuffled ? keyed.shuffle() : keyed;
  };

  test("warns for each side of a join that isn't shuffled", () => {
    const warnings = analyzeTopology(
      clicks(false).join(users(false), { windowMs: 1_000 }).to(mockSink()).compiled,
    );
    expect(warnings.map((w) => w.message.split(" side")[0])).toEqual(["the left", "the right"]);
    expect(warnings.every((w) => w.node.type === "join")).toBe(true);
  });

  test("names the one side that misses a shuffle", () => {
    const warnings = analyzeTopology(
      clicks(true).join(users(false), { windowMs: 1_000 }).to(mockSink()).compiled,
    );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.message).toStartWith("the right side");
  });

  test("no warning when both sides are shuffled", () => {
    const topology = clicks(true).join(users(true), { windowMs: 1_000 }).to(mockSink());
    expect(analyzeTopology(topology.compiled)).toEqual([]);
  });

  test("a stateful step after a join isn't warned about again", () => {
    const topology = clicks(false)
      .join(users(true), { windowMs: 1_000 })
      .dedupe((pair) => JSON.stringify(pair))
      .to(mockSink());
    const warnings = analyzeTopology(topology.compiled);
    expect(warnings.map((w) => w.node.type)).toEqual(["join"]);
  });
});
