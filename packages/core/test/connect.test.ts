// @perfect/core/connect — contracts, codecs, OffsetTracker, and
// autoCommitBatchWithin. Guard + tracker tests ported from promin
// (streaming-typeclasses.test.ts, offset-tracker.test.ts).

import { describe, test, expect } from "bun:test";
import { run } from "../src";
import { Stream } from "../src/stream";
import {
  isStreamable,
  isSinkable,
  isKeyedSinkable,
  isPartitionable,
  isReplayable,
  isAcknowledgeable,
  isCheckpointable,
  JsonCodec,
  codecFromSchema,
  codecTuple,
  codecRecord,
  codecArray,
  OffsetTracker,
  Partition,
  autoCommitBatchWithin,
  type Envelope,
} from "../src/connect";

const p0 = Partition(0);
const p1 = Partition(1);

// ── Type guards ────────────────────────────────────────────────────

describe("streaming typeclass guards", () => {
  const codec = { encode: (v: any) => v, decode: (v: any) => v };
  const mockStreamable = { subscribe: () => null as any, codec };
  const mockSinkable = { publish: async () => {}, codec };

  test("isStreamable", () => {
    expect(isStreamable(mockStreamable)).toBe(true);
    expect(isStreamable({})).toBe(false);
    expect(isStreamable(null)).toBe(false);
    expect(isStreamable({ subscribe: "not a fn" })).toBe(false);
  });

  test("isSinkable", () => {
    expect(isSinkable(mockSinkable)).toBe(true);
    expect(isSinkable({})).toBe(false);
    expect(isSinkable({ publish: "not a fn" })).toBe(false);
  });

  test("isKeyedSinkable (same shape as sinkable)", () => {
    expect(isKeyedSinkable(mockSinkable)).toBe(true);
  });

  test("isPartitionable", () => {
    expect(isPartitionable({ ...mockStreamable, partitions: 3 })).toBe(true);
    expect(isPartitionable(mockStreamable)).toBe(false);
    expect(isPartitionable({ ...mockStreamable, partitions: "3" })).toBe(false);
  });

  test("isReplayable", () => {
    expect(isReplayable({ ...mockStreamable, subscribeFrom: () => null })).toBe(true);
    expect(isReplayable(mockStreamable)).toBe(false);
  });

  test("isAcknowledgeable", () => {
    expect(isAcknowledgeable({ ...mockStreamable, subscribeAck: () => null })).toBe(true);
    expect(isAcknowledgeable(mockStreamable)).toBe(false);
  });

  test("isCheckpointable", () => {
    expect(
      isCheckpointable({
        ...mockStreamable,
        commitOffset: async () => {},
        getCommittedOffset: async () => null,
      }),
    ).toBe(true);
    expect(isCheckpointable(mockStreamable)).toBe(false);
    expect(isCheckpointable({ ...mockStreamable, commitOffset: async () => {} })).toBe(false);
  });
});

// ── Codecs ─────────────────────────────────────────────────────────

describe("codecs", () => {
  test("JsonCodec is identity", () => {
    const v = { a: 1 };
    expect(JsonCodec.encode(v)).toBe(v);
    expect(JsonCodec.decode(v)).toBe(v);
  });

  test("codecFromSchema decodes through parse", () => {
    const schema = {
      parse: (data: unknown) => {
        if (typeof data !== "number") throw new Error("not a number");
        return data;
      },
    };
    const codec = codecFromSchema(schema);
    expect(codec.decode(42)).toBe(42);
    expect(() => codec.decode("nope")).toThrow("not a number");
  });

  test("codecTuple round-trips", () => {
    const codec = codecTuple(JsonCodec, JsonCodec);
    expect(codec.decode(codec.encode([1, "x"]))).toEqual([1, "x"]);
  });

  test("codecRecord round-trips", () => {
    const codec = codecRecord(JsonCodec);
    expect(codec.decode(codec.encode({ a: 1, b: 2 }))).toEqual({ a: 1, b: 2 });
  });

  test("codecArray round-trips", () => {
    const codec = codecArray(JsonCodec);
    expect(codec.decode(codec.encode([1, 2, 3]))).toEqual([1, 2, 3]);
  });
});

// ── OffsetTracker ──────────────────────────────────────────────────

describe("OffsetTracker — parallel-safe commit ordering", () => {
  test("sequential completions — commit advances with each ack", () => {
    const tracker = new OffsetTracker();
    tracker.complete(p0, 0);
    tracker.complete(p0, 1);
    tracker.complete(p0, 2);
    expect(tracker.committable().get(p0)).toBe(3);
  });

  test("out-of-order completions — commit waits for the gap to fill", () => {
    const tracker = new OffsetTracker();
    tracker.complete(p0, 0);
    tracker.complete(p0, 2);
    tracker.complete(p0, 3);
    expect(tracker.committable().get(p0)).toBe(1);
    tracker.complete(p0, 1);
    expect(tracker.committable().get(p0)).toBe(4);
  });

  test("nothing committable when first offset is still pending", () => {
    const tracker = new OffsetTracker();
    tracker.setFrontier(p0, 0);
    tracker.complete(p0, 1);
    tracker.complete(p0, 2);
    expect(tracker.committable().size).toBe(0);
  });

  test("tracks partitions independently", () => {
    const tracker = new OffsetTracker();
    tracker.complete(p0, 0);
    tracker.complete(p0, 1);
    tracker.complete(p1, 0);
    const committable = tracker.committable();
    expect(committable.get(p0)).toBe(2);
    expect(committable.get(p1)).toBe(1);
  });

  test("committable is consumed — calling twice returns empty", () => {
    const tracker = new OffsetTracker();
    tracker.complete(p0, 0);
    tracker.complete(p0, 1);
    expect(tracker.committable().get(p0)).toBe(2);
    expect(tracker.committable().size).toBe(0);
  });

  test("new completions after commit are tracked from the new frontier", () => {
    const tracker = new OffsetTracker();
    tracker.complete(p0, 0);
    tracker.complete(p0, 1);
    tracker.committable();
    tracker.complete(p0, 2);
    tracker.complete(p0, 3);
    expect(tracker.committable().get(p0)).toBe(4);
  });

  test("pendingCount shows messages behind a gap", () => {
    const tracker = new OffsetTracker();
    tracker.complete(p0, 0);
    tracker.complete(p0, 2);
    tracker.complete(p0, 3);
    tracker.committable();
    expect(tracker.pendingCount()).toBe(2); // 2 and 3 held behind the gap at 1
  });

  test("observe seeds the frontier for non-zero resume", () => {
    const tracker = new OffsetTracker();
    tracker.observe(p0, 5000);
    tracker.complete(p0, 5000);
    tracker.complete(p0, 5001);
    expect(tracker.committable().get(p0)).toBe(5002);
  });

  test("observe lowers the frontier on rewind redelivery", () => {
    const tracker = new OffsetTracker();
    tracker.observe(p0, 10);
    tracker.complete(p0, 10);
    tracker.committable(); // frontier → 11
    // rebalance redelivers from committed offset 8
    tracker.observe(p0, 8);
    tracker.complete(p0, 8);
    tracker.complete(p0, 9);
    tracker.complete(p0, 10);
    expect(tracker.committable().get(p0)).toBe(11);
  });
});

// ── autoCommitBatchWithin ──────────────────────────────────────────

function makeEnvelope<T>(value: T, acked: T[]): Envelope<T> {
  return {
    value,
    ack: async () => {
      acked.push(value);
    },
    nack: async () => {},
    metadata: {},
  };
}

describe("autoCommitBatchWithin", () => {
  test("acks every envelope and unwraps values", async () => {
    const acked: number[] = [];
    const envelopes = [1, 2, 3, 4, 5].map((n) => makeEnvelope(n, acked));

    const out = await run(
      Stream.fromArray(envelopes).through(autoCommitBatchWithin<number>(2, 1000)).toArray() as any,
    );

    expect(out).toEqual([1, 2, 3, 4, 5]);
    expect(acked.sort()).toEqual([1, 2, 3, 4, 5]);
  });

  test("acks happen in batches of maxBatchSize", async () => {
    const ackBatches: number[][] = [];
    let current: number[] = [];
    const envelopes = [1, 2, 3, 4].map((n) => ({
      value: n,
      ack: async () => {
        current.push(n);
      },
      nack: async () => {},
      metadata: {},
    }));

    // groupWithin re-chunks; observe batch boundaries via a marker between chunks
    const stream = Stream.fromIterable(envelopes)
      .rechunk(1)
      .through(autoCommitBatchWithin<number>(2, 5000))
      .tap(() => {
        if (current.length >= 2) {
          ackBatches.push(current);
          current = [];
        }
      });

    await run(stream.drain() as any);
    if (current.length > 0) ackBatches.push(current);

    expect(ackBatches.flat().sort()).toEqual([1, 2, 3, 4]);
    expect(ackBatches[0]!.length).toBeGreaterThanOrEqual(2);
  });
});
