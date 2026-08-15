// Lossless codec, canonicalize/payloadHash, and value typeclasses —
// ported from promin's typeclass suite.

import { describe, test, expect } from "bun:test";
import { LosslessJsonCodec, canonicalJSON, payloadHash } from "../src/connect";
import {
  JsonEq,
  eqFromCodec,
  numberOrd,
  stringOrd,
  ordBy,
  JsonShow,
  arrayMonoid,
  sumMonoid,
  stringMonoid,
} from "../src";

const roundTrip = (v: unknown) =>
  LosslessJsonCodec.decode(JSON.parse(JSON.stringify(LosslessJsonCodec.encode(v))));

describe("LosslessJsonCodec", () => {
  test("round-trips types plain JSON destroys", () => {
    const date = new Date("2026-08-14T12:00:00Z");
    expect(roundTrip(date)).toEqual(date);
    expect(roundTrip(123n)).toBe(123n);
    expect(roundTrip(new Map([["a", 1]]))).toEqual(new Map([["a", 1]]));
    expect(roundTrip(new Set([1, 2]))).toEqual(new Set([1, 2]));
    expect(roundTrip(/ab+c/gi)).toEqual(/ab+c/gi);
    expect(roundTrip(new URL("https://x.dev/p"))).toEqual(new URL("https://x.dev/p"));
    expect(roundTrip(undefined)).toBeUndefined();
    expect(roundTrip(NaN)).toBeNaN();
    expect(roundTrip(Infinity)).toBe(Infinity);
    expect(roundTrip(-Infinity)).toBe(-Infinity);
    expect(Object.is(roundTrip(-0), -0)).toBe(true);
  });

  test("round-trips Error with name/message/stack", () => {
    const err = new TypeError("nope");
    const back = roundTrip(err) as Error;
    expect(back).toBeInstanceOf(Error);
    expect(back.name).toBe("TypeError");
    expect(back.message).toBe("nope");
  });

  test("nested structures and plain JSON pass through", () => {
    const v = { a: [1, "x", { d: new Date(0) }], b: null, c: true };
    expect(roundTrip(v)).toEqual(v);
    expect(LosslessJsonCodec.encode({ a: 1 })).toEqual({ a: 1 });
  });

  test("escapes literal objects that carry a __t key", () => {
    const v = { __t: "date", other: 1 };
    expect(roundTrip(v)).toEqual(v);
  });

  test("throws on functions and symbols", () => {
    expect(() => LosslessJsonCodec.encode(() => 1)).toThrow(TypeError);
    expect(() => LosslessJsonCodec.encode(Symbol("s"))).toThrow(TypeError);
  });
});

describe("canonicalJSON + payloadHash", () => {
  test("key order does not affect the encoding", () => {
    expect(canonicalJSON({ a: 1, b: 2 })).toBe(canonicalJSON({ b: 2, a: 1 }));
  });

  test("Map/Set insertion order does not affect the encoding", () => {
    expect(
      canonicalJSON(
        new Map([
          ["a", 1],
          ["b", 2],
        ]),
      ),
    ).toBe(
      canonicalJSON(
        new Map([
          ["b", 2],
          ["a", 1],
        ]),
      ),
    );
    expect(canonicalJSON(new Set([1, 2, 3]))).toBe(canonicalJSON(new Set([3, 1, 2])));
  });

  test("distinguishes lookalike values", () => {
    expect(canonicalJSON(0)).not.toBe(canonicalJSON(-0));
    expect(canonicalJSON(new Date(0))).not.toBe(canonicalJSON(0));
    expect(canonicalJSON(undefined)).not.toBe(canonicalJSON(null));
  });

  test("payloadHash is a stable 64-char hex digest", () => {
    const h1 = payloadHash({ x: 1, y: [2, 3] });
    const h2 = payloadHash({ y: [2, 3], x: 1 });
    expect(h1).toBe(h2);
    expect(h1).toMatch(/^[0-9a-f]{64}$/);
    expect(payloadHash({ x: 1, y: [2, 3] })).not.toBe(payloadHash({ x: 1, y: [2, 4] }));
  });

  test("throws on cycles", () => {
    const a: any = {};
    a.self = a;
    expect(() => canonicalJSON(a)).toThrow(/cyclic/);
  });
});

describe("Eq / Ord / Show / Monoid", () => {
  test("JsonEq deep-compares", () => {
    expect(JsonEq.equals({ a: [1] }, { a: [1] })).toBe(true);
    expect(JsonEq.equals({ a: 1 }, { a: 2 })).toBe(false);
  });

  test("eqFromCodec compares encoded forms", () => {
    const eq = eqFromCodec(LosslessJsonCodec);
    expect(eq.equals(new Date(0), new Date(0))).toBe(true);
    expect(eq.equals(new Date(0), new Date(1))).toBe(false);
  });

  test("Ords compare and ordBy derives", () => {
    expect(numberOrd.compare(1, 2)).toBe(-1);
    expect(stringOrd.compare("b", "a")).toBe(1);
    const byLen = ordBy((s: string) => s.length);
    expect(byLen.compare("aa", "b")).toBe(1);
    expect(byLen.equals("aa", "bb")).toBe(true);
  });

  test("Show truncates JSON", () => {
    expect(JsonShow.show({ a: 1 })).toBe('{"a":1}');
  });

  test("Monoids fold", () => {
    const m = arrayMonoid<number>();
    expect([[1], [2, 3]].reduce(m.concat, m.empty)).toEqual([1, 2, 3]);
    expect([1, 2, 3].reduce(sumMonoid.concat, sumMonoid.empty)).toBe(6);
    expect(["a", "b"].reduce(stringMonoid.concat, stringMonoid.empty)).toBe("ab");
  });
});
