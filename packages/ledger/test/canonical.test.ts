import { describe, expect, it } from "bun:test";
import { decodeCanonical, encodeCanonical, toHex, type CanonicalValue } from "../src";

describe("canonical encoding", () => {
  it("encodes the same data identically whatever the key order", () => {
    const a = encodeCanonical({ b: 1, a: [true, null], c: { y: "x", x: "y" } });
    const b = encodeCanonical({ c: { x: "y", y: "x" }, a: [true, null], b: 1 });
    expect(toHex(a)).toBe(toHex(b));
  });

  it("round-trips every supported type, including big integers and bytes", () => {
    const value: CanonicalValue = {
      nothing: null,
      yes: true,
      no: false,
      small: -7,
      zero: 0,
      big: 2n ** 80n,
      negBig: -(2n ** 70n),
      text: "héllo \u{1F600}",
      data: Uint8Array.of(0, 1, 255),
      list: [1, [2, [3]], {}],
    };
    expect(decodeCanonical(encodeCanonical(value))).toEqual(value);
  });

  it("gives different bytes to values that differ only in type", () => {
    const encodings = [1, "1", 1n === 1n ? true : false, null, [1], { 1: 1 }, Uint8Array.of(1)].map(
      (v) => toHex(encodeCanonical(v as CanonicalValue)),
    );
    expect(new Set(encodings).size).toBe(encodings.length);
  });

  it("orders keys by their UTF-8 bytes", () => {
    const bytes = encodeCanonical({ b: 0, a: 0, é: 0, z: 0 });
    expect(Object.keys(decodeCanonical(bytes) as object)).toEqual(["a", "b", "z", "é"]);
  });

  it("rejects values it cannot encode canonically", () => {
    expect(() => encodeCanonical(1.5)).toThrow(TypeError);
    expect(() => encodeCanonical(Number.NaN)).toThrow(TypeError);
    expect(() => encodeCanonical(undefined as unknown as CanonicalValue)).toThrow(TypeError);
    expect(() => encodeCanonical(new Date() as unknown as CanonicalValue)).toThrow(TypeError);
    expect(() => encodeCanonical(Number.MAX_SAFE_INTEGER + 2)).toThrow(TypeError);
    let deep: CanonicalValue = 0;
    for (let i = 0; i < 100; i++) deep = [deep];
    expect(() => encodeCanonical(deep)).toThrow(RangeError);
  });

  it("decodes only canonical input", () => {
    const ok = encodeCanonical({ a: 1, b: 2 });
    expect(() => decodeCanonical(Uint8Array.of(...ok, 0))).toThrow(RangeError);
    expect(() => decodeCanonical(ok.slice(0, -1))).toThrow(RangeError);
    // 0x03 integer written with a non-minimal varint (0x80 0x00 instead of 0x00)
    expect(() => decodeCanonical(Uint8Array.of(0x03, 0x80, 0x00))).toThrow(RangeError);
    // record with keys out of order, then with a repeated key
    const k = (s: string) => [0x04, s.length, ...new TextEncoder().encode(s)];
    expect(() => decodeCanonical(Uint8Array.of(0x07, 2, ...k("b"), 0, ...k("a"), 0))).toThrow(
      RangeError,
    );
    expect(() => decodeCanonical(Uint8Array.of(0x07, 2, ...k("a"), 0, ...k("a"), 0))).toThrow(
      RangeError,
    );
    expect(() => decodeCanonical(Uint8Array.of(0x09))).toThrow(RangeError);
    expect(() => decodeCanonical(Uint8Array.of(0x05, 5, 1))).toThrow(RangeError);
    expect(() => decodeCanonical(Uint8Array.of(0x04, 1, 0xff))).toThrow();
    expect(() => decodeCanonical(new Uint8Array(0))).toThrow(RangeError);
  });

  it("keeps a __proto__ key as data", () => {
    const value = JSON.parse('{"__proto__": 1, "a": 2}') as CanonicalValue;
    const back = decodeCanonical(encodeCanonical(value)) as Record<string, unknown>;
    expect(Object.getOwnPropertyDescriptor(back, "__proto__")?.value).toBe(1);
    expect(back["a"]).toBe(2);
  });
});
