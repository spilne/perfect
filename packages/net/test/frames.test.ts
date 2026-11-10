import { describe, expect, test } from "bun:test";
import { encodeFrame, FrameReader, FrameTooBig } from "../src/frames";

const bytes = (...values: number[]) => new Uint8Array(values);

describe("FrameReader", () => {
  test("reads frames however the bytes are split", () => {
    const wire = new Uint8Array([...encodeFrame(bytes(1, 2, 3)), ...encodeFrame(bytes(4))]);
    for (let split = 0; split <= wire.length; split++) {
      const reader = new FrameReader();
      const frames = [
        ...reader.push(wire.subarray(0, split)),
        ...reader.push(wire.subarray(split)),
      ];
      expect(frames.map((frame) => [...frame])).toEqual([[1, 2, 3], [4]]);
    }
  });

  test("reads a frame that arrives one byte at a time", () => {
    const reader = new FrameReader();
    const frames: Uint8Array[] = [];
    for (const byte of encodeFrame(bytes(9, 8, 7))) frames.push(...reader.push(bytes(byte)));
    expect(frames.map((frame) => [...frame])).toEqual([[9, 8, 7]]);
  });

  test("an empty message is a frame too", () => {
    expect(new FrameReader().push(encodeFrame(bytes())).map((frame) => frame.length)).toEqual([0]);
  });

  test("refuses a frame over the limit as soon as the header arrives", () => {
    const reader = new FrameReader(4);
    expect(() => reader.push(encodeFrame(bytes(1, 2, 3, 4, 5)).subarray(0, 4))).toThrow(
      FrameTooBig,
    );
  });
});

describe("FrameReader hardening", () => {
  const wire = (...payloads: Uint8Array[]) =>
    new Uint8Array(payloads.flatMap((payload) => [...encodeFrame(payload)]));
  const oversized = new Uint8Array([0, 0, 0, 9]);

  test("frames received before a bad header are still delivered", () => {
    const reader = new FrameReader(4);
    const chunk = new Uint8Array([...wire(bytes(1), bytes(2, 3)), ...oversized]);
    const frames = reader.push(chunk);
    expect(frames.map((frame) => [...frame])).toEqual([[1], [2, 3]]);
    expect(reader.failure).toBeInstanceOf(FrameTooBig);
  });

  test("once poisoned, it stays failed and delivers nothing more", () => {
    const reader = new FrameReader(4);
    expect(() => reader.push(oversized)).toThrow(FrameTooBig);
    expect(reader.failure).toBeInstanceOf(FrameTooBig);
    // Bytes that would form a valid frame must not resynchronise the stream.
    expect(() => reader.push(wire(bytes(1)))).toThrow(FrameTooBig);
    expect(() => reader.push(bytes())).toThrow(FrameTooBig);
  });

  test("a healthy reader has no failure and reports a partial frame as pending", () => {
    const reader = new FrameReader();
    expect(reader.failure).toBeUndefined();
    expect(reader.pendingBytes).toBe(0);
    reader.push(wire(bytes(1, 2, 3)).subarray(0, 5));
    expect(reader.pendingBytes).toBe(5);
    reader.push(wire(bytes(1, 2, 3)).subarray(5));
    expect(reader.pendingBytes).toBe(0);
  });

  test("a frame longer than the 32-bit header can express is refused, not wrapped", () => {
    const huge = { length: 2 ** 32 } as unknown as Uint8Array;
    expect(() => encodeFrame(huge)).toThrow(FrameTooBig);
  });

  test("limits that don't fit the header are rejected up front", () => {
    expect(() => new FrameReader(-1)).toThrow(RangeError);
    expect(() => new FrameReader(1.5)).toThrow(RangeError);
    expect(() => new FrameReader(2 ** 32)).toThrow(RangeError);
    expect(() => new FrameReader(Number.NaN)).toThrow(RangeError);
  });

  test("a big frame in tiny pieces is collected in linear time", () => {
    const payload = new Uint8Array(512 * 1024).fill(7);
    const bytesOnWire = wire(payload);
    const reader = new FrameReader();
    const started = performance.now();
    const frames: Uint8Array[] = [];
    for (const byte of bytesOnWire) frames.push(...reader.push(bytes(byte)));
    const elapsed = performance.now() - started;
    expect(frames).toHaveLength(1);
    expect(frames[0]!.length).toBe(payload.length);
    expect(frames[0]!.every((byte) => byte === 7)).toBe(true);
    expect(elapsed).toBeLessThan(2_000);
  });

  test("frames handed out do not change when later bytes arrive", () => {
    const reader = new FrameReader();
    const first = reader.push(wire(bytes(1, 2, 3)).subarray(0, 6));
    expect(first).toEqual([]);
    const [frame] = reader.push(wire(bytes(1, 2, 3)).subarray(6));
    reader.push(wire(bytes(9, 9, 9, 9, 9)));
    expect([...frame!]).toEqual([1, 2, 3]);
  });
});
