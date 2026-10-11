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
