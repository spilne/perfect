import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { StringValueSchema, TimestampSchema } from "@bufbuild/protobuf/wkt";
import { BinaryCodec } from "../src";
import { msgpackCodec } from "../src/msgpack";
import { protobufCodec } from "../src/protobuf";

const roundTrip = <T>(codec: BinaryCodec<T>, message: T): T => codec.decode(codec.encode(message));

describe("codecs", () => {
  test("bytes, UTF-8 text and JSON", () => {
    expect([...roundTrip(BinaryCodec.bytes, new Uint8Array([1, 2]))]).toEqual([1, 2]);
    expect(roundTrip(BinaryCodec.utf8, "héllo")).toBe("héllo");
    expect(roundTrip(BinaryCodec.json<{ a: number[] }>(), { a: [1, 2] })).toEqual({ a: [1, 2] });
  });

  test("UTF-8 refuses bytes that aren't text", () => {
    expect(() => BinaryCodec.utf8.decode(new Uint8Array([0xff, 0xfe]))).toThrow();
  });

  test("MessagePack keeps Dates and bytes", () => {
    const codec = msgpackCodec<{ at: Date; data: Uint8Array }>();
    const message = { at: new Date(1_000), data: new Uint8Array([7, 8]) };
    const decoded = roundTrip(codec, message);
    expect(decoded.at).toEqual(message.at);
    expect([...decoded.data]).toEqual([7, 8]);
  });

  test("Protobuf, from a generated schema", () => {
    const text = protobufCodec(StringValueSchema);
    expect(roundTrip(text, create(StringValueSchema, { value: "hi" })).value).toBe("hi");
    const time = protobufCodec(TimestampSchema);
    const decoded = roundTrip(time, create(TimestampSchema, { seconds: 42n, nanos: 5 }));
    expect([decoded.seconds, decoded.nanos]).toEqual([42n, 5]);
  });
});
