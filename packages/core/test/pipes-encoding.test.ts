import { describe, test, expect } from "bun:test";
import { run, Stream, Pipes } from "../src";

describe("utf8Encode / utf8Decode", () => {
  test("round-trips a string with multi-byte characters", async () => {
    const original = "héllo wörld — 🚀 日本語";
    const parts = await run(
      (Stream.succeed(original) as any)
        .through(Pipes.utf8Encode)
        .through(Pipes.utf8Decode)
        .toArray(),
    );
    expect(parts.join("")).toBe(original);
  });

  test("utf8Encode produces the expected UTF-8 bytes per chunk", async () => {
    const chunks: Uint8Array[] = await run(
      (Stream.of("hi", "é") as any).through(Pipes.utf8Encode).toArray(),
    );
    expect(chunks).toHaveLength(2);
    expect(Array.from(chunks[0]!)).toEqual([0x68, 0x69]);
    expect(Array.from(chunks[1]!)).toEqual([0xc3, 0xa9]); // é = C3 A9
  });

  test("utf8Decode handles a codepoint split across two chunks", async () => {
    // 🚀 is F0 9F 9A 80 — split in the middle of the sequence
    const a = new Uint8Array([0xf0, 0x9f]);
    const b = new Uint8Array([0x9a, 0x80]);
    const parts = await run((Stream.of(a, b) as any).through(Pipes.utf8Decode).toArray());
    expect(parts.join("")).toBe("🚀");
  });

  test("utf8Decode emits a replacement character for a trailing incomplete sequence", async () => {
    // Only the first 2 bytes of 🚀 — the flush step (decode() without
    // { stream: true }) turns the dangling prefix into U+FFFD.
    const parts = await run(
      (Stream.succeed(new Uint8Array([0xf0, 0x9f])) as any).through(Pipes.utf8Decode).toArray(),
    );
    expect(parts.join("")).toBe("�");
  });

  test("round-trips multi-chunk input", async () => {
    const parts = await run(
      (Stream.of("ab", "cd", "é🚀") as any)
        .through(Pipes.utf8Encode)
        .through(Pipes.utf8Decode)
        .toArray(),
    );
    expect(parts.join("")).toBe("abcdé🚀");
  });
});

describe("base64Encode / base64Decode", () => {
  test("round-trips arbitrary binary bytes", async () => {
    const bytes = new Uint8Array(256);
    for (let index = 0; index < bytes.length; index++) bytes[index] = index;

    const encoded = await run(Stream.succeed(bytes).through(Pipes.base64Encode).toArray());
    const decoded = await run(Stream.fromArray(encoded).through(Pipes.base64Decode).toArray());

    expect(decoded).toHaveLength(1);
    expect(Array.from(decoded[0]!)).toEqual(Array.from(bytes));
  });

  test("uses the standard base64 representation", async () => {
    const encoded = await run(
      Stream.succeed(new TextEncoder().encode("hello")).through(Pipes.base64Encode).toArray(),
    );
    expect(encoded).toEqual(["aGVsbG8="]);
  });

  test("encodes each binary chunk independently", async () => {
    const chunks = [new TextEncoder().encode("he"), new TextEncoder().encode("llo")];
    const encoded = await run(Stream.fromArray(chunks).through(Pipes.base64Encode).toArray());
    expect(encoded).toEqual(["aGU=", "bGxv"]);

    const decoded = await run(Stream.fromArray(encoded).through(Pipes.base64Decode).toArray());
    expect(new TextDecoder().decode(concatBytes(decoded))).toBe("hello");
  });

  test("text helpers round-trip Unicode through UTF-8", async () => {
    const decoded = await run(
      Stream.of("héllo ", "🚀 日本語")
        .through(Pipes.base64EncodeText)
        .through(Pipes.base64DecodeText)
        .toArray(),
    );
    expect(decoded.join("")).toBe("héllo 🚀 日本語");
  });
});

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const length = chunks.reduce((total, chunk) => total + chunk.length, 0);
  const output = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.length;
  }
  return output;
}
