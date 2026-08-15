// Encoding pipes: utf8Decode / utf8Encode / base64Encode / base64Decode.
//
// utf8Decode shares one TextDecoder with { stream: true }, so multi-byte
// codepoints split across chunks decode correctly. base64Encode/Decode are
// per-chunk btoa/atob maps over strings — NOT streaming encoders: each chunk
// is encoded independently, so a multi-chunk encode is not the same as
// encoding the concatenated input.

import { describe, test, expect } from "bun:test";
import { run, runExit, Stream, Pipes, Cause } from "../src";

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
  test("round-trips an ASCII string", async () => {
    const encoded = await run((Stream.succeed("hello") as any).through(Pipes.base64Encode).toArray());
    expect(encoded).toEqual(["aGVsbG8="]);

    const decoded = await run(
      (Stream.succeed("hello") as any)
        .through(Pipes.base64Encode)
        .through(Pipes.base64Decode)
        .toArray(),
    );
    expect(decoded.join("")).toBe("hello");
  });

  test("round-trips arbitrary bytes represented as a latin1 binary string", async () => {
    const bytes = new Uint8Array(256);
    for (let i = 0; i < 256; i++) bytes[i] = i;
    const binary = String.fromCharCode(...bytes);

    const decoded: string[] = await run(
      (Stream.succeed(binary) as any)
        .through(Pipes.base64Encode)
        .through(Pipes.base64Decode)
        .toArray(),
    );
    const out = decoded.join("");
    expect(out.length).toBe(256);
    for (let i = 0; i < 256; i++) expect(out.charCodeAt(i)).toBe(i);
  });

  test("encodes each chunk independently (per-chunk map, not a streaming encoder)", async () => {
    // NOTE: base64Encode is `input.map(btoa)` — each chunk gets its own
    // padding, so ["he","llo"] does NOT encode to btoa("hello"). Decoding
    // chunk-wise still round-trips.
    const encoded: string[] = await run(
      (Stream.of("he", "llo") as any).through(Pipes.base64Encode).toArray(),
    );
    expect(encoded).toEqual([btoa("he"), btoa("llo")]);
    expect(encoded.join("")).not.toBe(btoa("hello"));

    const decoded: string[] = await run(
      (Stream.of("he", "llo") as any)
        .through(Pipes.base64Encode)
        .through(Pipes.base64Decode)
        .toArray(),
    );
    expect(decoded.join("")).toBe("hello");
  });

  test("base64Encode fails with a Die cause on non-latin1 input (btoa limitation)", async () => {
    // btoa throws on codepoints > 0xFF; the throw happens inside a `.map`
    // callback and surfaces as a defect (Die), not an interpreter crash
    const exit = await runExit(
      (Stream.succeed("🚀") as any).through(Pipes.base64Encode).toArray(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasDie(exit.cause)).toBe(true);
    }
  });
});
