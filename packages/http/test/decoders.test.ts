import { describe, test, expect } from "bun:test";
import { binaryDecoder, textDecoder, jsonDecoder, arrayBufferDecoder, blobDecoder } from "../src";

describe("response decoders", () => {
  test("textDecoder reads body as UTF-8 string", async () => {
    const r = new Response("hello");
    expect(await textDecoder(r)).toBe("hello");
  });

  test("jsonDecoder parses body", async () => {
    const r = new Response(JSON.stringify({ x: 1 }), {
      headers: { "content-type": "application/json" },
    });
    expect(await jsonDecoder(r)).toEqual({ x: 1 });
  });

  test("arrayBufferDecoder returns ArrayBuffer", async () => {
    const r = new Response("abc");
    const buf = await arrayBufferDecoder(r);
    expect(buf.byteLength).toBe(3);
  });

  test("blobDecoder returns Blob", async () => {
    const r = new Response("xyz");
    const blob = await blobDecoder(r);
    expect(blob.size).toBe(3);
  });

  test("binaryDecoder returns the underlying body stream", async () => {
    const r = new Response("stream");
    const stream = await binaryDecoder(r);
    expect(stream).toBeDefined();
    // Consume the stream to verify it's real
    const reader = stream.getReader();
    const { value } = await reader.read();
    expect(value).toBeDefined();
  });
});
