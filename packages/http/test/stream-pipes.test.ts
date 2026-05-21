// httpStream + ad-hoc Pipe composition — proves the 4 named helpers are
// thin wrappers the user can swap for any Stream pipeline.

import { describe, test, expect } from "bun:test";
import {
  type Eff,
  type Throws,
  succeed,
  fail,
  sync,
  run,
  Pipes,
} from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  httpStream,
  httpStreamText,
  httpStreamLines,
  parseSSE,
  parseNDJSON,
  type ResponseParser,
} from "../src";

function streamingResponse(chunks: string[]): Response {
  const enc = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    async start(c) {
      for (const ch of chunks) c.enqueue(enc.encode(ch));
      c.close();
    },
  });
  return new Response(body, { status: 200 });
}

class OneShot implements HttpTransport {
  constructor(private readonly reply: () => Response | HttpClientError) {}
  execute(_: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(this.reply).flatMap((r) =>
      r instanceof Response ? succeed(r) : (fail(r) as Eff<Response, Throws<HttpClientError>>),
    );
  }
}

describe("httpStream + ad-hoc pipe composition", () => {
  test("httpStream gives raw Uint8Array chunks", async () => {
    const transport = new OneShot(() => streamingResponse(["hello ", "world"]));
    const bytes = await run(httpStream({ url: "/x", transport }).toArray());
    const combined = new Uint8Array(bytes.reduce((n, b) => n + b.length, 0));
    let off = 0;
    for (const b of bytes) {
      combined.set(b, off);
      off += b.length;
    }
    expect(new TextDecoder().decode(combined)).toBe("hello world");
  });

  test("named helpers are equivalent to raw pipe composition", async () => {
    const make = () => new OneShot(() => streamingResponse(["a\nb\nc\n"]));

    // Named helper
    const viaHelper = await run(
      httpStreamLines({ url: "/x", transport: make() }).toArray(),
    );

    // Same pipeline, hand-composed
    const viaPipes = await run(
      httpStream({ url: "/x", transport: make() })
        .through(Pipes.utf8Decode)
        .through(Pipes.lines)
        .toArray(),
    );

    expect(viaHelper).toEqual(viaPipes);
    expect(viaHelper).toEqual(["a", "b", "c"]);
  });

  test("ad-hoc: httpStream → utf8Decode → parseSSE (skip the `lines` step for comments...)", async () => {
    // Real user path: want SSE on a stream that already uses \r\n
    const transport = new OneShot(() =>
      streamingResponse(["event: ping\r\n", "data: hi\r\n\r\n"]),
    );
    const events = await run(
      httpStream({ url: "/x", transport })
        .through(Pipes.utf8Decode)
        .through(Pipes.lines)
        .through(parseSSE)
        .toArray(),
    );
    expect(events.length).toBe(1);
    expect(events[0]).toMatchObject({ event: "ping", data: "hi" });
  });

  test("ad-hoc: NDJSON pipe used standalone on an arbitrary line stream", async () => {
    interface Row { n: number }
    const parser: ResponseParser<Row> = {
      safeParse: (d: any) =>
        typeof d?.n === "number" ? { success: true, data: d } : { success: false, error: "x" },
    };
    const transport = new OneShot(() =>
      streamingResponse([`{"n":1}\n{"n":2}\n\n{"n":3}\n`]),
    );
    const rows = await run(
      httpStream({ url: "/y", transport })
        .through(Pipes.utf8Decode)
        .through(Pipes.lines)
        .through(parseNDJSON(parser))
        .toArray(),
    );
    expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
  });

  test("take(n) short-circuits through the whole pipe chain", async () => {
    const transport = new OneShot(() =>
      streamingResponse(["a\n", "b\n", "c\n", "d\n", "e\n"]),
    );
    const first2 = await run(
      httpStreamText({ url: "/x", transport })
        .through(Pipes.lines)
        .take(2)
        .toArray(),
    );
    expect(first2).toEqual(["a", "b"]);
  });
});
