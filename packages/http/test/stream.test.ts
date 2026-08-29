// Streaming helpers — build Responses with controllable ReadableStreams,
// then assert on the Perfect Stream we get back.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, fail, sync, run } from "@spilne/perfect-core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  httpStreamText,
  httpStreamLines,
  httpStreamNDJSON,
  httpStreamSSE,
  type ResponseParser,
} from "../src";

/**
 * Build a `Response` whose body is a `ReadableStream` pushing the given chunks
 * (as UTF-8-encoded Uint8Array) with an optional delay between them.
 */
function streamingResponse(chunks: string[], delayMs = 0): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const chunk of chunks) {
        if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(stream, { status: 200 });
}

class OneShotTransport implements HttpTransport {
  public calls = 0;
  constructor(private readonly respond: () => Response | HttpClientError) {}
  execute(_: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      this.calls++;
      return this.respond();
    }).flatMap((r) =>
      r instanceof Response ? succeed(r) : (fail(r) as Eff<Response, Throws<HttpClientError>>),
    );
  }
}

// ── httpStreamText ─────────────────────────────────────────────────

describe("httpStreamText", () => {
  test("emits decoded chunks as they arrive", async () => {
    const transport = new OneShotTransport(() => streamingResponse(["hello ", "world", "!"]));
    const collected = await run(httpStreamText({ url: "/x", transport }).toArray());
    expect(collected.join("")).toBe("hello world!");
  });

  test("empty body closes the stream with no emits", async () => {
    const transport = new OneShotTransport(() => new Response(null));
    const collected = await run(httpStreamText({ url: "/x", transport }).toArray());
    expect(collected).toEqual([]);
  });

  test("HTTP 500 → typed HttpStatusError", async () => {
    const transport = new OneShotTransport(() => new Response("boom", { status: 500 }));
    await expect(
      run(httpStreamText({ url: "/x", transport }).toArray() as any),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 500 });
  });
});

// ── httpStreamLines ────────────────────────────────────────────────

describe("httpStreamLines", () => {
  test("splits on \\n, strips \\r, flushes trailing partial", async () => {
    // Split across chunk boundaries to stress the buffer
    const transport = new OneShotTransport(() =>
      streamingResponse(["line one\r\nline ", "two\nline three\n", "tail"]),
    );
    const lines = await run(httpStreamLines({ url: "/x", transport }).toArray());
    expect(lines).toEqual(["line one", "line two", "line three", "tail"]);
  });

  test("empty body → no lines", async () => {
    const transport = new OneShotTransport(() => new Response(""));
    const lines = await run(httpStreamLines({ url: "/x", transport }).toArray());
    expect(lines).toEqual([]);
  });
});

// ── httpStreamNDJSON ───────────────────────────────────────────────

interface User {
  id: number;
  name: string;
}
const UserParser: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d as User }
      : { success: false, error: "bad shape" },
};

describe("httpStreamNDJSON", () => {
  test("parses one user per line", async () => {
    const body = [
      `{"id":1,"name":"alice"}\n`,
      `{"id":2,"name":"bob"}\n`,
      `{"id":3,"name":"carol"}\n`,
    ];
    const transport = new OneShotTransport(() => streamingResponse(body));
    const users = await run(
      httpStreamNDJSON({ url: "/x", transport, schema: UserParser }).toArray(),
    );
    expect(users).toEqual([
      { id: 1, name: "alice" },
      { id: 2, name: "bob" },
      { id: 3, name: "carol" },
    ]);
  });

  test("skips empty lines between valid JSON", async () => {
    const transport = new OneShotTransport(() =>
      streamingResponse([`{"id":1,"name":"a"}\n\n\n{"id":2,"name":"b"}\n`]),
    );
    const users = await run(
      httpStreamNDJSON({ url: "/x", transport, schema: UserParser }).toArray(),
    );
    expect(users.length).toBe(2);
  });

  test("invalid JSON surfaces as typed HttpParseError", async () => {
    const transport = new OneShotTransport(() =>
      streamingResponse([`{"id":1,"name":"a"}\nnot json\n{"id":2,"name":"b"}\n`]),
    );
    await expect(
      run(httpStreamNDJSON({ url: "/x", transport, schema: UserParser }).toArray() as any),
    ).rejects.toMatchObject({ _tag: "HttpParseError" });
  });

  test("schema mismatch surfaces as typed HttpParseError", async () => {
    const transport = new OneShotTransport(() =>
      streamingResponse([`{"id":1,"name":"a"}\n{"wrong":"shape"}\n`]),
    );
    await expect(
      run(httpStreamNDJSON({ url: "/x", transport, schema: UserParser }).toArray() as any),
    ).rejects.toMatchObject({ _tag: "HttpParseError" });
  });
});

// ── httpStreamSSE ──────────────────────────────────────────────────

describe("httpStreamSSE", () => {
  test("parses basic message events", async () => {
    const body = ["data: hello\n", "\n", "data: world\n", "\n"];
    const transport = new OneShotTransport(() => streamingResponse(body));
    const events = await run(httpStreamSSE({ url: "/x", transport }).toArray());
    expect(events).toEqual([
      { event: "message", data: "hello", id: undefined, retry: undefined },
      { event: "message", data: "world", id: undefined, retry: undefined },
    ]);
  });

  test("multi-line data joined with \\n per spec", async () => {
    const body = ["event: msg\n", "data: line1\n", "data: line2\n", "data: line3\n", "\n"];
    const transport = new OneShotTransport(() => streamingResponse(body));
    const events = await run(httpStreamSSE({ url: "/x", transport }).toArray());
    expect(events.length).toBe(1);
    expect(events[0]!.event).toBe("msg");
    expect(events[0]!.data).toBe("line1\nline2\nline3");
  });

  test("id + retry fields populate", async () => {
    const body = ["id: 42\n", "retry: 5000\n", "event: ping\n", "data: hi\n", "\n"];
    const transport = new OneShotTransport(() => streamingResponse(body));
    const events = await run(httpStreamSSE({ url: "/x", transport }).toArray());
    expect(events[0]).toEqual({
      event: "ping",
      data: "hi",
      id: "42",
      retry: 5000,
    });
  });

  test("comment lines (starting with :) are ignored", async () => {
    const body = [": heartbeat\n", "data: real\n", "\n"];
    const transport = new OneShotTransport(() => streamingResponse(body));
    const events = await run(httpStreamSSE({ url: "/x", transport }).toArray());
    expect(events.length).toBe(1);
    expect(events[0]!.data).toBe("real");
  });

  test("flushes a final in-progress event on stream close", async () => {
    // No trailing blank line — server closed mid-event
    const body = ["data: orphan\n"];
    const transport = new OneShotTransport(() => streamingResponse(body));
    const events = await run(httpStreamSSE({ url: "/x", transport }).toArray());
    expect(events.length).toBe(1);
    expect(events[0]!.data).toBe("orphan");
  });
});

// ── Cancellation: take(n) stops early, reader is cancelled ────────

describe("take(n) terminates early", () => {
  test("httpStreamLines.take(2) stops after 2 lines", async () => {
    const transport = new OneShotTransport(() => streamingResponse(["a\nb\nc\nd\ne\n"]));
    const lines = await run(httpStreamLines({ url: "/x", transport }).take(2).toArray());
    expect(lines).toEqual(["a", "b"]);
  });
});
