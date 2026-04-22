// Streaming — httpStream + composable Pipes (text / lines / NDJSON / SSE).
//
// Run: bun packages/http/examples/06-streaming.ts

import {
  type Eff,
  type Throws,
  succeed,
  run,
} from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  httpStreamLines,
  httpStreamSSE,
} from "../src";
import { assertEq } from "./_assert";

class StubTransport implements HttpTransport {
  constructor(private readonly reply: () => Response) {}
  execute(_o: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return succeed(this.reply());
  }
}

const streamOf = (chunks: string[]): Response => {
  const body = new ReadableStream({
    start(controller) {
      const enc = new TextEncoder();
      for (const c of chunks) controller.enqueue(enc.encode(c));
      controller.close();
    },
  });
  return new Response(body, { status: 200 });
};

// >>> example: stream-lines
// httpStreamLines = bytes → utf8Decode → lines. Every emitted item is one
// complete line (without the terminator).
const linesT = new StubTransport(() => streamOf(["alpha\nbe", "ta\ngamma\n"]));
const lines = await run(
  httpStreamLines({ url: "/log", transport: linesT }).toArray(),
);
assertEq(lines, ["alpha", "beta", "gamma"]);
// <<< example

// >>> example: stream-sse
// httpStreamSSE = lines → parseSSE. Server-Sent Events are emitted as
// SSEvent objects with { event, data, id?, retry? }.
const sseT = new StubTransport(
  () =>
    streamOf([
      "event: tick\ndata: 1\n\n",
      "event: tick\ndata: 2\nid: m-2\n\n",
    ]),
);
const events = await run(
  httpStreamSSE({ url: "/events", transport: sseT }).toArray(),
);
assertEq(events.length, 2);
assertEq(events[0]!.event, "tick");
assertEq(events[0]!.data, "1");
assertEq(events[1]!.id, "m-2");
// <<< example
