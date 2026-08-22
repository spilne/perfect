// Regression: the transport used to abort its AbortController as soon as the
// fetch effect yielded a Response — i.e. when the HEADERS arrived. Every
// caller that reads the body afterwards (httpRequestJson, httpRequestText,
// client.get, …) was racing that abort, and only won when the body was small
// enough to be buffered already. A body that arrives even slightly late failed
// with HttpParseError { cause: AbortError }.

import { afterAll, describe, expect, test } from "bun:test";
import { Cause, run, runExit } from "@perfect/core";
import { DefaultHttpClient, httpRequestJson, httpRequestText, identityParser } from "../src";

const payload = JSON.stringify({ data: "x".repeat(200_000) });

/** Sends 100 bytes, pauses, then the rest — the body cannot be pre-buffered. */
const server = Bun.serve({
  port: 0,
  fetch(req) {
    const url = new URL(req.url);
    const delayMs = Number(url.searchParams.get("delay") ?? "20");
    const stream = new ReadableStream({
      async start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode(payload.slice(0, 100)));
        await Bun.sleep(delayMs);
        controller.enqueue(encoder.encode(payload.slice(100)));
        controller.close();
      },
    });
    return new Response(stream, { headers: { "content-type": "application/json" } });
  },
});

const base = `http://localhost:${server.port}`;

afterAll(() => {
  server.stop(true);
});

describe("transport keeps the response body readable after the fetch effect", () => {
  test("httpRequestJson reads a slow body in full", async () => {
    const result = (await run(httpRequestJson({ url: `${base}/?delay=25` }) as any)) as {
      data: string;
    };
    expect(result.data).toHaveLength(200_000);
  });

  test("httpRequestText reads a slow body in full", async () => {
    const text = await run(httpRequestText({ url: `${base}/?delay=25` }) as any);
    expect((text as string).length).toBe(payload.length);
  });

  test("DefaultHttpClient.get reads a slow body in full", async () => {
    const client = new DefaultHttpClient({ baseUrl: base });
    const result = (await run(client.get("/?delay=25", identityParser) as any)) as {
      data: string;
    };
    expect(result.data).toHaveLength(200_000);
  });

  test("a longer stall still succeeds — no hidden timing dependence", async () => {
    const result = (await run(httpRequestJson({ url: `${base}/?delay=150` }) as any)) as {
      data: string;
    };
    expect(result.data).toHaveLength(200_000);
  });

  test("the request timeout still fires while the body is in flight", async () => {
    // The guard must not disable timeouts: AbortSignal.timeout covers the whole
    // exchange, so a body slower than the budget is still a failure.
    const exit = await runExit(
      httpRequestJson({ url: `${base}/?delay=400`, timeoutMs: 60 }) as any,
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const error = Cause.firstFail(exit.cause)?.value as { _tag?: string };
      expect(["HttpTimeoutError", "HttpParseError", "HttpNetworkError"]).toContain(error?._tag);
    }
  });
});
