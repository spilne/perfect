// withRetry + withRetryAll + PipelineResult tests.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, fail, sync, run } from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  HttpNetworkError,
  HttpStatusError,
  withRetry,
  withRetryAll,
  PipelineResult,
  httpRequestText,
} from "../src";

class ScriptedTransport implements HttpTransport {
  public calls = 0;
  constructor(private readonly script: Array<Response | HttpClientError>) {}
  execute(_: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      const next = this.script[this.calls++];
      if (next === undefined) throw new Error("script exhausted");
      return next;
    }).flatMap((r) => (r instanceof Response ? succeed(r) : (fail(r) as any)));
  }
}

describe("withRetry — HTTP-aware transient retry", () => {
  test("retries 503, then 200", async () => {
    const t = new ScriptedTransport([
      new Response("slow", { status: 503 }),
      new Response("slow", { status: 503 }),
      new Response("ok"),
    ]);
    const result = await run(
      withRetry(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }),
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(3);
  });

  test("gives up after maxRetries", async () => {
    const t = new ScriptedTransport([
      new Response("", { status: 503 }),
      new Response("", { status: 503 }),
      new Response("", { status: 503 }),
    ]);
    await expect(
      run(
        withRetry(httpRequestText({ url: "/x", transport: t }), {
          maxRetries: 2,
          baseDelayMs: 1,
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 503 });
    expect(t.calls).toBe(3); // 1 initial + 2 retries
  });

  test("does NOT retry 404 (caller bug)", async () => {
    const t = new ScriptedTransport([new Response("", { status: 404 })]);
    await expect(
      run(withRetry(httpRequestText({ url: "/x", transport: t })) as any),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 404 });
    expect(t.calls).toBe(1);
  });

  test("retries network errors", async () => {
    const t = new ScriptedTransport([
      new HttpNetworkError({ url: "/x", cause: null, message: "down" }),
      new Response("ok"),
    ]);
    const result = await run(
      withRetry(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }),
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(2);
  });

  test("custom `when` predicate overrides default", async () => {
    const t = new ScriptedTransport([new Response("", { status: 404 }), new Response("ok")]);
    const result = await run(
      withRetry(httpRequestText({ url: "/x", transport: t }), {
        baseDelayMs: 1,
        when: (e) => e._tag === "HttpStatusError" && e.status === 404,
      }),
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(2);
  });
});

describe("withRetryAll — full outcome ADT", () => {
  test("default: retries errors, not success", async () => {
    const t = new ScriptedTransport([new Response("", { status: 503 }), new Response("ok")]);
    const result = await run(
      withRetryAll(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }),
    );
    expect(result).toBe("ok");
  });

  test("retry on unsatisfactory success — the poll-for-completion pattern", async () => {
    const t = new ScriptedTransport([
      new Response(JSON.stringify({ status: "pending" }), {
        headers: { "content-type": "application/json" },
      }),
      new Response(JSON.stringify({ status: "pending" }), {
        headers: { "content-type": "application/json" },
      }),
      new Response(JSON.stringify({ status: "done" }), {
        headers: { "content-type": "application/json" },
      }),
    ]);

    // Use httpRequestText + JSON.parse to keep things simple here
    const eff = (httpRequestText({ url: "/x", transport: t }) as any).map(
      (s: string) => JSON.parse(s) as { status: string },
    );

    const result = await run(
      withRetryAll(eff, {
        baseDelayMs: 1,
        shouldRetry: (r) => (PipelineResult.isSuccess(r) ? r.value.status !== "done" : true),
      }) as any,
    );
    expect(result).toEqual({ status: "done" });
    expect(t.calls).toBe(3);
  });

  test("unwraps thrown defects back to defects", async () => {
    let calls = 0;
    const bug = sync(() => {
      calls++;
      throw new Error("bug");
    }) as any;
    await expect(
      run(
        withRetryAll(bug, {
          baseDelayMs: 1,
          maxRetries: 2,
          // retry thrown as well
          shouldRetry: (r) => r._tag !== "success",
        }) as any,
      ),
    ).rejects.toBeInstanceOf(Error);
    expect(calls).toBe(3); // 1 + 2 retries
  });

  test("PipelineResult type guards", () => {
    const s = PipelineResult.success(42);
    const h = PipelineResult.httpError(
      new HttpStatusError({ url: "/x", status: 503, body: "", message: "" }),
    );
    const e = PipelineResult.thrown(new Error("x"));
    expect(PipelineResult.isSuccess(s)).toBe(true);
    expect(PipelineResult.isHttpError(h)).toBe(true);
    expect(PipelineResult.isThrown(e)).toBe(true);
    expect(PipelineResult.isSuccess(h)).toBe(false);
  });
});
