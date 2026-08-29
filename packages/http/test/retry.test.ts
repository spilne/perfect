// withRetryAll + RetryAttempt tests.

import { describe, test, expect } from "bun:test";
import { RetryPolicy, type Eff, type Throws, succeed, fail, sync, run } from "@spilne/perfect-core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  HttpNetworkError,
  HttpStatusError,
  HTTP_RETRYABLE,
  withRetryAll,
  withRetryAllBy,
  retryHttp,
  Retry,
  RetryAttempt,
  RetryDecision,
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

describe("withRetryAll — full outcome ADT", () => {
  test("retries failures by default", async () => {
    const t = new ScriptedTransport([
      new Response("slow", { status: 503 }),
      new Response("slow", { status: 503 }),
      new Response("ok"),
    ]);
    const result = await run(
      withRetryAll(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }),
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
        withRetryAll(httpRequestText({ url: "/x", transport: t }), {
          maxRetries: 2,
          baseDelayMs: 1,
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 503 });
    expect(t.calls).toBe(3); // 1 initial + 2 retries
  });

  test("can stop on specific HTTP status", async () => {
    const t = new ScriptedTransport([new Response("", { status: 404 }), new Response("ok")]);
    await expect(
      run(
        withRetryAll(httpRequestText({ url: "/x", transport: t }), {
          baseDelayMs: 1,
          shouldRetry: (r) => {
            if (RetryAttempt.isHttpError(r)) {
              return HTTP_RETRYABLE(r.error);
            }
            return true;
          },
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 404 });
    expect(t.calls).toBe(1);
  });

  test("retries network errors", async () => {
    const t = new ScriptedTransport([
      new HttpNetworkError({ url: "/x", cause: null, message: "down" }),
      new Response("ok"),
    ]);
    const result = await run(
      withRetryAll(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }),
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(2);
  });

  test("policy override is honored", async () => {
    const t = new ScriptedTransport([new Response("", { status: 503 }), new Response("ok")]);
    await expect(
      run(
        withRetryAll(httpRequestText({ url: "/x", transport: t }), {
          policy: RetryPolicy.recurs(0),
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 503 });
    expect(t.calls).toBe(1);
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
        shouldRetry: (r) => (RetryAttempt.isSuccess(r) ? r.value.status !== "done" : true),
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

  test("RetryAttempt type guards", () => {
    const s = RetryAttempt.success(42);
    const h = RetryAttempt.httpError(
      new HttpStatusError({ url: "/x", status: 503, body: "", message: "" }),
    );
    const e = RetryAttempt.thrown(new Error("x"));
    expect(RetryAttempt.isSuccess(s)).toBe(true);
    expect(RetryAttempt.isHttpError(h)).toBe(true);
    expect(RetryAttempt.isThrown(e)).toBe(true);
    expect(RetryAttempt.isSuccess(h)).toBe(false);
  });

  test("withRetryAll uses custom retry policy", async () => {
    const t = new ScriptedTransport([
      new Response("", { status: 503 }),
      new Response("", { status: 503 }),
    ]);
    await expect(
      run(
        withRetryAll(httpRequestText({ url: "/x", transport: t }), {
          policy: RetryPolicy.recurs(1),
          shouldRetry: (r) => RetryAttempt.isHttpError(r) && HTTP_RETRYABLE(r.error),
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 503 });
    expect(t.calls).toBe(2);
  });

  test("retryHttp uses HTTP_RETRYABLE typed defaults", async () => {
    const t = new ScriptedTransport([new Response("", { status: 503 }), new Response("ok")]);
    const result = await run(
      retryHttp(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }),
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(2);
  });

  test("Retry.http namespace helper uses HTTP retry defaults", async () => {
    const t = new ScriptedTransport([new Response("", { status: 503 }), new Response("ok")]);
    const result = await run(
      Retry.http(httpRequestText({ url: "/x", transport: t }), { baseDelayMs: 1 }) as any,
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(2);
  });
});

describe("withRetryAllBy — handler style", () => {
  test("retries on pending success via handler", async () => {
    const t = new ScriptedTransport([
      new Response(JSON.stringify({ state: "pending" }), {
        headers: { "content-type": "application/json" },
      }),
      new Response(JSON.stringify({ state: "done" }), {
        headers: { "content-type": "application/json" },
      }),
    ]);
    const eff = (httpRequestText({ url: "/x", transport: t }) as any).map(
      (s: string) => JSON.parse(s) as { state: string },
    );

    const result = await run(
      withRetryAllBy(eff, {
        baseDelayMs: 1,
        handle: (r) =>
          RetryAttempt.isSuccess(r) && r.value.state === "pending"
            ? RetryDecision.retry()
            : RetryDecision.stop(),
      }) as any,
    );
    expect(result).toEqual({ state: "done" });
    expect(t.calls).toBe(2);
  });

  test("can stop on retryable HTTP errors", async () => {
    const t = new ScriptedTransport([new Response("", { status: 503 }), new Response("ok")]);
    await expect(
      run(
        withRetryAllBy(httpRequestText({ url: "/x", transport: t }), {
          baseDelayMs: 1,
          policy: RetryPolicy.recurs(0),
          handle: (r) =>
            RetryAttempt.isHttpError(r) ? RetryDecision.retry() : RetryDecision.stop(),
        }) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 503 });
    expect(t.calls).toBe(1);
  });
});
