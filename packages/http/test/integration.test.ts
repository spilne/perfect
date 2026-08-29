// Integration: verify the fetch effects compose cleanly with Perfect's
// fluent API (.retry, .catch, .catchTag, .timeout, etc).

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, fail, sync, run, RetryPolicy } from "@spilne/perfect-core";
import {
  type HttpTransport,
  type HttpRequestOptions,
  type HttpClientError,
  HttpStatusError,
  HttpNetworkError,
  httpFetchOk,
  httpRequestText,
  HTTP_RETRYABLE,
} from "../src";

/**
 * Transport whose response sequence can be scripted per call.
 * The "pick next response" step is inside a sync() block so retries re-read
 * the script rather than replaying the first item.
 */
class ScriptedTransport implements HttpTransport {
  public calls = 0;
  constructor(private readonly script: Array<Response | HttpClientError>) {}
  execute(_: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      const next = this.script[this.calls++];
      if (next === undefined) throw new Error("ScriptedTransport: script exhausted");
      return next;
    }).flatMap((next) =>
      next instanceof Response
        ? succeed(next)
        : (fail(next) as Eff<Response, Throws<HttpClientError>>),
    );
  }
}

describe(".retry with HTTP_RETRYABLE", () => {
  test("retries 503, then succeeds on 200", async () => {
    const t = new ScriptedTransport([
      new Response("slow", { status: 503 }),
      new Response("slow", { status: 503 }),
      new Response("ok"),
    ]);
    const result = await run(
      httpRequestText({ url: "/x", transport: t }).retry(
        RetryPolicy.recurs(5).whenError((e: HttpClientError) => HTTP_RETRYABLE(e)),
      ),
    );
    expect(result).toBe("ok");
    expect(t.calls).toBe(3);
  });

  test("does NOT retry 404", async () => {
    const t = new ScriptedTransport([new Response("nope", { status: 404 })]);
    await expect(
      run(
        httpRequestText({ url: "/x", transport: t }).retry(
          RetryPolicy.recurs(5).whenError((e: HttpClientError) => HTTP_RETRYABLE(e)),
        ) as any,
      ),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 404 });
    expect(t.calls).toBe(1);
  });
});

describe(".catchTag for targeted recovery", () => {
  test("catch HttpNetworkError, let HttpStatusError through", async () => {
    const t = new ScriptedTransport([
      new HttpNetworkError({ url: "/x", cause: null, message: "down" }),
    ]);
    const recovered = await run(
      httpFetchOk({ url: "/x", transport: t }).catchTag("HttpNetworkError", () =>
        succeed(new Response("fallback")),
      ),
    );
    expect(await recovered.text()).toBe("fallback");
  });
});

describe(".catchTags for multi-case dispatch", () => {
  test("routes by tag", async () => {
    const run404 = (t: HttpTransport) =>
      run(
        httpRequestText({ url: "/x", transport: t }).catchTags({
          HttpStatusError: (e: HttpStatusError) =>
            succeed(e.status === 404 ? "missing" : "server-error"),
          HttpNetworkError: () => succeed("offline"),
        }),
      );

    const t404 = new ScriptedTransport([new Response("", { status: 404 })]);
    expect(await run404(t404)).toBe("missing");

    const t500 = new ScriptedTransport([new Response("", { status: 500 })]);
    expect(await run404(t500)).toBe("server-error");

    const tdown = new ScriptedTransport([
      new HttpNetworkError({ url: "/x", cause: null, message: "" }),
    ]);
    expect(await run404(tdown)).toBe("offline");
  });
});
