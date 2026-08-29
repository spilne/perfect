// Tests against a MockTransport — the whole fetch pipeline without a real server.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, run, fail } from "@spilne/perfect-core";
import {
  type HttpTransport,
  type HttpRequestOptions,
  type HttpClientError,
  type ResponseParser,
  HttpNetworkError,
  httpFetch,
  httpFetchOk,
  httpRequest,
  httpRequestJson,
  httpRequestText,
} from "../src";

/** In-memory transport: returns a canned Response per call. */
class MockTransport implements HttpTransport {
  public calls: HttpRequestOptions[] = [];
  constructor(private readonly respond: (opts: HttpRequestOptions) => Response | HttpClientError) {}
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    this.calls.push(options);
    const r = this.respond(options);
    if (r instanceof Response) return succeed(r);
    return fail(r) as any;
  }
}

const jsonOk = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

describe("httpFetch — raw Response passthrough", () => {
  test("returns the Response as-is (200)", async () => {
    const t = new MockTransport(() => jsonOk({ hello: "world" }));
    const r = await run(httpFetch({ url: "https://api/", transport: t }));
    expect(r.status).toBe(200);
    expect(await r.json()).toEqual({ hello: "world" });
    expect(t.calls.length).toBe(1);
    expect(t.calls[0]!.url).toBe("https://api/");
  });

  test("returns 5xx as a Response — no status check at this tier", async () => {
    const t = new MockTransport(() => new Response("oops", { status: 503 }));
    const r = await run(httpFetch({ url: "/x", transport: t }));
    expect(r.status).toBe(503);
  });

  test("transport failure propagates as typed HttpClientError", async () => {
    const t = new MockTransport(
      () => new HttpNetworkError({ url: "/x", cause: new Error("down"), message: "down" }),
    );
    await expect(run(httpFetch({ url: "/x", transport: t }))).rejects.toMatchObject({
      _tag: "HttpNetworkError",
    });
  });
});

describe("httpFetchOk — fetch + status check", () => {
  test("2xx passes through unchanged", async () => {
    const t = new MockTransport(() => jsonOk({ ok: true }));
    const r = await run(httpFetchOk({ url: "/x", transport: t }));
    expect(r.status).toBe(200);
  });

  test("5xx fails with HttpStatusError carrying body + metadata", async () => {
    const t = new MockTransport(() => new Response("database down", { status: 503 }));
    await expect(
      run(httpFetchOk({ url: "/x", method: "POST", transport: t }) as any),
    ).rejects.toMatchObject({
      _tag: "HttpStatusError",
      status: 503,
      body: "database down",
      message: expect.stringContaining("POST"),
    });
  });

  test("4xx fails too (non-OK by default)", async () => {
    const t = new MockTransport(() => new Response("nope", { status: 404 }));
    await expect(run(httpFetchOk({ url: "/x", transport: t }) as any)).rejects.toMatchObject({
      _tag: "HttpStatusError",
      status: 404,
    });
  });

  test("acceptStatus override lets you opt in to non-default success codes", async () => {
    const t = new MockTransport(() => new Response("nobody here", { status: 404 }));
    const r = await run(httpFetchOk({ url: "/x", transport: t, acceptStatus: (s) => s === 404 }));
    expect(r.status).toBe(404);
  });
});

describe("httpRequest<T> — full pipeline with parser", () => {
  interface User {
    id: number;
    name: string;
  }
  const UserParser: ResponseParser<User> = {
    safeParse: (d: any) =>
      d && typeof d.id === "number" && typeof d.name === "string"
        ? { success: true, data: d as User }
        : { success: false, error: `bad user shape: ${JSON.stringify(d)}` },
  };

  test("success → validated typed value", async () => {
    const t = new MockTransport(() => jsonOk({ id: 7, name: "alice" }));
    const user = await run(httpRequest({ url: "/u/7", transport: t, schema: UserParser }));
    expect(user).toEqual({ id: 7, name: "alice" });
  });

  test("schema mismatch → HttpParseError", async () => {
    const t = new MockTransport(() => jsonOk({ id: "not a number" }));
    await expect(
      run(httpRequest({ url: "/u/7", transport: t, schema: UserParser }) as any),
    ).rejects.toMatchObject({ _tag: "HttpParseError" });
  });

  test("bad JSON → HttpParseError", async () => {
    const t = new MockTransport(
      () => new Response("<html>not json</html>", { headers: { "content-type": "text/html" } }),
    );
    await expect(
      run(httpRequest({ url: "/u/7", transport: t, schema: UserParser }) as any),
    ).rejects.toMatchObject({ _tag: "HttpParseError" });
  });

  test("5xx → HttpStatusError (parser never runs)", async () => {
    const t = new MockTransport(() => new Response("boom", { status: 500 }));
    await expect(
      run(httpRequest({ url: "/u/7", transport: t, schema: UserParser }) as any),
    ).rejects.toMatchObject({ _tag: "HttpStatusError", status: 500 });
  });
});

describe("httpRequestJson / httpRequestText", () => {
  test("httpRequestJson returns unvalidated unknown", async () => {
    const t = new MockTransport(() => jsonOk({ random: "shape" }));
    const data = await run(httpRequestJson({ url: "/x", transport: t }));
    expect(data).toEqual({ random: "shape" });
  });

  test("httpRequestText returns the body string", async () => {
    const t = new MockTransport(() => new Response("plain text"));
    const s = await run(httpRequestText({ url: "/x", transport: t }));
    expect(s).toBe("plain text");
  });
});

describe("Request options — headers, json body, method", () => {
  test("method defaults to GET; explicit method propagates to transport", async () => {
    const t = new MockTransport(() => jsonOk({}));
    await run(httpFetch({ url: "/a", transport: t }));
    await run(httpFetch({ url: "/b", method: "POST", transport: t }));
    expect(t.calls[0]!.method).toBeUndefined(); // default passed through as-is
    expect(t.calls[1]!.method).toBe("POST");
  });

  test("json body + headers are observable at transport level", async () => {
    const t = new MockTransport(() => jsonOk({}));
    await run(
      httpFetch({
        url: "/x",
        method: "POST",
        json: { a: 1 },
        headers: { Authorization: "Bearer abc" },
        transport: t,
      }),
    );
    expect(t.calls[0]!.json).toEqual({ a: 1 });
    expect(t.calls[0]!.headers).toEqual({ Authorization: "Bearer abc" });
  });
});
