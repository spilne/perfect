// DefaultHttpClient + AbstractHttpClient tests. Everything runs against a
// MockTransport — no real HTTP.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, fail, sync, run } from "@spilne/perfect-core";
import {
  AbstractHttpClient,
  DefaultHttpClient,
  type HttpClient,
  type HttpClientConfig,
  type HttpClientError,
  type HttpMiddleware,
  type HttpRequestOptions,
  type HttpRequestParams,
  type HttpTransport,
  type RequestOptions,
  type ResponseParser,
  jsonDecoder,
  textDecoder,
} from "../src";

/** Captures every request for assertion; returns canned responses. */
class RecordingTransport implements HttpTransport {
  public calls: HttpRequestOptions[] = [];
  constructor(private readonly respond: (opts: HttpRequestOptions) => Response | HttpClientError) {}
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      this.calls.push(options);
      return this.respond(options);
    }).flatMap((r) =>
      r instanceof Response ? succeed(r) : (fail(r) as Eff<Response, Throws<HttpClientError>>),
    );
  }
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

interface User {
  id: number;
  name: string;
}
const UserParser: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d as User }
      : { success: false, error: "shape" },
};

describe("DefaultHttpClient — baseUrl + path resolution", () => {
  test("prepends baseUrl for relative paths", async () => {
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const client = new DefaultHttpClient({ baseUrl: "https://api.example.com", transport: t });
    await run(client.get("/users/1", UserParser));
    expect(t.calls[0]!.url).toBe("https://api.example.com/users/1");
  });

  test("leaves absolute URLs alone", async () => {
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const client = new DefaultHttpClient({ baseUrl: "https://api.example.com", transport: t });
    await run(client.get("https://other.example/x", UserParser));
    expect(t.calls[0]!.url).toBe("https://other.example/x");
  });

  test("no baseUrl → path passed through", async () => {
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const client = new DefaultHttpClient({ transport: t });
    await run(client.get("https://api/y", UserParser));
    expect(t.calls[0]!.url).toBe("https://api/y");
  });

  test("baseUrl trailing slash handled", async () => {
    const t = new RecordingTransport(() => json({}));
    const client = new DefaultHttpClient({ baseUrl: "https://api/", transport: t });
    await run(client.getJson("x"));
    expect(t.calls[0]!.url).toBe("https://api/x");
  });
});

describe("DefaultHttpClient — header merging", () => {
  test("config headers + per-request headers merged (per-request wins)", async () => {
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const client = new DefaultHttpClient({
      transport: t,
      headers: { Authorization: "Bearer x", "X-App": "foo" },
    });
    await run(
      client.get("/u", UserParser, {
        headers: { "X-App": "override", "X-Req": "1" },
      }),
    );
    expect(t.calls[0]!.headers).toEqual({
      Authorization: "Bearer x",
      "X-App": "override",
      "X-Req": "1",
    });
  });
});

describe("DefaultHttpClient — withOverrides", () => {
  test("returns a new instance with merged config", async () => {
    const t = new RecordingTransport(() => json({}));
    const base = new DefaultHttpClient({
      baseUrl: "https://a.example",
      headers: { "X-A": "1" },
      transport: t,
    });
    const derived = base.withOverrides({
      baseUrl: "https://b.example",
      headers: { "X-B": "2" },
    });
    expect(derived).not.toBe(base);
    await run(derived.getJson("/x"));
    expect(t.calls[0]!.url).toBe("https://b.example/x");
    expect(t.calls[0]!.headers).toEqual({ "X-A": "1", "X-B": "2" });
  });

  test("middleware appends (doesn't replace)", async () => {
    const hits: string[] = [];
    const mw1: HttpMiddleware = { onRequest: () => hits.push("mw1") };
    const mw2: HttpMiddleware = { onRequest: () => hits.push("mw2") };
    const t = new RecordingTransport(() => json({}));
    const base = new DefaultHttpClient({ transport: t, middleware: [mw1] });
    const derived = base.withOverrides({ middleware: [mw2] });
    await run(derived.getJson("/x"));
    expect(hits).toEqual(["mw1", "mw2"]);
  });
});

describe("DefaultHttpClient — method coverage", () => {
  test("get/post/put/patch/delete pass method through", async () => {
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const c = new DefaultHttpClient({ transport: t });
    await run(c.get("/u", UserParser));
    await run(c.post("/u", UserParser, { json: { x: 1 } }));
    await run(c.put("/u", UserParser, { json: { x: 2 } }));
    await run(c.patch("/u", UserParser, { json: { x: 3 } }));
    await run(c.delete("/u", UserParser));
    expect(t.calls.map((c) => c.method)).toEqual(["GET", "POST", "PUT", "PATCH", "DELETE"]);
  });

  test("getJson / postJson return unvalidated unknown", async () => {
    const t = new RecordingTransport(() => json({ arbitrary: "shape" }));
    const c = new DefaultHttpClient({ transport: t });
    expect(await run(c.getJson("/x"))).toEqual({ arbitrary: "shape" });
    expect(await run(c.postJson("/x", { json: { a: 1 } }))).toEqual({ arbitrary: "shape" });
    expect(t.calls[1]!.json).toEqual({ a: 1 });
  });

  test("getText returns the body string", async () => {
    const t = new RecordingTransport(() => new Response("hello world"));
    const c = new DefaultHttpClient({ transport: t });
    expect(await run(c.getText("/x"))).toBe("hello world");
  });
});

describe("DefaultHttpClient — getResponse + decoders", () => {
  test("defaults to binaryDecoder + populates metadata", async () => {
    const t = new RecordingTransport(
      () =>
        new Response("raw", { headers: { "content-type": "text/plain", "content-length": "3" } }),
    );
    const c = new DefaultHttpClient({ transport: t });
    const r = await run(c.getResponse("/x"));
    expect(r.status).toBe(200);
    expect(r.contentType).toBe("text/plain");
    expect(r.contentLength).toBe(3);
    expect(r.body).toBeDefined(); // ReadableStream
  });

  test("custom decoder (textDecoder)", async () => {
    const t = new RecordingTransport(() => new Response("decoded"));
    const c = new DefaultHttpClient({ transport: t });
    const r = await run(c.getResponse("/x", { decoder: textDecoder }));
    expect(r.body).toBe("decoded");
  });

  test("custom decoder (jsonDecoder)", async () => {
    const t = new RecordingTransport(() => json({ k: 1 }));
    const c = new DefaultHttpClient({ transport: t });
    const r = await run(c.getResponse("/x", { decoder: jsonDecoder }));
    expect(r.body).toEqual({ k: 1 });
  });
});

describe("Middleware — sync hooks with duration tracking", () => {
  test("onRequest + onResponse fire with context + duration", async () => {
    const events: any[] = [];
    const mw: HttpMiddleware = {
      onRequest: (ctx) => events.push({ kind: "req", ...ctx }),
      onResponse: (ctx) => events.push({ kind: "res", ...ctx }),
    };
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const c = new DefaultHttpClient({
      transport: t,
      middleware: [mw],
      baseUrl: "https://api",
    });
    await run(c.get("/u/1", UserParser, { tag: "user.lookup" }));
    expect(events[0]).toMatchObject({
      kind: "req",
      method: "GET",
      url: "https://api/u/1",
      tag: "user.lookup",
    });
    expect(events[1]).toMatchObject({ kind: "res", tag: "user.lookup" });
    expect(events[1].durationMs).toBeGreaterThanOrEqual(0);
  });

  test("onError fires on typed failure", async () => {
    const seen: any[] = [];
    const mw: HttpMiddleware = { onError: (ctx, e) => seen.push({ ctx, e }) };
    const t = new RecordingTransport(() => new Response("boom", { status: 500 }));
    const c = new DefaultHttpClient({ transport: t, middleware: [mw] });
    await expect(run(c.getJson("/x") as any)).rejects.toMatchObject({ _tag: "HttpStatusError" });
    expect(seen.length).toBe(1);
    expect(seen[0].e._tag).toBe("HttpStatusError");
    expect(seen[0].ctx.durationMs).toBeGreaterThanOrEqual(0);
  });

  test("no middleware = zero overhead (functionally)", async () => {
    const t = new RecordingTransport(() => json({ id: 1, name: "a" }));
    const c = new DefaultHttpClient({ transport: t });
    expect(await run(c.get("/u", UserParser))).toEqual({ id: 1, name: "a" });
  });
});

describe("Extension pattern — custom subclass", () => {
  interface MyApi extends HttpClient {
    fetchUser(id: number): Eff<User, Throws<HttpClientError>>;
  }

  class MyApiClient extends DefaultHttpClient implements MyApi {
    fetchUser(id: number): Eff<User, Throws<HttpClientError>> {
      return this.get(`/users/${id}`, UserParser);
    }
    override withOverrides(overrides: Partial<HttpClientConfig>): MyApiClient {
      return new MyApiClient({ ...(this as any).config, ...overrides });
    }
  }

  test("subclass adds domain methods while keeping base API", async () => {
    const t = new RecordingTransport(() => json({ id: 42, name: "bob" }));
    const api = new MyApiClient({ baseUrl: "https://api", transport: t });
    const user = await run(api.fetchUser(42));
    expect(user).toEqual({ id: 42, name: "bob" });
    expect(t.calls[0]!.url).toBe("https://api/users/42");
  });
});

describe("AbstractHttpClient — minimal subclass", () => {
  class MinimalClient extends AbstractHttpClient {
    constructor(private readonly responder: (p: HttpRequestParams<any>) => Eff<any, any>) {
      super();
    }
    request<T>(params: HttpRequestParams<T>): Eff<T, Throws<HttpClientError>> {
      return this.responder(params);
    }
    getText(_path: string | URL, _options?: RequestOptions) {
      return succeed("minimal-text") as any;
    }
    getResponse<T = ReadableStream<Uint8Array>>(_path: any, _options?: any) {
      return succeed({
        status: 200,
        headers: new Headers(),
        contentType: null,
        contentLength: null,
        body: undefined as any as T,
      }) as any;
    }
    withOverrides() {
      return this;
    }
  }

  test("convenience methods delegate to request()", async () => {
    const calls: HttpRequestParams<any>[] = [];
    const client = new MinimalClient((p) => {
      calls.push(p);
      return succeed({ id: 1, name: "x" });
    });
    await run(client.get("/u", UserParser));
    await run(client.post("/u", UserParser, { json: { a: 1 } }));
    expect(calls.map((c) => c.method)).toEqual(["GET", "POST"]);
    expect(calls[1]!.json).toEqual({ a: 1 });
  });
});

describe("postMultipart", () => {
  test("builds FormData with file + fields and posts it", async () => {
    const { run } = await import("@spilne/perfect-core");
    const { DefaultHttpClient } = await import("../src");

    const { sync: syncFn } = await import("@spilne/perfect-core");
    let captured: { body?: unknown; method?: string; contentType?: string | undefined } = {};
    const transport = {
      execute: (options: any) =>
        syncFn(() => {
          captured = {
            body: options.body,
            method: options.method,
            contentType: options.headers?.["content-type"],
          };
          return new Response(JSON.stringify({ ok: true }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }),
    };

    const client = new DefaultHttpClient({ baseUrl: "http://x", transport: transport as any });
    const okParser = {
      safeParse: (d: any) =>
        d && d.ok === true
          ? { success: true as const, data: d }
          : { success: false as const, error: "bad" },
    };

    const result = await run(
      client.postMultipart("/upload", okParser as any, {
        file: new Blob(["hello"], { type: "text/plain" }),
        fileField: "doc",
        fields: { kind: "greeting" },
      }) as any,
    );

    expect(result).toEqual({ ok: true });
    expect(captured.method).toBe("POST");
    expect(captured.body).toBeInstanceOf(FormData);
    const fd = captured.body as FormData;
    expect(fd.get("kind")).toBe("greeting");
    expect(fd.get("doc")).toBeInstanceOf(Blob);
    // content-type must NOT be manually set — runtime adds the boundary
    expect(captured.contentType).toBeUndefined();
  });

  test("defaults the file field name to 'file'", async () => {
    const { run } = await import("@spilne/perfect-core");
    const { DefaultHttpClient, identityParser } = await import("../src");

    const { sync: syncFn } = await import("@spilne/perfect-core");
    let fd: FormData | undefined;
    const transport = {
      execute: (options: any) =>
        syncFn(() => {
          fd = options.body as FormData;
          return new Response("{}", {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }),
    };
    const client = new DefaultHttpClient({ baseUrl: "http://x", transport: transport as any });
    await run(client.postMultipart("/u", identityParser as any, { file: new Blob(["x"]) }) as any);
    expect(fd!.get("file")).toBeInstanceOf(Blob);
  });
});
