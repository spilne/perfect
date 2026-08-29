import { describe, test, expect } from "bun:test";
import { run, succeed } from "@spilne/perfect-core";
import { MockHttpClient, mockHttpClient, type ResponseParser } from "../src";

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

describe("MockHttpClient — static .on routes", () => {
  test("exact string match by method + path", async () => {
    const mock = new MockHttpClient()
      .on("GET", "/users/1", { id: 1, name: "alice" })
      .on("POST", "/users", { id: 2, name: "created" });

    const a = await run(mock.get("/users/1", UserParser));
    const b = await run(mock.post("/users", UserParser, { json: { name: "created" } }));
    expect(a).toEqual({ id: 1, name: "alice" });
    expect(b).toEqual({ id: 2, name: "created" });
  });

  test("RegExp path matcher", async () => {
    const mock = new MockHttpClient().on("GET", /^\/users\/\d+$/, {
      id: 99,
      name: "wildcard",
    });
    const r = await run(mock.get("/users/42", UserParser));
    expect(r.id).toBe(99);
  });

  test("unmatched route falls back to respondWith default", async () => {
    const mock = new MockHttpClient().respondWith({ id: 0, name: "default" });
    const r = await run(mock.get("/anywhere", UserParser));
    expect(r.name).toBe("default");
  });

  test("unmatched route without default returns empty object → schema error", async () => {
    const mock = new MockHttpClient();
    await expect(run(mock.get("/x", UserParser) as any)).rejects.toMatchObject({
      _tag: "HttpParseError",
    });
  });

  test("static error response via MockHttpClient.fail", async () => {
    const mock = new MockHttpClient().on("GET", "/boom", MockHttpClient.fail(503));
    await expect(run(mock.get("/boom", UserParser) as any)).rejects.toMatchObject({
      _tag: "HttpStatusError",
      status: 503,
    });
  });
});

describe("MockHttpClient — .onFn dynamic handler", () => {
  test("handler sees the call, returns value per-request", async () => {
    const mock = new MockHttpClient().onFn("POST", "/echo", (call) => ({
      id: 1,
      name: (call.json as any).name,
    }));
    const r = await run(mock.post("/echo", UserParser, { json: { name: "dynamic" } }));
    expect(r).toEqual({ id: 1, name: "dynamic" });
  });

  test("handler can return HttpClientError", async () => {
    const mock = new MockHttpClient().onFn("GET", "/flaky", (call) =>
      call.tag === "test" ? MockHttpClient.fail(500) : { id: 1, name: "ok" },
    );
    await expect(run(mock.get("/flaky", UserParser, { tag: "test" }) as any)).rejects.toMatchObject(
      { _tag: "HttpStatusError", status: 500 },
    );
    const ok = await run(mock.get("/flaky", UserParser));
    expect(ok.name).toBe("ok");
  });
});

describe("MockHttpClient — .onSequence ordered queue", () => {
  test("consumes responses in order; last repeats after exhaust", async () => {
    const mock = new MockHttpClient().onSequence("GET", "/poll", [
      { id: 1, name: "queued" },
      { id: 1, name: "running" },
      { id: 1, name: "done" },
    ]);
    const a = await run(mock.get("/poll", UserParser));
    const b = await run(mock.get("/poll", UserParser));
    const c = await run(mock.get("/poll", UserParser));
    const d = await run(mock.get("/poll", UserParser)); // exhausted → last
    expect([a.name, b.name, c.name, d.name]).toEqual(["queued", "running", "done", "done"]);
  });

  test("mix values and errors", async () => {
    const mock = new MockHttpClient().onSequence("GET", "/flaky", [
      MockHttpClient.fail(503),
      { id: 1, name: "recovered" },
    ]);
    await expect(run(mock.get("/flaky", UserParser) as any)).rejects.toMatchObject({
      status: 503,
    });
    const r = await run(mock.get("/flaky", UserParser));
    expect(r.name).toBe("recovered");
  });
});

describe("MockHttpClient — call recording + assertions", () => {
  test("calls array records method, path, json, headers, tag", async () => {
    const mock = new MockHttpClient().respondWith({ id: 1, name: "x" });
    await run(mock.get("/a", UserParser, { tag: "read" }));
    await run(mock.post("/a", UserParser, { json: { k: 1 }, headers: { "X-Trace": "t" } }));

    expect(mock.calls).toHaveLength(2);
    expect(mock.calls[0]).toMatchObject({ method: "GET", path: "/a", tag: "read" });
    expect(mock.calls[1]).toMatchObject({
      method: "POST",
      path: "/a",
      json: { k: 1 },
      headers: { "X-Trace": "t" },
    });
  });

  test("calledWith / calledTimes / callsFor / lastCall", async () => {
    const mock = new MockHttpClient().respondWith({ id: 1, name: "x" });
    await run(mock.get("/u/1", UserParser));
    await run(mock.get("/u/2", UserParser));
    await run(mock.get("/u/1", UserParser));

    expect(mock.calledWith("GET", "/u/1")).toBe(true);
    expect(mock.calledWith("POST", "/u/1")).toBe(false);
    expect(mock.calledTimes("GET", "/u/1")).toBe(2);
    expect(mock.calledTimes("GET", /^\/u\/\d+$/)).toBe(3);
    expect(mock.callsFor("GET", "/u/1")).toHaveLength(2);
    expect(mock.lastCall?.path).toBe("/u/1");
  });

  test("calledWithJson — deep-equality on body", async () => {
    const mock = new MockHttpClient().respondWith({ id: 1, name: "x" });
    await run(mock.post("/x", UserParser, { json: { a: 1, b: [2, 3] } }));
    expect(mock.calledWithJson("POST", "/x", { a: 1, b: [2, 3] })).toBe(true);
    expect(mock.calledWithJson("POST", "/x", { a: 1 })).toBe(false);
  });
});

describe("MockHttpClient — reset variants", () => {
  test("resetCalls keeps routes", async () => {
    const mock = new MockHttpClient().on("GET", "/x", { id: 1, name: "ok" });
    await run(mock.get("/x", UserParser));
    expect(mock.calls).toHaveLength(1);
    mock.resetCalls();
    expect(mock.calls).toHaveLength(0);
    // Route still there
    const r = await run(mock.get("/x", UserParser));
    expect(r.name).toBe("ok");
  });

  test("reset clears everything", async () => {
    const mock = new MockHttpClient()
      .on("GET", "/x", { id: 1, name: "ok" })
      .respondWith({ id: 0, name: "default" });
    await run(mock.get("/x", UserParser));
    mock.reset();
    expect(mock.calls).toHaveLength(0);
    await expect(run(mock.get("/x", UserParser) as any)).rejects.toMatchObject({
      _tag: "HttpParseError",
    });
  });
});

describe("MockHttpClient — getText + getResponse", () => {
  test("getText returns the configured string", async () => {
    const mock = new MockHttpClient().on("GET", "/readme", "hello world");
    expect(await run(mock.getText("/readme"))).toBe("hello world");
  });

  test("getResponse wraps body + metadata", async () => {
    const mock = new MockHttpClient().on("GET", "/file", "the-body");
    const r = await run(mock.getResponse("/file"));
    expect(r.status).toBe(200);
    expect(r.body).toBe("the-body");
  });
});

describe("mockHttpClient() convenience", () => {
  test("returns a MockHttpClient typed as HttpClient", async () => {
    const mock = mockHttpClient().respondWith({ id: 1, name: "ok" });
    const r = await run(mock.get("/u", UserParser));
    expect(r.name).toBe("ok");
  });
});

describe("Integration: plug MockHttpClient into a Layer-consuming program", () => {
  test("swap implementation via HttpClientService for a test run", async () => {
    // Import here to exercise the service wiring
    const { HttpClientService } = await import("../src");
    const { eff } = await import("@spilne/perfect-core");

    const mock = new MockHttpClient().on("GET", "/u/7", { id: 7, name: "alice" });

    const program = eff(function* () {
      const client = yield* HttpClientService.get;
      return yield* client.get("/u/7", UserParser);
    });

    const result = await run(program.with(succeed({ HttpClient: mock })) as any);
    expect(result).toEqual({ id: 7, name: "alice" });
  });
});
