// HttpClient as a Perfect service — Layer-based DI.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, eff, succeed, sync, run } from "@perfect/core";
import {
  DefaultHttpClient,
  type HttpClient,
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  HttpClientService,
} from "../src";

class StubTransport implements HttpTransport {
  constructor(private readonly reply: () => Response) {}
  execute(_: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(this.reply);
  }
}

interface User {
  id: number;
}
const UserParser: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number"
      ? { success: true, data: d as User }
      : { success: false, error: "shape" },
};

describe("HttpClientService — Layer-based DI", () => {
  test("provide a DefaultHttpClient via succeed(), consume via Service.get", async () => {
    const liveClient: HttpClient = new DefaultHttpClient({
      transport: new StubTransport(
        () =>
          new Response(JSON.stringify({ id: 7 }), {
            headers: { "content-type": "application/json" },
          }),
      ),
    });
    const HttpClientLive = succeed({ HttpClient: liveClient });

    const program = eff(function* () {
      const client = yield* HttpClientService.get;
      return yield* client.get("/u/7", UserParser);
    });

    const result = await run(program.with(HttpClientLive) as any);
    expect(result).toEqual({ id: 7 });
  });

  test("swap in a mock client for tests via a different layer", async () => {
    let called = 0;
    const mockClient: HttpClient = {
      get: () =>
        sync(() => {
          called++;
          return { id: 99 } as User;
        }) as any,
      post: () => succeed(null) as any,
      put: () => succeed(null) as any,
      patch: () => succeed(null) as any,
      delete: () => succeed(null) as any,
      getJson: () => succeed(null) as any,
      postJson: () => succeed(null) as any,
      getText: () => succeed("mock") as any,
      getResponse: () =>
        succeed({
          status: 200,
          headers: new Headers(),
          contentType: null,
          contentLength: null,
          body: null,
        }) as any,
      request: () => succeed(null) as any,
      withOverrides: () => mockClient,
    };

    const program = eff(function* () {
      const client = yield* HttpClientService.get;
      return yield* client.get("/anything", UserParser);
    });

    const result = await run(program.with(succeed({ HttpClient: mockClient })) as any);
    expect(result).toEqual({ id: 99 });
    expect(called).toBe(1);
  });
});
