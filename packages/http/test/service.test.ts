// HttpClient as a Perfect service — Layer-based DI.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, die, eff, succeed, sync, run } from "@spilne/perfect-core";
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

    const result = await run(program.with(HttpClientLive).orDie());
    expect(result).toEqual({ id: 7 });
  });

  test("swap in a mock client for tests via a different layer", async () => {
    let called = 0;
    // Only get() is called below; the other methods just fill out the interface.
    const unused = () => die(new Error("not used in this test"));
    const mockClient: HttpClient = {
      get: <T>() =>
        sync(() => {
          called++;
          // The fake ignores the schema and always answers with a User.
          return { id: 99 } as T;
        }),
      post: unused,
      postMultipart: unused,
      put: unused,
      patch: unused,
      delete: unused,
      getJson: unused,
      postJson: unused,
      getText: unused,
      getResponse: unused,
      request: unused,
      withOverrides: () => mockClient,
    };

    const program = eff(function* () {
      const client = yield* HttpClientService.get;
      return yield* client.get("/anything", UserParser);
    });

    const result = await run(program.with(succeed({ HttpClient: mockClient })).orDie());
    expect(result).toEqual({ id: 99 });
    expect(called).toBe(1);
  });
});
