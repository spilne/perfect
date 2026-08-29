// HttpClient — base URL, headers, middleware, withOverrides.
//
// Run: bun packages/http/examples/02-client.ts

import { type Eff, type Throws, succeed } from "@spilne/perfect-core";
import {
  type HttpClientError,
  type HttpMiddleware,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  DefaultHttpClient,
} from "../src";
import { assertEq, assertContains } from "./_assert";

class StubTransport implements HttpTransport {
  public last?: HttpRequestOptions;
  constructor(private readonly reply: () => Response) {}
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    this.last = options;
    return succeed(this.reply());
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
const UserSchema: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d as User }
      : { success: false, error: "no" },
};

// >>> example: client-basic
// A client carries baseUrl, default headers, and a transport. Convenience
// methods (.get/.post/.put/.patch/.delete) parse the response through a
// ResponseParser-shaped schema.
const transport = new StubTransport(() => json({ id: 1, name: "alice" }));
const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: { authorization: "Bearer xyz" },
  transport,
});

const user = await client.get("/users/1", UserSchema).run();
assertEq(user, { id: 1, name: "alice" });
assertEq(transport.last!.url, "https://api.example.com/users/1");
assertEq(transport.last!.headers!.authorization, "Bearer xyz");
// <<< example

// >>> example: client-overrides
// withOverrides returns a derived client. Headers spread-merge; everything
// else falls back to the base when the override is undefined.
const traced = client.withOverrides({ headers: { "x-trace": "t-123" } });
await traced.get("/users/1", UserSchema).run();
assertContains(JSON.stringify(transport.last!.headers), "x-trace");
assertContains(JSON.stringify(transport.last!.headers), "Bearer xyz"); // base header preserved
// <<< example

// >>> example: client-middleware
// Sync middleware hooks fire on every request — perfect for metrics or
// request-id propagation. The same context object is passed to onRequest /
// onResponse / onError, so middleware can key per-request state by reference.
const calls: string[] = [];
const logging: HttpMiddleware = {
  onRequest: (ctx) => calls.push(`→ ${ctx.method} ${ctx.url}`),
  onResponse: (ctx) => calls.push(`← ${ctx.method} ${ctx.url} (${ctx.durationMs!.toFixed(0)}ms)`),
  onError: (ctx, err) => calls.push(`✗ ${ctx.method} ${ctx.url} ${err._tag}`),
};
const observed = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  transport: new StubTransport(() => json({ id: 2, name: "bob" })),
  middleware: [logging],
});
await observed.get("/users/2", UserSchema).run();
assertContains(calls.join("|"), "→ GET https://api.example.com/users/2");
assertContains(calls.join("|"), "← GET");
// <<< example
