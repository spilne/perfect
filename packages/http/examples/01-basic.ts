// HTTP basics — three tiers of fetch + a typed schema.
//
// Run: bun packages/http/examples/01-basic.ts

import {
  type Eff,
  type Throws,
  succeed,
  fail,
  run,
} from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  HttpStatusError,
  httpFetch,
  httpFetchOk,
  httpRequest,
} from "../src";
import { assertEq } from "./_assert";

// ── Stub transport — keeps the examples network-free ──────────────

class StubTransport implements HttpTransport {
  constructor(private readonly reply: () => Response | HttpClientError) {}
  execute(_options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    const r = this.reply();
    return r instanceof Response ? succeed(r) : (fail(r) as any);
  }
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

interface User { id: number; name: string }
const UserSchema: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d as User }
      : { success: false, error: "not a User" },
};

// >>> example: tier-1-raw
// Tier 1 — raw Response. No status check, no parsing. Useful when you want
// the headers / streaming body before deciding what to do with it.
const tier1 = await run(
  httpFetch({
    url: "https://api/users/1",
    transport: new StubTransport(() => json({ id: 1, name: "alice" })),
  }),
);
assertEq(tier1.status, 200);
// <<< example

// >>> example: tier-2-status-check
// Tier 2 — adds a status check. Non-2xx fails with HttpStatusError carrying
// the response body for diagnostics.
const tier2 = await run(
  httpFetchOk({
    url: "https://api/users/1",
    transport: new StubTransport(() => json({ id: 1, name: "alice" })),
  }),
);
assertEq(tier2.status, 200);
// <<< example

// >>> example: tier-3-validated
// Tier 3 — full pipeline: fetch → status check → JSON → schema. Returns the
// typed value directly; any step failing surfaces as a typed HttpClientError.
const user = await run(
  httpRequest({
    url: "https://api/users/1",
    schema: UserSchema,
    transport: new StubTransport(() => json({ id: 1, name: "alice" })),
  }),
);
assertEq(user, { id: 1, name: "alice" });
// <<< example

// >>> example: status-error
// Non-OK responses become HttpStatusError. Discriminate on .status, retry
// 5xx/429 with .isRetryable.
let caught: HttpStatusError | undefined;
try {
  await run(
    httpFetchOk({
      url: "https://api/users/1",
      transport: new StubTransport(() => new Response("nope", { status: 404 })),
    }) as any,
  );
} catch (e) {
  caught = e as HttpStatusError;
}
assertEq(caught!._tag, "HttpStatusError");
assertEq(caught!.status, 404);
assertEq(caught!.isClientError, true);
// <<< example
