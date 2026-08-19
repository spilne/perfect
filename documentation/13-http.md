# HTTP

`@perfect/http` is the typed-effect HTTP client. Three tiers of fetch, a
configurable client with middleware, retry with full outcome control, native
streaming (text / lines / NDJSON / SSE), typed error response bodies, and a
test-double mock — all returning `Eff<A, Throws<HttpClientError>>`.

```bash
bun add @perfect/http
```

## Three tiers of fetch

Every request flows through a `HttpTransport`. The default transport is
`globalThis.fetch`; pass your own to mock, proxy, or instrument. The three
tiers compose: pick the level of automation you need.

### Tier 1 — `httpFetch` (raw Response)

<!-- @embed packages/http/examples/01-basic.ts#tier-1-raw -->
```ts
import { httpFetch } from "@perfect/http";

// Tier 1 — raw Response. No status check, no parsing. Useful when you want
// the headers / streaming body before deciding what to do with it.
const tier1 = await httpFetch({
  url: "https://api/users/1",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
}).run();
console.log(tier1.status); // → 200
```
<!-- @end -->

### Tier 2 — `httpFetchOk` (status check)

<!-- @embed packages/http/examples/01-basic.ts#tier-2-status-check -->
```ts
import { httpFetchOk } from "@perfect/http";

// Tier 2 — adds a status check. Non-2xx fails with HttpStatusError carrying
// the response body for diagnostics.
const tier2 = await httpFetchOk({
  url: "https://api/users/1",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
}).run();
console.log(tier2.status); // → 200
```
<!-- @end -->

### Tier 3 — `httpRequest` (full pipeline)

<!-- @embed packages/http/examples/01-basic.ts#tier-3-validated -->
```ts
import { httpRequest } from "@perfect/http";

// Tier 3 — full pipeline: fetch → status check → JSON → schema. Returns the
// typed value directly; any step failing surfaces as a typed HttpClientError.
const user = await httpRequest({
  url: "https://api/users/1",
  schema: UserSchema,
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
}).run();
console.log(user); // → { id: 1, name: "alice" }
```
<!-- @end -->

`schema` accepts anything with a `safeParse(unknown)` method — Zod, Valibot,
arktype, or your own `{ safeParse }` wrapper. See [Schema libraries](#schema-libraries)
below for concrete adapters.

## Typed errors

| Tag | When |
|---|---|
| `HttpNetworkError` | DNS fail, socket hang up, fetch aborted |
| `HttpTimeoutError` | request exceeded `timeoutMs` |
| `HttpStatusError<B>` | server returned a non-OK status |
| `HttpUnknownError` | server failed AND `errorSchema` didn't match the body |
| `HttpParseError` | success-path body parse failure (bad JSON / schema mismatch) |

<!-- @embed packages/http/examples/01-basic.ts#status-error -->
```ts
import { HttpStatusError, httpFetchOk } from "@perfect/http";

// Non-OK responses become HttpStatusError. Discriminate on .status, retry
// 5xx/429 with .isRetryable.
let caught: HttpStatusError | undefined;
try {
  await (
    httpFetchOk({
      url: "https://api/users/1",
      transport: new StubTransport(() => new Response("nope", { status: 404 })),
    }) as any
  ).run();
} catch (e) {
  caught = e as HttpStatusError;
}
console.log(caught!._tag); // → "HttpStatusError"
console.log(caught!.status); // → 404
console.log(caught!.isClientError); // → true
```
<!-- @end -->

## HttpClient

A reusable client carries `baseUrl`, default headers, transport, middleware,
and an optional `errorSchema`.

<!-- @embed packages/http/examples/02-client.ts#client-basic -->
```ts
import { DefaultHttpClient } from "@perfect/http";

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
console.log(user); // → { id: 1, name: "alice" }
console.log(transport.last!.url); // → "https://api.example.com/users/1"
console.log(transport.last!.headers!.authorization); // → "Bearer xyz"
```
<!-- @end -->

### Derive a client with `withOverrides`

<!-- @embed packages/http/examples/02-client.ts#client-overrides -->
```ts
// withOverrides returns a derived client. Headers spread-merge; everything
// else falls back to the base when the override is undefined.
const traced = client.withOverrides({ headers: { "x-trace": "t-123" } });
await traced.get("/users/1", UserSchema).run();
assertContains(JSON.stringify(transport.last!.headers), "x-trace");
assertContains(JSON.stringify(transport.last!.headers), "Bearer xyz"); // base header preserved
```
<!-- @end -->

### Middleware

Sync hooks fired around every request. The same `HttpRequestContext` object
is passed through `onRequest` / `onResponse` / `onError` — middleware can
key per-request state by reference (e.g. `WeakMap<Context, Span>` for
tracing).

<!-- @embed packages/http/examples/02-client.ts#client-middleware -->
```ts
import { type HttpMiddleware, DefaultHttpClient } from "@perfect/http";

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
```
<!-- @end -->

## Retry

`withRetry` retries the default transient set (5xx, 429, timeouts, network
errors). `withRetryAll` exposes a full outcome ADT — useful for polling job
status or reacting to thrown defects.

### `withRetry` — transient HTTP errors

<!-- @embed packages/http/examples/03-retry.ts#with-retry-default -->
```ts
import { DefaultHttpClient, withRetry } from "@perfect/http";
import { RetryPolicy } from "@perfect/core";

// withRetry retries 5xx, 429, timeouts, and network errors.
// Pass a RetryPolicy builder when you want custom retry timing/deadline logic.
const t = new ScriptedTransport([
  new Response("down", { status: 503 }),
  new Response("down", { status: 503 }),
  json({ id: 1, name: "alice" }),
]);
const client = new DefaultHttpClient({ transport: t });

const policy = RetryPolicy.exponential(1).withMaxRetries(3);
const user = await withRetry(client.get("/u", UserSchema), { policy }).run();
console.log(user); // → { id: 1, name: "alice" }
console.log(t.attempts); // → 3
```
<!-- @end -->

### `withRetryAll` — outcome-aware retry

<!-- @embed packages/http/examples/03-retry.ts#with-retry-all -->
```ts
import { type ResponseParser, DefaultHttpClient, RetryAttempt, withRetryAll } from "@perfect/http";

// withRetryAll exposes the full RetryAttempt ADT. Use it to retry on
// "not ready" success values (polling), thrown defects, or any combination
// of HTTP errors. The shouldRetry predicate sees every outcome.
interface JobStatus {
  state: "pending" | "done";
  result?: number;
}
const JobSchema: ResponseParser<JobStatus> = {
  safeParse: (d: any) =>
    d && (d.state === "pending" || d.state === "done")
      ? { success: true, data: d }
      : { success: false, error: "no" },
};

const t2 = new ScriptedTransport([
  json({ state: "pending" }),
  json({ state: "pending" }),
  json({ state: "done", result: 42 }),
]);
const client2 = new DefaultHttpClient({ transport: t2 });

const job = await withRetryAll(client2.get("/job/123", JobSchema), {
  maxRetries: 5,
  baseDelayMs: 1,
  shouldRetry: (r) => (RetryAttempt.isSuccess(r) ? r.value.state !== "done" : true),
}).run();
console.log(job); // → { state: "done", result: 42 }
console.log(t2.attempts); // → 3
```
<!-- @end -->

For polling cadence with a max-attempts/max-duration cap, prefer core's
`.repeatUntil` / `.repeatUntilWithBackoff` — they subsume the polling pattern.

## Typed error response bodies

Pass `errorSchema` (per-request or on the client config) and non-2xx JSON
bodies are parsed into `HttpStatusError<B>`. `e.body` carries the typed
shape — no narrowing required.

<!-- @embed packages/http/examples/04-error-schema.ts#error-schema-typed -->
```ts
import { type ResponseParser, DefaultHttpClient, HttpStatusError } from "@perfect/http";

// Pass errorSchema and non-2xx JSON bodies are parsed into HttpStatusError<B>.
// e.body has the typed shape — no narrowing required.
interface ApiError {
  code: "NOT_FOUND" | "FORBIDDEN" | "RATE_LIMITED";
  detail: string;
}
const ApiErrorSchema: ResponseParser<ApiError> = {
  safeParse: (d: any) =>
    d &&
    ["NOT_FOUND", "FORBIDDEN", "RATE_LIMITED"].includes(d?.code) &&
    typeof d.detail === "string"
      ? { success: true, data: d }
      : { success: false, error: "not ApiError" },
};

const client = new DefaultHttpClient({
  transport: new StubTransport(
    () =>
      new Response(JSON.stringify({ code: "RATE_LIMITED", detail: "slow down" }), {
        status: 429,
        headers: { "content-type": "application/json" },
      }),
  ),
  // Client-level errorSchema applies to every request. Per-request
  // errorSchema overrides it.
  errorSchema: ApiErrorSchema,
});

let caught: HttpStatusError<ApiError> | undefined;
try {
  await client.get<User, ApiError>("/u", UserSchema).run();
} catch (e) {
  caught = e as HttpStatusError<ApiError>;
}
console.log(caught!._tag); // → "HttpStatusError"
console.log(caught!.status); // → 429
console.log(caught!.body.code); // → "RATE_LIMITED"
console.log(caught!.body.detail); // → "slow down"
```
<!-- @end -->

When the body doesn't match (bad JSON or wrong shape), `HttpUnknownError`
is raised instead — carries the raw text + parse cause + status code.

<!-- @embed packages/http/examples/04-error-schema.ts#error-schema-mismatch -->
```ts
import { DefaultHttpClient, HttpUnknownError } from "@perfect/http";

// When errorSchema is provided but the body doesn't match (bad JSON or
// wrong shape), HttpUnknownError is raised instead. Carries the raw text
// + the parse failure cause + the status code (so retry predicates still
// classify by HTTP code).
const broken = new DefaultHttpClient({
  transport: new StubTransport(() => new Response("<html>500</html>", { status: 500 })),
  errorSchema: ApiErrorSchema,
});

let unknown: HttpUnknownError | undefined;
try {
  await broken.get<User, ApiError>("/u", UserSchema).run();
} catch (e) {
  unknown = e as HttpUnknownError;
}
console.log(unknown!._tag); // → "HttpUnknownError"
console.log(unknown!.status); // → 500
console.log(unknown!.body); // → "<html>500</html>"
// 500 is retryable
console.log(unknown!.isRetryable); // → true
```
<!-- @end -->

## Streaming

`httpStream(opts)` returns `Stream<Uint8Array, Throws<HttpClientError>>`.
Every other helper is a composition of this base + composable `Pipe`s
(`utf8Decode`, `lines`, `parseSSE`, `parseNDJSON`).

| Wrapper | Pipeline |
|---|---|
| `httpStreamText(opts)` | bytes → `utf8Decode` |
| `httpStreamLines(opts)` | bytes → `utf8Decode` → `lines` |
| `httpStreamNDJSON(opts, schema)` | lines → `parseNDJSON(schema)` |
| `httpStreamSSE(opts)` | lines → `parseSSE` |

<!-- @embed packages/http/examples/06-streaming.ts#stream-lines -->
```ts
import { httpStreamLines } from "@perfect/http";

// httpStreamLines = bytes → utf8Decode → lines. Every emitted item is one
// complete line (without the terminator).
const linesT = new StubTransport(() => streamOf(["alpha\nbe", "ta\ngamma\n"]));
const lines = await httpStreamLines({ url: "/log", transport: linesT }).toArray().run();
console.log(lines); // → ["alpha", "beta", "gamma"]
```
<!-- @end -->

<!-- @embed packages/http/examples/06-streaming.ts#stream-sse -->
```ts
import { httpStreamSSE } from "@perfect/http";

// httpStreamSSE = lines → parseSSE. Server-Sent Events are emitted as
// SSEvent objects with { event, data, id?, retry? }.
const sseT = new StubTransport(() =>
  streamOf(["event: tick\ndata: 1\n\n", "event: tick\ndata: 2\nid: m-2\n\n"]),
);
const events = await httpStreamSSE({ url: "/events", transport: sseT }).toArray().run();
console.log(events.length); // → 2
console.log(events[0]!.event); // → "tick"
console.log(events[0]!.data); // → "1"
console.log(events[1]!.id); // → "m-2"
```
<!-- @end -->

For ad-hoc compositions, drop down to the base:

```ts
httpStream(opts)
  .through(Pipes.utf8Decode)
  .through(Pipes.lines)
  .through(parseSSE)
  // …any further pipes
```

## Testing — `MockHttpClient`

Drop-in `HttpClient` for tests. Records every call; responds per registered
route via `.on` / `.onFn` / `.onSequence` / `.respondWith`.

<!-- @embed packages/http/examples/05-mock.ts#mock-basic -->
```ts
import { MockHttpClient } from "@perfect/http";

// Set up route → response, run the program, assert what was called.
const mock = new MockHttpClient();
mock.on("GET", "/users/1", { id: 1, name: "alice" });

const user = await mock.get("/users/1", UserSchema).run();
console.log(user); // → { id: 1, name: "alice" }
console.log(mock.calledTimes("GET", "/users/1")); // → 1
```
<!-- @end -->

<!-- @embed packages/http/examples/05-mock.ts#mock-failure -->
```ts
import { MockHttpClient } from "@perfect/http";

// MockHttpClient.fail builds an HttpStatusError for use as a route response.
mock.reset();
mock.on("GET", "/users/999", MockHttpClient.fail(404, "not found"));

let caught: any;
try {
  await mock.get("/users/999", UserSchema).run();
} catch (e) {
  caught = e;
}
console.log(caught._tag); // → "HttpStatusError"
console.log(caught.status); // → 404
```
<!-- @end -->

<!-- @embed packages/http/examples/05-mock.ts#mock-sequence -->
```ts
import { MockHttpClient } from "@perfect/http";

// onSequence consumes responses in order; the last item is reused after the
// queue exhausts. Useful for simulating retry-then-succeed scenarios.
mock.reset();
mock.onSequence("GET", "/u", [MockHttpClient.fail(503, "down"), { id: 7, name: "after-retry" }]);

let firstErr: any;
try {
  await mock.get("/u", UserSchema).run();
} catch (e) {
  firstErr = e;
}
console.log(firstErr.status); // → 503

const second = await mock.get("/u", UserSchema).run();
console.log(second); // → { id: 7, name: "after-retry" }
```
<!-- @end -->

Assertions: `.calledWith` / `.calledTimes` / `.calledWithJson` /
`.callsFor` / `.lastCall`. Cleanup: `.resetCalls()` / `.reset()`.

## Schema libraries

`ResponseParser<T>` is intentionally a tiny duck-typed interface:

```ts
interface ResponseParser<T> {
  safeParse(data: unknown):
    | { readonly success: true; readonly data: T }
    | { readonly success: false; readonly error: unknown };
}
```

### Zod (zero-adapter)

Zod schemas have `.safeParse` natively — they **are** `ResponseParser<T>`.
Pass the schema directly.

<!-- @embed packages/http/examples/07-schema-libs.ts#zod-direct -->
```ts
import { z } from "zod";
import { DefaultHttpClient } from "@perfect/http";

// Zod schemas have .safeParse natively — they ARE ResponseParser<T> with no
// adapter. Pass the schema directly to client.get / httpRequest / etc.
const ZodUser = z.object({ id: z.number(), name: z.string() });
type ZodUser = z.infer<typeof ZodUser>;

const zodClient = new DefaultHttpClient({
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
});

const zodUser: ZodUser = await zodClient.get("/u/1", ZodUser).run();
console.log(zodUser); // → { id: 1, name: "alice" }
```
<!-- @end -->

The same applies to `errorSchema`:

<!-- @embed packages/http/examples/07-schema-libs.ts#zod-error-schema -->
```ts
import { z } from "zod";
import { DefaultHttpClient, HttpStatusError } from "@perfect/http";

// Same adapter-free integration works for errorSchema. Define your error
// envelope as a Zod schema, pass it as errorSchema, and HttpStatusError<B>
// carries the typed shape.
const ApiError = z.object({
  code: z.enum(["NOT_FOUND", "FORBIDDEN", "RATE_LIMITED"]),
  detail: z.string(),
});
type ApiError = z.infer<typeof ApiError>;

const errClient = new DefaultHttpClient({
  transport: new StubTransport(
    () =>
      new Response(JSON.stringify({ code: "FORBIDDEN", detail: "no access" }), {
        status: 403,
        headers: { "content-type": "application/json" },
      }),
  ),
  errorSchema: ApiError,
});

let caught: HttpStatusError<ApiError> | undefined;
try {
  await errClient.get<ZodUser, ApiError>("/u/1", ZodUser).run();
} catch (e) {
  caught = e as HttpStatusError<ApiError>;
}
console.log(caught!.body.code); // → "FORBIDDEN"
console.log(caught!.body.detail); // → "no access"
```
<!-- @end -->

### Valibot (3-line adapter)

Valibot uses `safeParse(schema, input)` — wrap it once and reuse for any
schema:

<!-- @embed packages/http/examples/07-schema-libs.ts#valibot-adapter -->
```ts
import * as v from "valibot";
import { type ResponseParser, DefaultHttpClient } from "@perfect/http";

// Valibot uses safeParse(schema, input) — wrap it once with a tiny adapter
// so the result shape matches ResponseParser. Reusable for any valibot schema.
function valibotParser<S extends v.GenericSchema>(schema: S): ResponseParser<v.InferOutput<S>> {
  return {
    safeParse: (data: unknown) => {
      const r = v.safeParse(schema, data);
      return r.success ? { success: true, data: r.output } : { success: false, error: r.issues };
    },
  };
}

const ValibotUser = v.object({ id: v.number(), name: v.string() });
type ValibotUser = v.InferOutput<typeof ValibotUser>;

const valibotClient = new DefaultHttpClient({
  transport: new StubTransport(() => json({ id: 2, name: "bob" })),
});

const valibotUser: ValibotUser = await valibotClient.get("/u/2", valibotParser(ValibotUser)).run();
console.log(valibotUser); // → { id: 2, name: "bob" }
```
<!-- @end -->

### arktype, custom validators

Same pattern: produce `{ safeParse(data) → { success, data | error } }`
once and reuse. Any library that exposes a parse / validate function can
be adapted in 5 lines.

## Layer-based DI

The package exports `HttpClientService` — a service tag that lets you wire
an `HttpClient` through a `Layer` instead of passing it explicitly. Any
effect that uses `HttpClientService.get` will need an `HttpClient` provided
at run time. See [Services and Layers](./04-services-and-layers.md).

## Next

- [http-otel](./14-http-otel.md) — OpenTelemetry tracing middleware + W3C `traceparent` injection.
