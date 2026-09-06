# HTTP

`@spilne/perfect-http` is the typed-effect HTTP client. Three tiers of fetch, a
configurable client with middleware, retry with full outcome control, native
streaming (text / lines / NDJSON / SSE), typed error response bodies, and a
test-double mock — all returning `Eff<A, Throws<HttpClientError>>`.

```bash
bun add @spilne/perfect-http
```

## Three tiers of fetch

Examples below are excerpts from the [HTTP example files](../packages/http/examples/).
`StubTransport`, `json`, and `UserSchema` are shared fixtures in those files,
so they run without a network service. Run the full file to include that setup.
`.orDie().run()` rejects the Promise if a request fails; use `.catchTag(...)`
to recover or `.runExit()` to inspect an outcome without rejecting.

Every request flows through a `HttpTransport`. The default transport is
`globalThis.fetch`; pass your own to mock, proxy, or instrument. The three
tiers compose: pick the level of automation you need.

The default transport owns cancellation until response headers arrive. After
that, the caller owns the body stream; finishing the fetch effect does not
abort it. The request timeout and an external abort signal still apply during
body consumption. Consume or cancel a raw `Response` body when you are done.

### Tier 1 — `httpFetch` (raw Response)

<!-- @embed packages/http/examples/01-basic.ts#tier-1-raw -->

```ts
import { httpFetch } from "@spilne/perfect-http";

// Tier 1 — raw Response. No status check, no parsing. Useful when you want
// the headers / streaming body before deciding what to do with it.
const tier1 = await httpFetch({
  url: "https://api/users/1",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
})
  .orDie()
  .run();
console.log(tier1.status); // → 200
```

<!-- @end -->

### Tier 2 — `httpFetchOk` (status check)

<!-- @embed packages/http/examples/01-basic.ts#tier-2-status-check -->

```ts
import { httpFetchOk } from "@spilne/perfect-http";

// Tier 2 — adds a status check. Non-2xx fails with HttpStatusError carrying
// the response body for diagnostics.
const tier2 = await httpFetchOk({
  url: "https://api/users/1",
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
})
  .orDie()
  .run();
console.log(tier2.status); // → 200
```

<!-- @end -->

### Tier 3 — `httpRequest` (full pipeline)

<!-- @embed packages/http/examples/01-basic.ts#tier-3-validated -->

```ts
import { httpRequest } from "@spilne/perfect-http";

// Tier 3 — full pipeline: fetch → status check → JSON → schema. Returns the
// typed value directly; any step failing surfaces as a typed HttpClientError.
const user = await httpRequest({
  url: "https://api/users/1",
  schema: UserSchema,
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
})
  .orDie()
  .run();
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
import { HttpStatusError, httpFetchOk } from "@spilne/perfect-http";

// Non-OK responses become HttpStatusError. Discriminate on .status, retry
// 5xx/429 with .isRetryable.
let caught: HttpStatusError | undefined;
try {
  await httpFetchOk({
    url: "https://api/users/1",
    transport: new StubTransport(() => new Response("nope", { status: 404 })),
  })
    .orDie()
    .run();
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
import { DefaultHttpClient } from "@spilne/perfect-http";

// A client carries baseUrl, default headers, and a transport. Convenience
// methods (.get/.post/.put/.patch/.delete) parse the response through a
// ResponseParser-shaped schema.
const transport = new StubTransport(() => json({ id: 1, name: "alice" }));
const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: { authorization: "Bearer xyz" },
  transport,
});

const user = await client.get("/users/1", UserSchema).orDie().run();
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
await traced.get("/users/1", UserSchema).orDie().run();
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
import { type HttpMiddleware, DefaultHttpClient } from "@spilne/perfect-http";

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
await observed.get("/users/2", UserSchema).orDie().run();
assertContains(calls.join("|"), "→ GET https://api.example.com/users/2");
assertContains(calls.join("|"), "← GET");
```

<!-- @end -->

## Retry

`withRetryAll` exposes a full outcome ADT — useful for polling job status or
reacting to thrown defects.

For common transient HTTP failures, use `retryHttp` instead of full outcome
control. It retries `HTTP_RETRYABLE` typed failures (5xx/429/network/timeout)
with default backoff, then hands through failures and success values unchanged.

### `withRetryAll` — outcome-aware retry

<!-- @embed packages/http/examples/03-retry.ts#with-retry-all -->

```ts
import { type ResponseParser, DefaultHttpClient, RetryAttempt, withRetryAll } from "@spilne/perfect-http";

// withRetryAll exposes the full RetryAttempt ADT. Use it to retry on
// "not ready" success values (polling), thrown defects, or any combination
// of HTTP errors. The shouldRetry predicate sees every outcome.
interface User {
  id: number;
  name: string;
}
const UserSchema: ResponseParser<User> = {
  safeParse: (d: unknown) =>
    d !== null &&
    typeof d === "object" &&
    "id" in d &&
    "name" in d &&
    typeof d.id === "number" &&
    typeof d.name === "string"
      ? { success: true, data: { id: d.id, name: d.name } }
      : { success: false, error: "no" },
};
interface JobStatus {
  state: "pending" | "done";
  result?: number;
}
const JobSchema: ResponseParser<JobStatus> = {
  safeParse: (d: unknown) =>
    d !== null &&
    typeof d === "object" &&
    "state" in d &&
    (d.state === "pending" || d.state === "done") &&
    (!("result" in d) || d.result === undefined || typeof d.result === "number")
      ? {
          success: true,
          data: {
            state: d.state,
            result: "result" in d ? (d.result as number | undefined) : undefined,
          },
        }
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
})
  .orDie()
  .run();
console.log(job); // → { state: "done", result: 42 }
console.log(t2.attempts); // → 3
```

<!-- @end -->

### `retryHttp` — transient HTTP retry

<!-- @embed packages/http/examples/03-retry.ts#retryHttp -->

```ts
import { DefaultHttpClient, retryHttp } from "@spilne/perfect-http";

// retryHttp uses HTTP_RETRYABLE defaults for common transient failure patterns.
const t4 = new ScriptedTransport([
  new Response("down", { status: 503 }),
  new Response("down", { status: 503 }),
  json({ id: 1, name: "alice" }),
]);
const client4 = new DefaultHttpClient({ transport: t4 });

const user = await retryHttp(client4.get("/u", UserSchema), { baseDelayMs: 1 }).orDie().run();
console.log(user); // → { id: 1, name: "alice" }
console.log(t4.attempts); // → 3
```

<!-- @end -->

### `Retry.http` — namespace-style wrapper

<!-- @embed packages/http/examples/03-retry.ts#retry-namespace-http -->

```ts
import { DefaultHttpClient, Retry } from "@spilne/perfect-http";

// Namespace-style access from import. Same behavior, different call style.
const t5 = new ScriptedTransport([
  new Response("down", { status: 503 }),
  new Response("down", { status: 503 }),
  json({ id: 1, name: "alice" }),
]);
const client5 = new DefaultHttpClient({ transport: t5 });

const user2 = await Retry.http(client5.get("/u", UserSchema), { baseDelayMs: 1 }).orDie().run();
console.log(user2); // → { id: 1, name: "alice" }
console.log(t5.attempts); // → 3
```

<!-- @end -->

For polling cadence with a max-attempts/max-duration cap, prefer core's
`.repeatUntil` / `.repeatUntilWithBackoff` — they subsume the polling pattern.

## Typed error response bodies

Pass `errorSchema` (per-request or on the client config) and non-2xx JSON
bodies are parsed into `HttpStatusError<B>`. Its `body` is the parsed value.
A JavaScript `catch` variable is still `unknown`; narrow the error before
inspecting it, and validate its body when the generic type is not available.

<!-- @embed packages/http/examples/04-error-schema.ts#error-schema-typed -->

```ts
import { type ResponseParser, DefaultHttpClient, HttpStatusError } from "@spilne/perfect-http";

// Pass errorSchema and non-2xx JSON bodies are parsed into HttpStatusError<B>.
// JavaScript catch values are unknown; check the error and its body before use.
interface ApiError {
  code: "NOT_FOUND" | "FORBIDDEN" | "RATE_LIMITED";
  detail: string;
}
const ApiErrorSchema: ResponseParser<ApiError> = {
  safeParse: (d: unknown) =>
    d !== null &&
    typeof d === "object" &&
    "code" in d &&
    "detail" in d &&
    (d.code === "NOT_FOUND" || d.code === "FORBIDDEN" || d.code === "RATE_LIMITED") &&
    typeof d.detail === "string"
      ? { success: true, data: { code: d.code, detail: d.detail } }
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

let caught: unknown;
try {
  await client.get<User, ApiError>("/u", UserSchema).orDie().run();
} catch (e) {
  caught = e;
}
if (!(caught instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
const parsedError = ApiErrorSchema.safeParse(caught.body);
if (!parsedError.success) throw new Error("Expected ApiError body");
console.log(caught._tag); // → "HttpStatusError"
console.log(caught.status); // → 429
console.log(parsedError.data.code); // → "RATE_LIMITED"
console.log(parsedError.data.detail); // → "slow down"
```

<!-- @end -->

When the body doesn't match (bad JSON or wrong shape), `HttpUnknownError`
is raised instead — carries the raw text + parse cause + status code.

<!-- @embed packages/http/examples/04-error-schema.ts#error-schema-mismatch -->

```ts
import { DefaultHttpClient, HttpUnknownError } from "@spilne/perfect-http";

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
  await broken.get<User, ApiError>("/u", UserSchema).orDie().run();
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
import { httpStreamLines } from "@spilne/perfect-http";

// httpStreamLines = bytes → utf8Decode → lines. Every emitted item is one
// complete line (without the terminator).
const linesT = new StubTransport(() => streamOf(["alpha\nbe", "ta\ngamma\n"]));
const lines = await httpStreamLines({ url: "/log", transport: linesT }).toArray().orDie().run();
console.log(lines); // → ["alpha", "beta", "gamma"]
```

<!-- @end -->

<!-- @embed packages/http/examples/06-streaming.ts#stream-sse -->

```ts
import { httpStreamSSE } from "@spilne/perfect-http";

// httpStreamSSE = lines → parseSSE. Server-Sent Events are emitted as
// SSEvent objects with { event, data, id?, retry? }.
const sseT = new StubTransport(() =>
  streamOf(["event: tick\ndata: 1\n\n", "event: tick\ndata: 2\nid: m-2\n\n"]),
);
const events = await httpStreamSSE({ url: "/events", transport: sseT }).toArray().orDie().run();
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
import { MockHttpClient } from "@spilne/perfect-http";

// Set up route → response, run the program, assert what was called.
const mock = new MockHttpClient();
mock.on("GET", "/users/1", { id: 1, name: "alice" });

const user = await mock.get("/users/1", UserSchema).orDie().run();
console.log(user); // → { id: 1, name: "alice" }
console.log(mock.calledTimes("GET", "/users/1")); // → 1
```

<!-- @end -->

<!-- @embed packages/http/examples/05-mock.ts#mock-failure -->

```ts
import { HttpStatusError, MockHttpClient } from "@spilne/perfect-http";

// MockHttpClient.fail builds an HttpStatusError for use as a route response.
mock.reset();
mock.on("GET", "/users/999", MockHttpClient.fail(404, "not found"));

let caught: unknown;
try {
  await mock.get("/users/999", UserSchema).orDie().run();
} catch (e) {
  caught = e;
}
if (!(caught instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
console.log(caught._tag); // → "HttpStatusError"
console.log(caught.status); // → 404
```

<!-- @end -->

<!-- @embed packages/http/examples/05-mock.ts#mock-sequence -->

```ts
import { HttpStatusError, MockHttpClient } from "@spilne/perfect-http";

// onSequence consumes responses in order; the last item is reused after the
// queue exhausts. Useful for simulating retry-then-succeed scenarios.
mock.reset();
mock.onSequence("GET", "/u", [MockHttpClient.fail(503, "down"), { id: 7, name: "after-retry" }]);

let firstErr: unknown;
try {
  await mock.get("/u", UserSchema).orDie().run();
} catch (e) {
  firstErr = e;
}
if (!(firstErr instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
console.log(firstErr.status); // → 503

const second = await mock.get("/u", UserSchema).orDie().run();
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
import { DefaultHttpClient } from "@spilne/perfect-http";

// Zod schemas have .safeParse natively — they ARE ResponseParser<T> with no
// adapter. Pass the schema directly to client.get / httpRequest / etc.
const ZodUser = z.object({ id: z.number(), name: z.string() });
type ZodUser = z.infer<typeof ZodUser>;

const zodClient = new DefaultHttpClient({
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
});

const zodUser: ZodUser = await zodClient.get("/u/1", ZodUser).orDie().run();
console.log(zodUser); // → { id: 1, name: "alice" }
```

<!-- @end -->

The same applies to `errorSchema`:

<!-- @embed packages/http/examples/07-schema-libs.ts#zod-error-schema -->

```ts
import { z } from "zod";
import { DefaultHttpClient, HttpStatusError } from "@spilne/perfect-http";

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

let caught: unknown;
try {
  await errClient.get<ZodUser, ApiError>("/u/1", ZodUser).orDie().run();
} catch (e) {
  caught = e;
}
if (!(caught instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
const parsedError = ApiError.safeParse(caught.body);
if (!parsedError.success) throw new Error("Expected ApiError body");
console.log(parsedError.data.code); // → "FORBIDDEN"
console.log(parsedError.data.detail); // → "no access"
```

<!-- @end -->

### Valibot (3-line adapter)

Valibot uses `safeParse(schema, input)` — wrap it once and reuse for any
schema:

<!-- @embed packages/http/examples/07-schema-libs.ts#valibot-adapter -->

```ts
import * as v from "valibot";
import { type ResponseParser, DefaultHttpClient } from "@spilne/perfect-http";

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

const valibotUser: ValibotUser = await valibotClient
  .get("/u/2", valibotParser(ValibotUser))
  .orDie()
  .run();
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
