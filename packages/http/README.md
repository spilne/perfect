# @perfect/http

Typed-effect HTTP client for `@perfect/core`. Three tiers of fetch, a
configurable client with middleware, retry with full outcome control, native
streaming (text / lines / NDJSON / SSE), typed error response bodies, and a
test-double mock — every request returns `Eff<A, Throws<HttpClientError>>`,
so failures are visible in the type and handled with `.catch` / `.catchTag`.

## Install

```bash
bun add @perfect/http
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

```ts
import { succeed } from "@perfect/core";
import { DefaultHttpClient, type ResponseParser } from "@perfect/http";

interface User {
  id: number;
  name: string;
}

// Any { safeParse } object works — zod schemas satisfy this directly.
const UserSchema: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d as User }
      : { success: false, error: "not a User" },
};

const client = new DefaultHttpClient({
  baseUrl: "https://api.example.com",
  headers: { authorization: "Bearer xyz" },
});

// fetch → status check → JSON → schema, as one typed effect
const user = await client.get("/users/1", UserSchema).run();

// errors are tagged — handle them in the type
const safe = client
  .get("/users/1", UserSchema)
  .catchTag("HttpStatusError", (e) => succeed({ id: -1, name: `(status ${e.status})` }));
```

`client.withOverrides({ headers: { "x-trace": "t-123" } })` derives a client —
headers spread-merge, everything else falls back to the base.

## Three tiers of fetch

Below the client sit three free functions, composing upward:

| Tier | Function                                              | Adds                                           |
| ---- | ----------------------------------------------------- | ---------------------------------------------- |
| 1    | `httpFetch`                                           | raw `Response` through a transport — no checks |
| 2    | `httpFetchOk`                                         | status check → `HttpStatusError` on non-2xx    |
| 3    | `httpRequest` / `httpRequestJson` / `httpRequestText` | body decode + schema parse                     |

Every request flows through an `HttpTransport` (default: `globalThis.fetch`) —
pass your own to mock, proxy, or instrument.

## Features

- **Typed errors** — `HttpNetworkError`, `HttpTimeoutError`, `HttpStatusError`,
  `HttpParseError`, `HttpUnknownError`; `HTTP_RETRYABLE` lists the transient tags
- **Client** — `DefaultHttpClient` with baseUrl, default headers,
  `.get/.post/.put/.patch/.delete`, `withOverrides`, `HttpMiddleware` hooks
  (`onRequest` / `onResponse` / `onError`)
- **DI** — `HttpClientService` tag for Layer-based injection
- **Retry** — `withRetry` (HTTP-aware transient retry), `withRetryAll`
  (full `RetryAttempt` outcome ADT); for polling use core's
  `.repeatUntil` / `.repeatUntilWithBackoff`
- **Streaming** — `httpStream` base plus `httpStreamText` / `httpStreamLines` /
  `httpStreamNDJSON` / `httpStreamSSE`, and composable `parseSSE` / `parseNDJSON`
  pipes
- **Testing** — `MockHttpClient` / `mockHttpClient`: route-matched test double
  with call recording

## Links

- Repo: https://github.com/spilne/perfect
- Full guide: [`documentation/13-http.md`](../../documentation/13-http.md)
- Runnable examples: [`examples/`](./examples/)
- OpenTelemetry tracing for this client: `@perfect/http-otel`
