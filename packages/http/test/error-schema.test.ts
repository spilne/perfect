// Typed error-body parsing via `errorSchema`.
//
// Two outcomes per non-2xx response when `errorSchema` is provided:
//   - body matches schema    → HttpStatusError<B> with body: B
//   - body fails JSON/schema → HttpUnknownError with raw text + parseError
// Without errorSchema, behaviour is unchanged: HttpStatusError<string>.

import { describe, test, expect } from "bun:test";
import { type Eff, type Throws, succeed, fail, run } from "@spilne/perfect-core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  DefaultHttpClient,
  HttpStatusError,
  HttpUnknownError,
  httpFetchOk,
  httpRequest,
} from "../src";

class MockTransport implements HttpTransport {
  constructor(private readonly respond: () => Response | HttpClientError) {}
  execute(_options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    const r = this.respond();
    if (r instanceof Response) return succeed(r);
    return fail(r) as any;
  }
}

interface ApiError {
  code: "NOT_FOUND" | "FORBIDDEN" | "RATE_LIMITED";
  detail: string;
}
const ApiErrorParser: ResponseParser<ApiError> = {
  safeParse: (d: any) =>
    d &&
    (d.code === "NOT_FOUND" || d.code === "FORBIDDEN" || d.code === "RATE_LIMITED") &&
    typeof d.detail === "string"
      ? { success: true, data: d as ApiError }
      : { success: false, error: "does not match ApiError" },
};

interface User {
  id: number;
}
const UserParser: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number"
      ? { success: true, data: d as User }
      : { success: false, error: "bad" },
};

describe("httpFetchOk — errorSchema → HttpStatusError<B>", () => {
  test("non-2xx with matching JSON body → HttpStatusError<B> carries typed body", async () => {
    const t = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "RATE_LIMITED", detail: "slow down" }), {
          status: 429,
          headers: { "content-type": "application/json" },
        }),
    );
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(
        httpFetchOk<ApiError>({ url: "/x", transport: t, errorSchema: ApiErrorParser }) as any,
      );
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!._tag).toBe("HttpStatusError");
    expect(caught!.status).toBe(429);
    // Body is typed — no narrowing needed.
    expect(caught!.body.code).toBe("RATE_LIMITED");
    expect(caught!.body.detail).toBe("slow down");
  });
});

describe("httpFetchOk — errorSchema mismatch → HttpUnknownError", () => {
  test("non-JSON body → HttpUnknownError with raw text + parseError", async () => {
    const t = new MockTransport(() => new Response("<html>500</html>", { status: 500 }));
    let caught: HttpUnknownError | undefined;
    try {
      await run(
        httpFetchOk<ApiError>({ url: "/x", transport: t, errorSchema: ApiErrorParser }) as any,
      );
    } catch (e) {
      caught = e as HttpUnknownError;
    }
    expect(caught!._tag).toBe("HttpUnknownError");
    expect(caught!.status).toBe(500);
    expect(caught!.body).toBe("<html>500</html>");
    expect(caught!.parseError).toBeDefined();
    expect(caught!.isRetryable).toBe(true); // 500 is retryable
  });

  test("JSON body but schema rejects → HttpUnknownError with schema error", async () => {
    const t = new MockTransport(
      () => new Response(JSON.stringify({ unexpected: true }), { status: 400 }),
    );
    let caught: HttpUnknownError | undefined;
    try {
      await run(
        httpFetchOk<ApiError>({ url: "/x", transport: t, errorSchema: ApiErrorParser }) as any,
      );
    } catch (e) {
      caught = e as HttpUnknownError;
    }
    expect(caught!._tag).toBe("HttpUnknownError");
    expect(caught!.status).toBe(400);
    expect(caught!.body).toBe('{"unexpected":true}');
    expect(caught!.parseError).toBe("does not match ApiError");
    expect(caught!.isRetryable).toBe(false); // 400 is not
  });
});

describe("httpFetchOk — no errorSchema → unchanged behaviour", () => {
  test("body is raw string, no escalation", async () => {
    const t = new MockTransport(() => new Response("plain text", { status: 500 }));
    let caught: HttpStatusError | undefined;
    try {
      await run(httpFetchOk({ url: "/x", transport: t }) as any);
    } catch (e) {
      caught = e as HttpStatusError;
    }
    expect(caught!._tag).toBe("HttpStatusError");
    expect(caught!.body).toBe("plain text");
  });
});

describe("httpRequest — errorSchema propagates", () => {
  test("happy path: 2xx returns the parsed value — errorSchema unused", async () => {
    const t = new MockTransport(
      () =>
        new Response(JSON.stringify({ id: 1 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    );
    const user = await run(
      httpRequest<User, ApiError>({
        url: "/u/1",
        transport: t,
        schema: UserParser,
        errorSchema: ApiErrorParser,
      }),
    );
    expect(user.id).toBe(1);
  });

  test("error path: errorSchema parses body into HttpStatusError<ApiError>", async () => {
    const t = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "FORBIDDEN", detail: "nope" }), {
          status: 403,
          headers: { "content-type": "application/json" },
        }),
    );
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(
        httpRequest<User, ApiError>({
          url: "/u/1",
          transport: t,
          schema: UserParser,
          errorSchema: ApiErrorParser,
        }),
      );
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!._tag).toBe("HttpStatusError");
    expect(caught!.body.code).toBe("FORBIDDEN");
  });
});

describe("DefaultHttpClient — errorSchema per-request + client-level", () => {
  test("per-request errorSchema parses error body", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "NOT_FOUND", detail: "y" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        }),
    );
    const client = new DefaultHttpClient({ transport });
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(client.get<User, ApiError>("/u", UserParser, { errorSchema: ApiErrorParser }));
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!.body.code).toBe("NOT_FOUND");
    expect(caught!.body.detail).toBe("y");
  });

  test("client-level errorSchema applies to every request", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "RATE_LIMITED", detail: "boom" }), {
          status: 503,
          headers: { "content-type": "application/json" },
        }),
    );
    const client = new DefaultHttpClient({ transport, errorSchema: ApiErrorParser });
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(client.get<User, ApiError>("/u", UserParser));
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!.body.code).toBe("RATE_LIMITED");
  });

  test("per-request errorSchema overrides client-level default", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ different: "shape" }), {
          status: 400,
          headers: { "content-type": "application/json" },
        }),
    );
    const Permissive: ResponseParser<{ different: string }> = {
      safeParse: (d: any) =>
        d && typeof d.different === "string"
          ? { success: true, data: d }
          : { success: false, error: "no" },
    };
    const client = new DefaultHttpClient({ transport, errorSchema: ApiErrorParser });
    let caught: HttpStatusError<{ different: string }> | undefined;
    try {
      await run(
        client.get<User, { different: string }>("/u", UserParser, { errorSchema: Permissive }),
      );
    } catch (e) {
      caught = e as HttpStatusError<{ different: string }>;
    }
    expect(caught!.body.different).toBe("shape");
  });

  test("client-level errorSchema carries through withOverrides", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "NOT_FOUND", detail: "b" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        }),
    );
    const base = new DefaultHttpClient({ transport, errorSchema: ApiErrorParser });
    const derived = base.withOverrides({ headers: { "x-trace": "1" } });
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(derived.get<User, ApiError>("/u", UserParser));
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!.body.code).toBe("NOT_FOUND");
  });

  test("client-level errorSchema mismatch → HttpUnknownError", async () => {
    const transport = new MockTransport(() => new Response("<html>down</html>", { status: 502 }));
    const client = new DefaultHttpClient({ transport, errorSchema: ApiErrorParser });
    let caught: HttpUnknownError | undefined;
    try {
      await run(client.get<User, ApiError>("/u", UserParser));
    } catch (e) {
      caught = e as HttpUnknownError;
    }
    expect(caught!._tag).toBe("HttpUnknownError");
    expect(caught!.body).toBe("<html>down</html>");
  });
});
