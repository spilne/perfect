// Typed error-body parsing via `errorSchema`.
//
// Tests cover the full propagation path: fetch.ts → client.ts → HttpStatusError<B>,
// including the graceful fallback when JSON or schema parsing fails.

import { describe, test, expect } from "bun:test";
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
  DefaultHttpClient,
  HttpStatusError,
  httpFetchOk,
  httpRequest,
} from "../src";

class MockTransport implements HttpTransport {
  constructor(private readonly respond: () => Response | HttpClientError) {}
  execute(options: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    const r = this.respond();
    if (r instanceof Response) return succeed(r);
    return fail(r) as any;
  }
}

interface ApiError {
  code: string;
  detail: string;
}
const ApiErrorParser: ResponseParser<ApiError> = {
  safeParse: (d: any) =>
    d && typeof d.code === "string" && typeof d.detail === "string"
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

describe("httpFetchOk — errorSchema (typed error body)", () => {
  test("non-2xx with JSON body matching schema → body typed + parsed:true", async () => {
    const body = JSON.stringify({ code: "rate_limited", detail: "slow down" });
    const t = new MockTransport(
      () =>
        new Response(body, {
          status: 429,
          headers: { "content-type": "application/json" },
        }),
    );
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(httpFetchOk<ApiError>({ url: "/x", transport: t, errorSchema: ApiErrorParser }) as any);
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught).toBeDefined();
    expect(caught!._tag).toBe("HttpStatusError");
    expect(caught!.status).toBe(429);
    expect(caught!.parsed).toBe(true);
    // Type guard narrows body to ApiError
    expect(HttpStatusError.isParsed<ApiError>(caught!)).toBe(true);
    if (HttpStatusError.isParsed<ApiError>(caught!)) {
      expect(caught!.body.code).toBe("rate_limited");
      expect(caught!.body.detail).toBe("slow down");
    }
    expect(caught!.parseError).toBeUndefined();
  });

  test("non-JSON body → parsed:false, raw body preserved, parseError set", async () => {
    const t = new MockTransport(() => new Response("<html>500</html>", { status: 500 }));
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(httpFetchOk<ApiError>({ url: "/x", transport: t, errorSchema: ApiErrorParser }) as any);
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!.parsed).toBe(false);
    expect(caught!.body).toBe("<html>500</html>");
    expect(caught!.parseError).toBeDefined();
    expect(HttpStatusError.isParsed<ApiError>(caught!)).toBe(false);
  });

  test("JSON body but schema rejects → parsed:false, raw kept, parseError set", async () => {
    const t = new MockTransport(
      () => new Response(JSON.stringify({ unexpected: true }), { status: 400 }),
    );
    let caught: HttpStatusError<ApiError> | undefined;
    try {
      await run(httpFetchOk<ApiError>({ url: "/x", transport: t, errorSchema: ApiErrorParser }) as any);
    } catch (e) {
      caught = e as HttpStatusError<ApiError>;
    }
    expect(caught!.parsed).toBe(false);
    expect(caught!.body).toBe('{"unexpected":true}'); // raw text
    expect(caught!.parseError).toBe("does not match ApiError");
  });

  test("no errorSchema → body is raw string, parsed:false, no parseError", async () => {
    const t = new MockTransport(() => new Response("plain text", { status: 500 }));
    let caught: HttpStatusError | undefined;
    try {
      await run(httpFetchOk({ url: "/x", transport: t }) as any);
    } catch (e) {
      caught = e as HttpStatusError;
    }
    expect(caught!.parsed).toBe(false);
    expect(caught!.body).toBe("plain text");
    expect(caught!.parseError).toBeUndefined();
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
        new Response(JSON.stringify({ code: "forbidden", detail: "nope" }), {
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
    expect(caught!.parsed).toBe(true);
    if (HttpStatusError.isParsed<ApiError>(caught!)) {
      expect(caught!.body.code).toBe("forbidden");
    }
  });
});

describe("DefaultHttpClient — errorSchema per-request + client-level", () => {
  test("per-request errorSchema parses error body", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "x", detail: "y" }), {
          status: 400,
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
    expect(caught!.parsed).toBe(true);
    if (HttpStatusError.isParsed<ApiError>(caught!)) {
      expect(caught!.body.code).toBe("x");
      expect(caught!.body.detail).toBe("y");
    }
  });

  test("client-level errorSchema applies to every request", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "srv", detail: "boom" }), {
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
    expect(caught!.parsed).toBe(true);
    if (HttpStatusError.isParsed<ApiError>(caught!)) {
      expect(caught!.body.code).toBe("srv");
    }
  });

  test("per-request errorSchema overrides client-level default", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ different: "shape" }), {
          status: 400,
          headers: { "content-type": "application/json" },
        }),
    );
    // Client default expects ApiError ({ code, detail }); per-request uses a
    // permissive schema that accepts anything.
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
    expect(caught!.parsed).toBe(true);
    if (HttpStatusError.isParsed<{ different: string }>(caught!)) {
      expect(caught!.body.different).toBe("shape");
    }
  });

  test("client-level errorSchema carries through withOverrides", async () => {
    const transport = new MockTransport(
      () =>
        new Response(JSON.stringify({ code: "a", detail: "b" }), {
          status: 500,
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
    expect(caught!.parsed).toBe(true);
  });
});
