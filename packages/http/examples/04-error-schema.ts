// Typed error response bodies via errorSchema.
//
// Run: bun packages/http/examples/04-error-schema.ts

import { type Eff, type Throws, succeed } from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  DefaultHttpClient,
  HttpStatusError,
  HttpUnknownError,
} from "../src";
import { assertEq } from "./_assert";

class StubTransport implements HttpTransport {
  constructor(private readonly reply: () => Response) {}
  execute(_o: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return succeed(this.reply());
  }
}

interface User {
  id: number;
  name: string;
}
const UserSchema: ResponseParser<User> = {
  safeParse: (d: any) =>
    d && typeof d.id === "number" && typeof d.name === "string"
      ? { success: true, data: d }
      : { success: false, error: "no" },
};

// >>> example: error-schema-typed
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
assertEq(caught!._tag, "HttpStatusError");
assertEq(caught!.status, 429);
assertEq(caught!.body.code, "RATE_LIMITED");
assertEq(caught!.body.detail, "slow down");
// <<< example

// >>> example: error-schema-mismatch
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
assertEq(unknown!._tag, "HttpUnknownError");
assertEq(unknown!.status, 500);
assertEq(unknown!.body, "<html>500</html>");
assertEq(unknown!.isRetryable, true); // 500 is retryable
// <<< example
