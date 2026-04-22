// ResponseParser adapters for popular validation libraries.
//
// `ResponseParser<T>` is intentionally tiny: { safeParse(unknown): { success, data | error } }.
// Zod schemas satisfy it natively. Valibot needs a 3-line adapter.
//
// Run: bun packages/http/examples/07-schema-libs.ts

import { z } from "zod";
import * as v from "valibot";
import {
  type Eff,
  type Throws,
  succeed,
  run,
} from "@perfect/core";
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

// ── Stub transport ────────────────────────────────────────────────

class StubTransport implements HttpTransport {
  constructor(private readonly reply: () => Response) {}
  execute(_o: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return succeed(this.reply());
  }
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

// >>> example: zod-direct
// Zod schemas have .safeParse natively — they ARE ResponseParser<T> with no
// adapter. Pass the schema directly to client.get / httpRequest / etc.
const ZodUser = z.object({ id: z.number(), name: z.string() });
type ZodUser = z.infer<typeof ZodUser>;

const zodClient = new DefaultHttpClient({
  transport: new StubTransport(() => json({ id: 1, name: "alice" })),
});

const zodUser: ZodUser = await run(zodClient.get("/u/1", ZodUser));
assertEq(zodUser, { id: 1, name: "alice" });
// <<< example

// >>> example: valibot-adapter
// Valibot uses safeParse(schema, input) — wrap it once with a tiny adapter
// so the result shape matches ResponseParser. Reusable for any valibot schema.
function valibotParser<S extends v.GenericSchema>(
  schema: S,
): ResponseParser<v.InferOutput<S>> {
  return {
    safeParse: (data: unknown) => {
      const r = v.safeParse(schema, data);
      return r.success
        ? { success: true, data: r.output }
        : { success: false, error: r.issues };
    },
  };
}

const ValibotUser = v.object({ id: v.number(), name: v.string() });
type ValibotUser = v.InferOutput<typeof ValibotUser>;

const valibotClient = new DefaultHttpClient({
  transport: new StubTransport(() => json({ id: 2, name: "bob" })),
});

const valibotUser: ValibotUser = await run(
  valibotClient.get("/u/2", valibotParser(ValibotUser)),
);
assertEq(valibotUser, { id: 2, name: "bob" });
// <<< example

// >>> example: zod-error-schema
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
  await run(errClient.get<ZodUser, ApiError>("/u/1", ZodUser));
} catch (e) {
  caught = e as HttpStatusError<ApiError>;
}
assertEq(caught!.body.code, "FORBIDDEN");
assertEq(caught!.body.detail, "no access");
// <<< example
