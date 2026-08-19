// Retry — withRetry for transient errors, withRetryAll for full outcome control.
//
// Run: bun packages/http/examples/03-retry.ts

import { RetryPolicy, type Eff, type Throws, sync } from "@perfect/core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  DefaultHttpClient,
  RetryAttempt,
  withRetry,
  withRetryAll,
} from "../src";
import { assertEq } from "./_assert";

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

// Scripted transport: side effects live INSIDE the effect, so each retry
// pulls a fresh response from the script.
class ScriptedTransport implements HttpTransport {
  public attempts = 0;
  constructor(private readonly script: (Response | (() => Response))[]) {}
  execute(_opts: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      const next = this.script[this.attempts++] ?? this.script[this.script.length - 1]!;
      return typeof next === "function" ? next() : next;
    });
  }
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

// >>> example: with-retry-default
// withRetry retries 5xx, 429, timeouts, and network errors.
// You can pass a full RetryPolicy builder for custom timing/deadline behavior.
const t = new ScriptedTransport([
  new Response("down", { status: 503 }),
  new Response("down", { status: 503 }),
  json({ id: 1, name: "alice" }),
]);
const client = new DefaultHttpClient({ transport: t });

const user = await withRetry(client.get("/u", UserSchema), {
  policy: RetryPolicy.exponential(1).withMaxRetries(3),
}).run();
assertEq(user, { id: 1, name: "alice" });
assertEq(t.attempts, 3);
// <<< example

// >>> example: with-retry-all
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
assertEq(job, { state: "done", result: 42 });
assertEq(t2.attempts, 3);
// <<< example
