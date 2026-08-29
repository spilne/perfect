// Retry — withRetryAll, withRetryAllBy, retryHttp, and Retry namespace helpers.
//
// Run: bun packages/http/examples/03-retry.ts

import { type Eff, type Throws, sync } from "@spilne/perfect-core";
import {
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type ResponseParser,
  DefaultHttpClient,
  RetryAttempt,
  RetryDecision,
  retryHttp,
  Retry,
  withRetryAll,
  withRetryAllBy,
} from "../src";
import { assertEq } from "./_assert";

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

// >>> example: with-retry-all
// withRetryAll exposes the full RetryAttempt ADT. Use it to retry on
// "not ready" success values (polling), thrown defects, or any combination
// of HTTP errors. The shouldRetry predicate sees every outcome.
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

// >>> example: with-retry-all-by
// withRetryAllBy uses the same logic as withRetryAll but exposes a handler
// that returns RetryDecision.retry() / RetryDecision.stop().
const t3 = new ScriptedTransport([
  json({ state: "pending" }),
  json({ state: "pending" }),
  json({ state: "done", result: 99 }),
]);
const client3 = new DefaultHttpClient({ transport: t3 });

const jobBy = await withRetryAllBy(client3.get("/job/456", JobSchema), {
  maxRetries: 5,
  baseDelayMs: 1,
  handle: (r) =>
    RetryAttempt.isSuccess(r) && r.value.state === "pending"
      ? RetryDecision.retry()
      : RetryDecision.stop(),
}).run();
assertEq(jobBy, { state: "done", result: 99 });
assertEq(t3.attempts, 3);
// <<< example

// >>> example: retryHttp
// retryHttp uses HTTP_RETRYABLE defaults for common transient failure patterns.
const t4 = new ScriptedTransport([
  new Response("down", { status: 503 }),
  new Response("down", { status: 503 }),
  json({ id: 1, name: "alice" }),
]);
const client4 = new DefaultHttpClient({ transport: t4 });

const user = await retryHttp(client4.get("/u", UserSchema), { baseDelayMs: 1 }).run();
assertEq(user, { id: 1, name: "alice" });
assertEq(t4.attempts, 3);
// <<< example

// >>> example: retry-namespace-http
// Namespace-style access from import. Same behavior, different call style.
const t5 = new ScriptedTransport([
  new Response("down", { status: 503 }),
  new Response("down", { status: 503 }),
  json({ id: 1, name: "alice" }),
]);
const client5 = new DefaultHttpClient({ transport: t5 });

const user2 = await Retry.http(client5.get("/u", UserSchema), { baseDelayMs: 1 }).run();
assertEq(user2, { id: 1, name: "alice" });
assertEq(t5.attempts, 3);
// <<< example
