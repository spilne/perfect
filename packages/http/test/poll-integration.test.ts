// Integration: .repeatUntil on a client.getResponse(...) — the "wait for
// the job/report/operation to be done" pattern.

import { describe, test, expect } from "bun:test";
import {
  type Eff,
  type Throws,
  succeed,
  fail,
  sync,
  run,
  type RepeatTimeoutError,
} from "@spilne/perfect-core";
import {
  DefaultHttpClient,
  type HttpClientError,
  type HttpRequestOptions,
  type HttpTransport,
  type HttpResponse,
  jsonDecoder,
} from "../src";

/** Step through a list of pre-built Responses; each invocation returns a fresh one. */
class ScriptedTransport implements HttpTransport {
  public calls = 0;
  constructor(private readonly script: Array<Response | HttpClientError>) {}
  execute(_: HttpRequestOptions): Eff<Response, Throws<HttpClientError>> {
    return sync(() => {
      const next = this.script[this.calls++];
      if (next === undefined) throw new Error("script exhausted");
      return next;
    }).flatMap((r) =>
      r instanceof Response ? succeed(r) : (fail(r) as Eff<Response, Throws<HttpClientError>>),
    );
  }
}

const job = (body: unknown, status: number): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

describe(".repeatUntil on client.getResponse — the async job pattern", () => {
  test("polls until status === 200, returns the final HttpResponse with decoded body", async () => {
    const t = new ScriptedTransport([
      job({ status: "queued" }, 202),
      job({ status: "running" }, 202),
      job({ result: "ok" }, 200),
    ]);
    const client = new DefaultHttpClient({ transport: t });

    const final = await run(
      client
        .getResponse("/jobs/abc", {
          decoder: jsonDecoder,
          acceptStatus: () => true, // don't fail on 202 during polling
        })
        .repeatUntil({
          until: (res: HttpResponse<unknown>) => res.status === 200,
          intervalMs: 1,
          maxAttempts: 10,
        }),
    );

    expect(final.status).toBe(200);
    expect(final.body).toEqual({ result: "ok" });
    expect(t.calls).toBe(3);
  });

  test("poll times out — RepeatTimeoutError.lastResult is the last HttpResponse seen", async () => {
    const t = new ScriptedTransport([
      job({ status: "queued" }, 202),
      job({ status: "running" }, 202),
      job({ status: "running" }, 202),
    ]);
    const client = new DefaultHttpClient({ transport: t });

    const program = client
      .getResponse("/jobs/xyz", {
        decoder: jsonDecoder,
        acceptStatus: () => true,
      })
      .repeatUntil({
        until: (res: HttpResponse<unknown>) => res.status === 200,
        intervalMs: 1,
        maxAttempts: 3,
      });

    await expect(run(program as any)).rejects.toMatchObject({
      _tag: "RepeatTimeoutError",
      attempts: 3,
      reason: "maxAttempts",
    });

    // Re-run to grab the typed lastResult via catchTag
    const t2 = new ScriptedTransport([
      job({ stage: "queued" }, 202),
      job({ stage: "running" }, 202),
      job({ stage: "running" }, 202),
    ]);
    const client2 = new DefaultHttpClient({ transport: t2 });
    const outcome = await run(
      client2
        .getResponse("/jobs/xyz", { decoder: jsonDecoder, acceptStatus: () => true })
        .repeatUntil({
          until: (res: HttpResponse<unknown>) => res.status === 200,
          intervalMs: 1,
          maxAttempts: 3,
        })
        .catchTag("RepeatTimeoutError", (e: RepeatTimeoutError<HttpResponse<unknown>>) =>
          succeed({
            timedOut: true,
            lastStatus: e.lastResult.status,
            lastBody: e.lastResult.body,
          }),
        ) as any,
    );
    expect(outcome).toEqual({
      timedOut: true,
      lastStatus: 202,
      lastBody: { stage: "running" },
    });
  });
});
