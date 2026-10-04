import { afterAll, describe, expect, test } from "bun:test";
import { run, runExit } from "@spilne/perfect-core";
import { httpStreamText } from "../src/stream";

// A server that sends "tick" every `everyMs`, `count` times, then stops
// (or goes quiet without closing, when `hang` is set).
const server = Bun.serve({
  port: 0,
  fetch(request) {
    const url = new URL(request.url);
    const everyMs = Number(url.searchParams.get("every"));
    const count = Number(url.searchParams.get("count"));
    const hang = url.searchParams.has("hang");
    let sent = 0;
    return new Response(
      new ReadableStream({
        async pull(controller) {
          if (sent === count) {
            if (hang) await new Promise((r) => setTimeout(r, 5_000));
            controller.close();
            return;
          }
          await new Promise((r) => setTimeout(r, everyMs));
          sent++;
          controller.enqueue(new TextEncoder().encode("tick "));
        },
      }),
    );
  },
});
afterAll(() => server.stop(true));
const base = `http://localhost:${server.port}`;

describe("httpStream timeouts", () => {
  test("a stream that lasts longer than timeoutMs is not cut off", async () => {
    // 8 ticks, 30 ms apart: about 240 ms in total, with timeoutMs 100.
    const text = await run(
      httpStreamText({ url: `${base}/?every=30&count=8`, timeoutMs: 100 })
        .toArray()
        .orDie(),
    );
    expect(text.join("").trim().split(" ")).toHaveLength(8);
  });

  test("idleTimeoutMs fails a stream that goes quiet", async () => {
    const exit = await runExit(
      httpStreamText({ url: `${base}/?every=5&count=1&hang`, idleTimeoutMs: 100 }).drain(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const error = (exit.cause as { error?: { _tag: string; timeoutMs: number } }).error;
      expect(error?._tag).toBe("HttpTimeoutError");
      expect(error?.timeoutMs).toBe(100);
    }
  });
});
