import { describe, expect, test } from "bun:test";
import { Stream, run } from "@spilne/perfect-core";
import { parseNDJSON, parseSSE } from "../src/stream";

describe("parseSSE", () => {
  test("a second run doesn't start inside the first run's unfinished event", async () => {
    const events = Stream.of("data: a", "", "data: b").through(parseSSE);
    // Stops after the first event, while "data: b" is half-read.
    expect((await run(events.take(1).toArray())).map((e) => e.data)).toEqual(["a"]);
    expect((await run(events.toArray())).map((e) => e.data)).toEqual(["a", "b"]);
  });
});

describe("parseNDJSON", () => {
  test("skips blank lines and validates the rest", async () => {
    const schema = {
      safeParse: (d: unknown) =>
        typeof (d as { n?: unknown })?.n === "number"
          ? { success: true as const, data: d as { n: number } }
          : { success: false as const, error: "no n" },
    };
    const out = await run(
      Stream.of('{"n":1}', "  ", '{"n":2}').through(parseNDJSON(schema)).toArray().orDie(),
    );
    expect(out).toEqual([{ n: 1 }, { n: 2 }]);
  });
});
