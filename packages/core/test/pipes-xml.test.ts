import { describe, expect, test } from "bun:test";
import { Pipes, Stream } from "../src";

const parse = (...pieces: string[]) =>
  Stream.of(...pieces)
    .through(Pipes.xml)
    .toArray()
    .run();

describe("Pipes.xml", () => {
  test("a tag split across pieces is still one tag", async () => {
    expect(await parse('<item id="1">h', "i</it", "em>")).toEqual([
      { type: "open", tag: "item", attributes: { id: "1" } },
      { type: "text", text: "hi" },
      { type: "close", tag: "item" },
    ]);
  });

  test("pieces of one character each", async () => {
    expect(await parse(..."<a>text</a>".split(""))).toEqual([
      { type: "open", tag: "a", attributes: undefined },
      { type: "text", text: "text" },
      { type: "close", tag: "a" },
    ]);
  });

  test("the declaration, comments and DOCTYPE are skipped, even when split", async () => {
    expect(await parse("<?xm", 'l version="1.0"?><!', "-- note --><!DOCTYPE r>", "<r/>")).toEqual([
      { type: "selfClose", tag: "r", attributes: undefined },
    ]);
  });

  test("CDATA is text as written", async () => {
    expect(await parse("<r><![CDATA[x <y>", " & z]]></r>")).toEqual([
      { type: "open", tag: "r", attributes: undefined },
      { type: "text", text: "x <y> & z" },
      { type: "close", tag: "r" },
    ]);
  });

  test("a < that is not a tag is skipped, like before", async () => {
    expect(await parse("<p>a < b</p>")).toEqual([
      { type: "open", tag: "p", attributes: undefined },
      { type: "text", text: "a" },
      { type: "text", text: "b" },
      { type: "close", tag: "p" },
    ]);
    expect(await parse("a < b")).toEqual([
      { type: "text", text: "a" },
      { type: "text", text: "b" },
    ]);
  });

  test("text at the end of the input is emitted", async () => {
    expect(await parse("<a>tail")).toEqual([
      { type: "open", tag: "a", attributes: undefined },
      { type: "text", text: "tail" },
    ]);
  });

  test("a long text node in many pieces stays linear", async () => {
    const pieces = Array.from({ length: 200_000 }, () => "x");
    const started = performance.now();
    const events = await Stream.fromArray(["<a>", ...pieces, "</a>"])
      .through(Pipes.xml)
      .toArray()
      .run();
    // Re-reading the pending text for every piece would take seconds here.
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(events[1]).toEqual({ type: "text", text: "x".repeat(200_000) });
  });

  test("each run starts fresh", async () => {
    const stream = Stream.of("<a>", "b</a>").through(Pipes.xml);
    const first = await stream.toArray().run();
    expect(await stream.toArray().run()).toEqual(first);
  });
});
