// Format pipes: tsv / ssv / fixedWidth / regex / parseAs / parseAsLenient /
// lengthPrefixed / binaryDecode / xml.
//
// tsv honors double-quoted fields ("" escapes a quote); ssv collapses runs of
// whitespace and has no quoting; lengthPrefixed buffers partial frames across
// chunk splits (4-byte big-endian header by default); xml scans each chunk
// independently (no cross-chunk tag buffering).

import { describe, test, expect } from "bun:test";
import { run, runExit, Stream, Pipes, Cause, SchemaParseError } from "../src";
import type { SchemaParser } from "../src";

const NumberSchema: SchemaParser<number> = {
  safeParse: (data) =>
    typeof data === "number" ? { success: true, data } : { success: false, error: "not a number" },
};

describe("tsv", () => {
  test("splits lines on tabs", async () => {
    const rows = await run(
      (Stream.succeed("a\tb\tc\n1\t2\t3\n") as any).through(Pipes.tsv).toArray(),
    );
    expect(rows).toEqual([
      ["a", "b", "c"],
      ["1", "2", "3"],
    ]);
  });

  test("honors quoted fields containing tabs", async () => {
    const rows = await run((Stream.succeed('"x\ty"\tz\n') as any).through(Pipes.tsv).toArray());
    expect(rows).toEqual([["x\ty", "z"]]);
  });

  test("a doubled quote inside a quoted field is an escaped quote", async () => {
    const rows = await run(
      (Stream.succeed('"he said ""hi"""\tok\n') as any).through(Pipes.tsv).toArray(),
    );
    expect(rows).toEqual([['he said "hi"', "ok"]]);
  });

  test("joins lines split across chunks", async () => {
    const rows = await run((Stream.of("a\tb\nc", "\td\n") as any).through(Pipes.tsv).toArray());
    expect(rows).toEqual([
      ["a", "b"],
      ["c", "d"],
    ]);
  });

  test("empty stream emits nothing", async () => {
    expect(await run((Stream.empty<string>() as any).through(Pipes.tsv).toArray())).toEqual([]);
  });
});

describe("ssv", () => {
  test("collapses runs of whitespace and trims the line", async () => {
    const rows = await run(
      (Stream.succeed("  foo   bar\tbaz  \n1 2 3\n") as any).through(Pipes.ssv).toArray(),
    );
    expect(rows).toEqual([
      ["foo", "bar", "baz"],
      ["1", "2", "3"],
    ]);
  });

  test("handles a final line without a trailing newline", async () => {
    const rows = await run((Stream.succeed("a b") as any).through(Pipes.ssv).toArray());
    expect(rows).toEqual([["a", "b"]]);
  });
});

describe("fixedWidth", () => {
  const columns = [
    { name: "id", start: 0, end: 5 },
    { name: "name", start: 5, end: 12 },
    { name: "amount", start: 12, end: 18 },
  ];

  test("slices positional columns into records, trimming by default", async () => {
    const rows = await run(
      (Stream.succeed("00001Alice  100.50\n") as any).through(Pipes.fixedWidth(columns)).toArray(),
    );
    expect(rows).toEqual([{ id: "00001", name: "Alice", amount: "100.50" }]);
  });

  test("trim: false preserves padding", async () => {
    const rows = await run(
      (Stream.succeed("00001Alice  100.50\n") as any)
        .through(Pipes.fixedWidth([{ name: "name", start: 5, end: 12, trim: false }]))
        .toArray(),
    );
    expect(rows).toEqual([{ name: "Alice  " }]);
  });
});

describe("regex", () => {
  test("extracts named capture groups per line, dropping non-matches", async () => {
    const rows = await run(
      (Stream.succeed("warn: low disk\nnope\ninfo: ok\n") as any)
        .through(Pipes.regex(/^(?<level>\w+): (?<msg>.+)$/))
        .toArray(),
    );
    expect(rows).toEqual([
      { level: "warn", msg: "low disk" },
      { level: "info", msg: "ok" },
    ]);
  });

  test("a /g flag does not skip lines via stateful lastIndex", async () => {
    const rows = await run(
      (Stream.succeed("a=1\na=2\na=3\n") as any).through(Pipes.regex(/^a=(?<n>\d+)$/g)).toArray(),
    );
    expect(rows).toEqual([{ n: "1" }, { n: "2" }, { n: "3" }]);
  });
});

describe("parseAs / parseAsLenient", () => {
  test("parseAs emits validated values", async () => {
    const result = await run(
      (Stream.of(1, 2, 3) as any).through(Pipes.parseAs(NumberSchema)).toArray(),
    );
    expect(result).toEqual([1, 2, 3]);
  });

  test("parseAs fails the stream with SchemaParseError on the first invalid element", async () => {
    const exit = await runExit(
      (Stream.of<unknown>(1, "x", 3) as any).through(Pipes.parseAs(NumberSchema)).toArray(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const err = Cause.firstFail(exit.cause)?.value as any;
      expect(err).toBeInstanceOf(SchemaParseError);
      expect(err._tag).toBe("SchemaParseError");
      expect(err.error).toBe("not a number");
    }
  });

  test("parseAsLenient drops invalid elements", async () => {
    const result = await run(
      (Stream.of<unknown>(1, "x", 3) as any).through(Pipes.parseAsLenient(NumberSchema)).toArray(),
    );
    expect(result).toEqual([1, 3]);
  });

  test("empty stream stays empty for both", async () => {
    expect(
      await run((Stream.empty<unknown>() as any).through(Pipes.parseAs(NumberSchema)).toArray()),
    ).toEqual([]);
    expect(
      await run(
        (Stream.empty<unknown>() as any).through(Pipes.parseAsLenient(NumberSchema)).toArray(),
      ),
    ).toEqual([]);
  });
});

describe("lengthPrefixed", () => {
  const frame4 = (payload: number[]): number[] => {
    const len = payload.length;
    return [(len >>> 24) & 0xff, (len >>> 16) & 0xff, (len >>> 8) & 0xff, len & 0xff, ...payload];
  };

  test("splits 4-byte big-endian framed messages", async () => {
    const bytes = new Uint8Array([...frame4([1, 2, 3]), ...frame4([9])]);
    const messages = await run(
      (Stream.succeed(bytes) as any).through(Pipes.lengthPrefixed()).toArray(),
    );
    expect(messages.map((m: Uint8Array) => Array.from(m))).toEqual([[1, 2, 3], [9]]);
  });

  test("buffers frames split across chunk boundaries — even inside the header", async () => {
    const all = [...frame4([1, 2, 3]), ...frame4([4, 5])];
    const chunks = [
      new Uint8Array(all.slice(0, 2)), // half of the first header
      new Uint8Array(all.slice(2, 8)),
      new Uint8Array(all.slice(8)),
    ];
    const messages = await run(
      (Stream.fromArray(chunks) as any).through(Pipes.lengthPrefixed()).toArray(),
    );
    expect(messages.map((m: Uint8Array) => Array.from(m))).toEqual([
      [1, 2, 3],
      [4, 5],
    ]);
  });

  test("a trailing incomplete frame is dropped", async () => {
    const bytes = new Uint8Array([...frame4([7]), 0, 0, 0, 5, 1, 2]); // second frame truncated
    const messages = await run(
      (Stream.succeed(bytes) as any).through(Pipes.lengthPrefixed()).toArray(),
    );
    expect(messages.map((m: Uint8Array) => Array.from(m))).toEqual([[7]]);
  });

  test("supports 1-byte headers", async () => {
    const bytes = new Uint8Array([3, 1, 2, 3, 1, 9]);
    const messages = await run(
      (Stream.succeed(bytes) as any).through(Pipes.lengthPrefixed({ headerBytes: 1 })).toArray(),
    );
    expect(messages.map((m: Uint8Array) => Array.from(m))).toEqual([[1, 2, 3], [9]]);
  });

  test("supports 2-byte little-endian headers", async () => {
    const bytes = new Uint8Array([3, 0, 1, 2, 3]);
    const messages = await run(
      (Stream.succeed(bytes) as any)
        .through(Pipes.lengthPrefixed({ headerBytes: 2, littleEndian: true }))
        .toArray(),
    );
    expect(messages.map((m: Uint8Array) => Array.from(m))).toEqual([[1, 2, 3]]);
  });
});

describe("binaryDecode", () => {
  test("decodes each chunk with the supplied decoder", async () => {
    const decode = (buf: Uint8Array) => buf.reduce((a, b) => a + b, 0);
    const result = await run(
      (Stream.of(new Uint8Array([1, 2]), new Uint8Array([10])) as any)
        .through(Pipes.binaryDecode(decode))
        .toArray(),
    );
    expect(result).toEqual([3, 10]);
  });

  test("composes with lengthPrefixed for framed decoding", async () => {
    const bytes = new Uint8Array([2, 104, 105, 3, 121, 111, 33]); // 1-byte headers: "hi", "yo!"
    const result = await run(
      (Stream.succeed(bytes) as any)
        .through(Pipes.lengthPrefixed({ headerBytes: 1 }))
        .through(Pipes.binaryDecode((buf) => new TextDecoder().decode(buf)))
        .toArray(),
    );
    expect(result).toEqual(["hi", "yo!"]);
  });
});

describe("xml", () => {
  test("emits SAX-style events for tags, attributes and text", async () => {
    const events = await run(
      (Stream.succeed('<root><item id="1">hi</item><br/></root>') as any)
        .through(Pipes.xml)
        .toArray(),
    );
    expect(events).toEqual([
      { type: "open", tag: "root", attributes: undefined },
      { type: "open", tag: "item", attributes: { id: "1" } },
      { type: "text", text: "hi" },
      { type: "close", tag: "item" },
      { type: "selfClose", tag: "br", attributes: undefined },
      { type: "close", tag: "root" },
    ]);
  });

  test("parses multiple attributes", async () => {
    const events = await run(
      (Stream.succeed('<a href="x" rel="nofollow"/>') as any).through(Pipes.xml).toArray(),
    );
    expect(events).toEqual([
      { type: "selfClose", tag: "a", attributes: { href: "x", rel: "nofollow" } },
    ]);
  });

  test("whitespace-only text nodes are dropped", async () => {
    const events = await run((Stream.succeed("<a>\n  </a>") as any).through(Pipes.xml).toArray());
    expect(events).toEqual([
      { type: "open", tag: "a", attributes: undefined },
      { type: "close", tag: "a" },
    ]);
  });

  test("empty stream emits nothing", async () => {
    expect(await run((Stream.empty<string>() as any).through(Pipes.xml).toArray())).toEqual([]);
  });
});
