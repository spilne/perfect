import { describe, expect, test } from "bun:test";
import { Pipes, Stream, run } from "../src";

describe("Pipes.csv", () => {
  test("keeps the direct through form as headerless arrays", async () => {
    const rows = await run(Stream.of("name,age\nalice,30\n").through(Pipes.csv).toArray());
    expect(rows).toEqual([
      ["name", "age"],
      ["alice", "30"],
    ]);
  });

  test("supports headers, quoted separators, and escaped quotes", async () => {
    const rows = await run(
      Stream.of('name,note\r\nalice,"hello, world"\r\n', 'bob,"say ""hi"""\r\n')
        .through(Pipes.csv({ header: true }))
        .toArray(),
    );

    expect(rows).toEqual([
      { name: "alice", note: "hello, world" },
      { name: "bob", note: 'say "hi"' },
    ]);
  });

  test("preserves quoted newlines and quote escapes split across chunks", async () => {
    const rows = await run(
      Stream.of('id,text\n1,"first line\n', 'second line"\n2,"say "', '"hello"""\n')
        .through(Pipes.csv({ header: true }))
        .toArray(),
    );

    expect(rows).toEqual([
      { id: "1", text: "first line\nsecond line" },
      { id: "2", text: 'say "hello"' },
    ]);
  });

  test("supports custom separators and explicit trimming", async () => {
    const rows = await run(
      Stream.of("name; age\nalice; 30\n")
        .through(Pipes.csv({ header: true, separator: ";", trim: true }))
        .toArray(),
    );
    expect(rows).toEqual([{ name: "alice", age: "30" }]);
  });

  test("fills missing header fields with empty strings", async () => {
    const rows = await run(
      Stream.of("a,b,c\n1,2\n")
        .through(Pipes.csv({ header: true }))
        .toArray(),
    );
    expect(rows).toEqual([{ a: "1", b: "2", c: "" }]);
  });

  test("rejects ambiguous delimiter configuration", () => {
    expect(() => Pipes.csv({ separator: "::" })).toThrow();
    expect(() => Pipes.csv({ separator: ",", quote: "," })).toThrow();
  });
});
