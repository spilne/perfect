import { describe, expect, test } from "bun:test";
import {
  Console,
  FileSystem,
  FileSystemError,
  TestConsole,
  TestFileSystem,
  eff,
  join,
  provide,
  realFileSystem,
  run,
  runExit,
  yieldNow,
  Cause,
} from "../src";

const withFs = (fs: TestFileSystem, program: any) => provide(program, FileSystem, fs) as any;

describe("TestFileSystem", () => {
  test("reads seeded files and reports missing ones as typed failures", async () => {
    const fs = new TestFileSystem({ "/etc/app.conf": "debug=true" });

    const read = eff(function* () {
      const io = yield* FileSystem.get;
      return yield* io.readFile("/etc/app.conf");
    });
    expect(await run(withFs(fs, read))).toBe("debug=true");

    const missing = eff(function* () {
      const io = yield* FileSystem.get;
      return yield* io.readFile("/nope");
    });
    const exit = await runExit(withFs(fs, missing));
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      const err = Cause.firstFail(exit.cause)?.value as FileSystemError;
      expect(err).toBeInstanceOf(FileSystemError);
      expect(err.op).toBe("readFile");
      expect(err.path).toBe("/nope");
    }
  });

  test("write / append / exists / stat round-trip", async () => {
    const fs = new TestFileSystem();
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      yield* io.writeFile("/a.txt", "one");
      yield* io.appendFile("/a.txt", "-two");
      const contents = yield* io.readFile("/a.txt");
      const there = yield* io.exists("/a.txt");
      const absent = yield* io.exists("/b.txt");
      const stat = yield* io.stat("/a.txt");
      return { contents, there, absent, size: stat.size, isFile: stat.isFile };
    });

    expect(await run(withFs(fs, program))).toEqual({
      contents: "one-two",
      there: true,
      absent: false,
      size: 7,
      isFile: true,
    });
  });

  test("appendFile creates the file when absent", async () => {
    const fs = new TestFileSystem();
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      yield* io.appendFile("/new.txt", "hello");
      return yield* io.readFile("/new.txt");
    });
    expect(await run(withFs(fs, program))).toBe("hello");
  });

  test("readDir lists immediate children only", async () => {
    const fs = new TestFileSystem({
      "/src/index.ts": "",
      "/src/lib/util.ts": "",
      "/src/lib/deep/x.ts": "",
      "/other.txt": "",
    });
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      return yield* io.readDir("/src");
    });
    expect(await run(withFs(fs, program))).toEqual(["index.ts", "lib"]);
  });

  test("remove recursive clears a subtree", async () => {
    const fs = new TestFileSystem({ "/a/one.txt": "1", "/a/b/two.txt": "2", "/keep.txt": "k" });
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      yield* io.remove("/a", { recursive: true });
      return {
        gone: yield* io.exists("/a/b/two.txt"),
        kept: yield* io.exists("/keep.txt"),
      };
    });
    expect(await run(withFs(fs, program))).toEqual({ gone: false, kept: true });
    expect(fs.snapshot()).toEqual({ "/keep.txt": "k" });
  });

  test("watch emits change events for writes", async () => {
    const fs = new TestFileSystem({ "/watched.txt": "v0" });

    const program = eff(function* () {
      const io = yield* FileSystem.get;
      const fiber = yield* io.watch("/watched.txt").take(2).toArray().fork();
      // Let the forked stream register its listener before we mutate.
      yield* yieldNow;
      yield* io.writeFile("/watched.txt", "v1");
      yield* io.writeFile("/watched.txt", "v2");
      return yield* join(fiber);
    });

    const events = (await run(withFs(fs, program))) as any[];
    expect(events).toHaveLength(2);
    expect(events.every((e) => e.path === "/watched.txt")).toBe(true);
    expect(events.every((e) => e.type === "change")).toBe(true);
  });

  test("watch stops delivering once the stream ends", async () => {
    const fs = new TestFileSystem({ "/w.txt": "" });
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      const fiber = yield* io.watch("/w.txt").take(1).toArray().fork();
      yield* yieldNow;
      yield* io.writeFile("/w.txt", "a");
      yield* join(fiber);
      // After the stream finished its finalizer must have removed the listener.
      yield* io.writeFile("/w.txt", "b");
      return true;
    });
    expect(await run(withFs(fs, program))).toBe(true);
  });

  test("snapshot exposes the whole store for assertions", async () => {
    const fs = new TestFileSystem();
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      yield* io.writeFile("/x", "1");
      yield* io.writeFile("/y", "2");
    });
    await run(withFs(fs, program));
    expect(fs.snapshot()).toEqual({ "/x": "1", "/y": "2" });
  });
});

describe("Console.readLine", () => {
  test("TestConsole returns scripted lines then undefined", async () => {
    const console = new TestConsole(["alpha", "beta"]);
    const program = eff(function* () {
      const io = yield* Console.get;
      return [yield* io.readLine(), yield* io.readLine(), yield* io.readLine()];
    });
    expect(await run(provide(program, Console, console) as any)).toEqual([
      "alpha",
      "beta",
      undefined,
    ]);
  });

  test("feed appends more input and remainingInput reflects consumption", async () => {
    const console = new TestConsole(["one"]);
    console.feed("two", "three");
    const program = eff(function* () {
      const io = yield* Console.get;
      return yield* io.readLine();
    });
    expect(await run(provide(program, Console, console) as any)).toBe("one");
    expect(console.remainingInput()).toEqual(["two", "three"]);
  });

  test("a read loop terminates at end of input", async () => {
    const console = new TestConsole(["a", "b", "c"]);
    const program = eff(function* () {
      const io = yield* Console.get;
      const seen: string[] = [];
      while (true) {
        const line = yield* io.readLine();
        if (line === undefined) break;
        seen.push(line);
      }
      return seen;
    });
    expect(await run(provide(program, Console, console) as any)).toEqual(["a", "b", "c"]);
  });

  test("clear() drops pending input too", async () => {
    const console = new TestConsole(["x"]);
    console.clear();
    expect(console.remainingInput()).toEqual([]);
  });
});

describe("RealFileSystem", () => {
  const root = `${process.env.TMPDIR ?? "/tmp"}/perfect-fs-test-${process.pid}`;

  test("round-trips through the real filesystem", async () => {
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      yield* io.mkdir(`${root}/nested`, { recursive: true });
      yield* io.writeFile(`${root}/nested/a.txt`, "hello");
      yield* io.appendFile(`${root}/nested/a.txt`, " world");

      const contents = yield* io.readFile(`${root}/nested/a.txt`);
      const bytes = yield* io.readFileBytes(`${root}/nested/a.txt`);
      const stat = yield* io.stat(`${root}/nested/a.txt`);
      const entries = yield* io.readDir(`${root}/nested`);
      const present = yield* io.exists(`${root}/nested/a.txt`);
      const absent = yield* io.exists(`${root}/nope.txt`);

      yield* io.remove(root, { recursive: true });
      const gone = yield* io.exists(`${root}/nested/a.txt`);

      return { contents, byteLen: bytes.length, size: stat.size, entries, present, absent, gone };
    });

    expect(await run(provide(program, FileSystem, realFileSystem) as any)).toEqual({
      contents: "hello world",
      byteLen: 11,
      size: 11,
      entries: ["a.txt"],
      present: true,
      absent: false,
      gone: false,
    });
  });

  test("a missing file is a typed FileSystemError, not a defect", async () => {
    const program = eff(function* () {
      const io = yield* FileSystem.get;
      return yield* io.readFile(`${root}/definitely-not-here`);
    });
    const exit = await runExit(provide(program, FileSystem, realFileSystem) as any);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBeInstanceOf(FileSystemError);
      expect(Cause.hasDie(exit.cause)).toBe(false);
    }
  });
});
