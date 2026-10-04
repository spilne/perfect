import { describe, expect, test } from "bun:test";
import { Cause, Either, Stream, TaggedError, die, fail, run, runExit, sync } from "../src";

class SourceError extends TaggedError("SourceError")<{
  readonly message: string;
}>() {}

class OtherError extends TaggedError("OtherError")<{
  readonly message: string;
}>() {}

describe("Stream error algebra", () => {
  test("catch preserves emitted values and finalizes the source and recovery", async () => {
    let sourceFinalized = 0;
    let recoveryFinalized = 0;
    const source = Stream.of(1)
      .concat(Stream.fail(new SourceError({ message: "failed" })))
      .onFinalize(sync(() => void sourceFinalized++));

    const values = await run(
      source
        .catch((error) =>
          Stream.of(error.message.length).onFinalize(sync(() => void recoveryFinalized++)),
        )
        .toArray(),
    );

    expect(values).toEqual([1, 6]);
    expect(sourceFinalized).toBe(1);
    expect(recoveryFinalized).toBe(1);
  });

  test("catchTag handles only the selected tagged error", async () => {
    const handled = await run(
      Stream.fail(new SourceError({ message: "expected" }))
        .catchTag("SourceError", (error) => Stream.succeed(error.message))
        .toArray(),
    );
    expect(handled).toEqual(["expected"]);

    const exit = await runExit(
      Stream.fail(new OtherError({ message: "other" }))
        .catchTag("SourceError", () => Stream.succeed("wrong"))
        .toArray(),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toBeInstanceOf(OtherError);
    }
  });

  test("catchAllCause can materialize defects", async () => {
    const values = await run(
      Stream.fromEffect(die("defect"))
        .catchAllCause((cause) => Stream.succeed(Cause.pretty(cause)))
        .toArray(),
    );
    expect(values).toEqual(["Die(defect)"]);
  });

  test("mapError and tapError preserve the failure channel", async () => {
    const seen: string[] = [];
    const exit = await runExit(
      Stream.fail(new SourceError({ message: "source" }))
        .tapError((error) =>
          sync(() => {
            seen.push(error.message);
          }),
        )
        .mapError((error) => new OtherError({ message: error.message }))
        .toArray(),
    );

    expect(seen).toEqual(["source"]);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toEqual(new OtherError({ message: "source" }));
    }
  });

  test("either and attempt materialize typed errors after prior values", async () => {
    const source = Stream.of(1).concat(Stream.fail(new SourceError({ message: "nope" })));

    expect(await run(source.either().toArray())).toEqual([
      { _tag: "Right", right: 1 },
      { _tag: "Left", left: new SourceError({ message: "nope" }) },
    ]);
    expect(await run(source.attempt().toArray())).toEqual([
      { _tag: "Right", right: 1 },
      { _tag: "Left", left: new SourceError({ message: "nope" }) },
    ]);
  });

  test("exit and attemptCause materialize the full cause", async () => {
    const source = Stream.fromEffect(die("boom"));
    const exits = await run(source.exit().toArray());
    const attempts = await run(source.attemptCause().toArray());

    expect(exits).toHaveLength(1);
    expect(attempts).toHaveLength(1);
    expect(exits[0]?._tag).toBe("Failure");
    expect(attempts[0]?._tag).toBe("Failure");
    if (exits[0]?._tag === "Failure") expect(Cause.firstDie(exits[0].cause)?.value).toBe("boom");
  });

  test("rethrow emits values before the first Left, then fails with it", async () => {
    const pulled: number[] = [];
    const source = Stream.fromArray([1, 2, 3, 4])
      .tap((n) => pulled.push(n))
      .map((n) => (n === 3 ? Either.left(new SourceError({ message: "three" })) : Either.right(n)));
    const seen: number[] = [];
    const exit = await runExit(
      source
        .rethrow()
        .tap((n) => seen.push(n))
        .drain(),
    );

    expect(seen).toEqual([1, 2]);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.firstFail(exit.cause)?.value).toEqual(new SourceError({ message: "three" }));
    }
  });

  test("rethrow fails on a Left that opens a chunk", async () => {
    const exit = await runExit(
      Stream.of<Either<string, number>>(Either.left("first"), Either.right(1)).rethrow().toArray(),
    );
    expect(exit._tag === "Failure" && Cause.firstFail(exit.cause)?.value).toBe("first");
  });

  test("rethrow inverts either and exit across chunks", async () => {
    const source = Stream.fromArray([1, 2]).concat(Stream.fromArray([3]));
    expect(await run(source.either().rethrow().toArray())).toEqual([1, 2, 3]);
    expect(await run(source.exit().rethrow().toArray())).toEqual([1, 2, 3]);
  });

  test("rethrow keeps a Failure's whole cause", async () => {
    const exit = await runExit(Stream.fromEffect(die("boom")).exit().rethrow().drain());
    expect(exit._tag === "Failure" && Cause.firstDie(exit.cause)?.value).toBe("boom");
  });

  test("rethrow runs the source finalizer when it fails", async () => {
    let finalized = 0;
    const exit = await runExit(
      Stream.of<Either<string, number>>(Either.right(1), Either.left("stop"))
        .onFinalize(sync(() => void finalized++))
        .rethrow()
        .drain(),
    );
    expect(exit._tag).toBe("Failure");
    expect(finalized).toBe(1);
  });

  test("catchSome leaves unhandled failures intact", async () => {
    const exit = await runExit(
      Stream.fail(new OtherError({ message: "other" }))
        .catchSome((error: SourceError | OtherError) =>
          error instanceof SourceError ? Stream.succeed(error.message) : undefined,
        )
        .toArray(),
    );
    expect(exit._tag).toBe("Failure");
  });

  test("orDie turns typed failures into defects and keeps prior values and defects", async () => {
    const error = new SourceError({ message: "fatal" });
    let finalized = 0;
    const emitted: number[] = [];
    const exit = await runExit(
      Stream.of(1)
        .concat(Stream.fail(error))
        .onFinalize(sync(() => void finalized++))
        .orDie()
        .tap((value) => emitted.push(value))
        .drain(),
    );

    expect(emitted).toEqual([1]);
    expect(finalized).toBe(1);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.failures(exit.cause)).toEqual([]);
      expect(Cause.firstDie(exit.cause)?.value).toBe(error);
    }

    const defect = await runExit(Stream.fromEffect(die("boom")).orDie().drain());
    expect(defect._tag === "Failure" && Cause.firstDie(defect.cause)?.value).toBe("boom");
  });

  test("orDie also turns typed finalizer failures into defects", async () => {
    const error = new SourceError({ message: "release failed" });
    const exit = await runExit(Stream.of(1).onFinalize(fail(error)).orDie().drain());

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.failures(exit.cause)).toEqual([]);
      expect(Cause.firstDie(exit.cause)?.value).toBe(error);
    }
  });
});
