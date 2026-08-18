import { describe, expect, test } from "bun:test";
import { Cause, Stream, TaggedError, die, run, runExit, sync } from "../src";

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
      .onFinalize(sync(() => sourceFinalized++));

    const values = await run(
      source
        .catch((error) =>
          Stream.of(error.message.length).onFinalize(sync(() => recoveryFinalized++)),
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

  test("catchSome leaves unhandled failures intact", async () => {
    const exit = await runExit(
      Stream.fail(new OtherError({ message: "other" }))
        .catchSome((error) =>
          error instanceof SourceError ? Stream.succeed(error.message) : undefined,
        )
        .toArray(),
    );
    expect(exit._tag).toBe("Failure");
  });
});
