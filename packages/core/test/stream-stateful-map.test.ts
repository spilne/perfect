import { describe, expect, test } from "bun:test";
import { fromPromise, run } from "../src";
import { InMemoryState } from "../src/connect";
import { Stream } from "../src/stream";

describe("Stream.statefulMap", () => {
  test("preserves the pure accumulator overload", async () => {
    const values = await run(
      Stream.fromArray([10, 20, 30])
        .statefulMap(0, (index, value) => [index + 1, `${index}:${value}`] as const)
        .toArray(),
    );

    expect(values).toEqual(["0:10", "1:20", "2:30"]);
  });

  test("runs effect-typed keyed processing against a StateBackend", async () => {
    const state = new InMemoryState<string, number>();
    const values = await run(
      Stream.fromArray([
        { user: "a", amount: 2 },
        { user: "b", amount: 5 },
        { user: "a", amount: 3 },
      ])
        .statefulMap({
          stateBackend: state,
          keyBy: (event) => event.user,
          process: (event, backend, key) =>
            fromPromise(
              async () => {
                const total = (await backend.get(key)) ?? 0;
                const next = total + event.amount;
                await backend.put(key, next);
                return { user: key, total: next };
              },
              (cause) => new Error("state update failed", { cause }),
            ),
        })
        .toArray()
        .orDie(),
    );

    expect(values).toEqual([
      { user: "a", total: 2 },
      { user: "b", total: 5 },
      { user: "a", total: 5 },
    ]);
  });
});
