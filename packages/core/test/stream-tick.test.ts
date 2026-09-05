import { describe, test, expect } from "bun:test";
import { run, provide, sync, Clock, TestClock, Stream } from "../src";

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("Stream.tick", () => {
  test("emits once per interval on virtual time", async () => {
    const c = new TestClock();
    const done = run(provide(Stream.tick(100).take(3).toArray(), Clock, c));
    for (let i = 0; i < 5; i++) {
      await tick();
      c.advance(100);
    }
    const result = await done;
    expect(result).toEqual([undefined, undefined, undefined]);
    expect(c.now()).toBeLessThanOrEqual(500);
  });

  test("does not emit before the first interval elapses", async () => {
    const c = new TestClock();
    let emitted = 0;
    const done = run(
      provide(
        Stream.tick(1000)
          .take(1)
          .forEach(() =>
            sync(() => {
              emitted++;
            }),
          ),
        Clock,
        c,
      ),
    );
    await tick();
    expect(emitted).toBe(0);
    c.advance(1000);
    await done;
    expect(emitted).toBe(1);
  });
});
