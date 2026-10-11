import { describe, expect, test } from "bun:test";
import {
  acquireDisposable,
  acquireRelease,
  fail,
  runDisposable,
  scoped,
  sleep,
  succeed,
  sync,
} from "../src";

describe("runDisposable", () => {
  // `await using` itself is tested in disposable-using-syntax.test.ts; here
  // the dispose call it makes is written out, so this runs on any runtime.
  test("keeps the scope open until disposed", async () => {
    const events: string[] = [];
    const resource = acquireRelease(
      sync(() => {
        events.push("open");
        return "connection";
      }),
      () => sync(() => void events.push("close")),
    );
    const held = await runDisposable(resource);
    events.push(`use ${held.value}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(events).toEqual(["open", "use connection"]);
    await held[Symbol.asyncDispose]();
    expect(events).toEqual(["open", "use connection", "close"]);
  });

  test("releases in reverse order", async () => {
    const events: string[] = [];
    const step = (name: string) =>
      acquireRelease(
        sync(() => void events.push(`open ${name}`)),
        () => sync(() => void events.push(`close ${name}`)),
      );
    const held = await runDisposable(step("a").flatMap(() => step("b")));
    await held[Symbol.asyncDispose]();
    expect(events).toEqual(["open a", "open b", "close b", "close a"]);
  });

  test("an effect that fails rejects, with what it opened already closed", async () => {
    const events: string[] = [];
    const failing = acquireRelease(
      sync(() => void events.push("open")),
      () => sync(() => void events.push("close")),
    ).flatMap(() => fail("no"));
    await expect(runDisposable(failing.catch(() => fail(new Error("no"))).orDie())).rejects.toThrow(
      "no",
    );
    expect(events).toEqual(["open", "close"]);
  });

  test("a release that fails makes disposing reject", async () => {
    const held = await runDisposable(
      acquireRelease(succeed(1), () =>
        sync(() => {
          throw new Error("release failed");
        }),
      ),
    );
    await expect(held[Symbol.asyncDispose]()).rejects.toThrow("release failed");
  });

  test("an aborted signal closes it early; disposing afterwards is fine", async () => {
    const controller = new AbortController();
    const events: string[] = [];
    const held = await runDisposable(
      acquireRelease(succeed("x"), () => sync(() => void events.push("close"))),
      { signal: controller.signal },
    );
    controller.abort();
    await sleep(5).run();
    expect(events).toEqual(["close"]);
    await held[Symbol.asyncDispose]();
  });
});

describe("acquireDisposable", () => {
  test("disposes a Disposable when the scope closes", async () => {
    const events: string[] = [];
    const value = await scoped(
      acquireDisposable(
        sync(() => ({
          name: "handle",
          [Symbol.dispose]: () => void events.push("disposed"),
        })),
      ).map((handle) => {
        events.push(`use ${handle.name}`);
        return handle.name;
      }),
    ).run();
    expect(value).toBe("handle");
    expect(events).toEqual(["use handle", "disposed"]);
  });

  test("prefers Symbol.asyncDispose and waits for it", async () => {
    const events: string[] = [];
    await scoped(
      acquireDisposable(
        sync(() => ({
          [Symbol.dispose]: () => void events.push("sync dispose"),
          [Symbol.asyncDispose]: async () => {
            await new Promise((resolve) => setTimeout(resolve, 5));
            events.push("async dispose");
          },
        })),
      ),
    ).run();
    expect(events).toEqual(["async dispose"]);
  });

  // DisposableStack is missing from older runtimes (Node 22).
  test.skipIf(typeof DisposableStack !== "function")(
    "works with objects made for using, like a DisposableStack",
    async () => {
      const events: string[] = [];
      await scoped(
        acquireDisposable(
          sync(() => {
            const stack = new DisposableStack();
            stack.defer(() => void events.push("first"));
            stack.defer(() => void events.push("second"));
            return stack;
          }),
        ),
      ).run();
      expect(events).toEqual(["second", "first"]);
    },
  );
});
