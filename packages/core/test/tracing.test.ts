import { describe, test, expect } from "bun:test";
import { succeed, fail, sync, provide, run, runExit } from "../src";
import { Tracer, withSpan, currentSpan, TestTracer } from "../src";

describe("withSpan", () => {
  test("records span with attributes and ok status", async () => {
    const tracer = new TestTracer();
    const program = withSpan(succeed(42), "op", { route: "/x" });
    expect(await run(provide(program, Tracer, tracer) as any)).toBe(42);

    const span = tracer.find("op")!;
    expect(span.attributes).toEqual({ route: "/x" });
    expect(span.status).toEqual({ ok: true });
  });

  test("failure ends the span with error status and re-raises", async () => {
    const tracer = new TestTracer();
    const program = withSpan(fail("boom"), "failing-op");
    const exit = await runExit(provide(program, Tracer, tracer) as any);

    expect(exit._tag).toBe("Failure");
    const span = tracer.find("failing-op")!;
    expect(span.status).toEqual({ ok: false, error: "boom", interrupted: false });
  });

  test("nested spans record parentage; children end first", async () => {
    const tracer = new TestTracer();
    const program = withSpan(
      withSpan(succeed("inner-done"), "child").flatMap(() => succeed("outer-done")),
      "parent",
    );
    await run(provide(program, Tracer, tracer) as any);

    expect(tracer.finished.map((s) => s.name)).toEqual(["child", "parent"]);
    expect(tracer.find("child")!.parentName).toBe("parent");
    expect(tracer.find("parent")!.parentName).toBeNull();
  });

  test("currentSpan is null outside and set inside a span", async () => {
    const tracer = new TestTracer();
    const program = currentSpan.flatMap((outside) =>
      withSpan(
        currentSpan.map((inside) => ({ outside, inside: inside?.name ?? null })),
        "region",
      ),
    );
    const result = await run(provide(program, Tracer, tracer) as any);
    expect(result).toEqual({ outside: null, inside: "region" });
  });

  test("no-op tracer short-circuits — program runs unchanged", async () => {
    // default context has the noop tracer; withSpan should be transparent
    expect(await run(withSpan(succeed("plain"), "ignored") as any)).toBe("plain");
  });

  test("fluent .withSpan works", async () => {
    const tracer = new TestTracer();
    const program = (succeed(7) as any).withSpan("fluent-op");
    expect(await run(provide(program, Tracer, tracer) as any)).toBe(7);
    expect(tracer.find("fluent-op")!.status).toEqual({ ok: true });
  });

  test("throwing sync body ends span with error", async () => {
    const tracer = new TestTracer();
    const program = withSpan(
      sync(() => {
        throw new Error("defect!");
      }),
      "defective",
    );
    const exit = await runExit(provide(program, Tracer, tracer) as any);
    expect(exit._tag).toBe("Failure");
    const span = tracer.find("defective")!;
    expect(span.status!.ok).toBe(false);
  });
});
