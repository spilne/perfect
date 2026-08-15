// Coverage gaps: Exit.mapError, Fiber.childrenSnapshot, and the standalone
// Layer.memoize function (the .memoize() method is covered in layer.test.ts).

import { describe, test, expect } from "bun:test";
import {
  eff,
  run,
  runFiber,
  sync,
  fork,
  join,
  service,
  Deferred,
  Exit,
  Cause,
  Layer,
  type Eff,
  type Fiber,
} from "../src";

const tick = () => new Promise((r) => setTimeout(r, 0));

describe("Exit.mapError", () => {
  test("passes Success through unchanged (same object)", () => {
    const e = Exit.succeed(42);
    const mapped = Exit.mapError(e, (err: never) => err);
    expect(mapped).toBe(e);
  });

  test("maps a plain Fail error", () => {
    const e = Exit.fail("boom");
    const mapped = Exit.mapError(e, (s) => s.toUpperCase());
    expect(mapped._tag).toBe("Failure");
    if (mapped._tag === "Failure") {
      expect(Cause.firstFail(mapped.cause)?.value).toBe("BOOM");
    }
  });

  test("maps every Fail leaf in a Both cause, preserving structure", () => {
    const e = Exit.failure(Cause.both(Cause.fail(1), Cause.fail(2)));
    const mapped = Exit.mapError(e, (n: number) => n * 10);
    expect(mapped._tag).toBe("Failure");
    if (mapped._tag === "Failure") {
      expect(mapped.cause._tag).toBe("Both");
      expect(Cause.failures(mapped.cause)).toEqual([10, 20]);
    }
  });

  test("leaves Die and Interrupt leaves untouched", () => {
    const defect = new Error("defect");
    const e = Exit.failure(Cause.then(Cause.die(defect), Cause.fail("x")));
    let calls = 0;
    const mapped = Exit.mapError(e, (s: string) => {
      calls++;
      return `mapped:${s}`;
    });
    expect(calls).toBe(1); // only the Fail leaf goes through f
    if (mapped._tag === "Failure") {
      expect(mapped.cause._tag).toBe("Then");
      if (mapped.cause._tag === "Then") {
        expect(mapped.cause.left).toEqual(Cause.die(defect));
        expect(Cause.firstFail(mapped.cause.right)?.value).toBe("mapped:x");
      }
    }

    const interrupted = Exit.interrupt();
    const mappedInterrupt = Exit.mapError(interrupted, () => "never");
    expect(Exit.isInterrupted(mappedInterrupt)).toBe(true);
  });
});

describe("Fiber.childrenSnapshot", () => {
  test("lists live forked children; empties once the parent completes", async () => {
    let captured: Fiber<any>[] = [];
    let gate: any = null;

    const program = (Deferred.make<void>() as any).flatMap((d: any) =>
      (fork(d.await) as any).flatMap((c1: any) =>
        (fork(d.await) as any).flatMap((c2: any) =>
          sync(() => {
            captured = [c1, c2];
            gate = d;
          })
            .flatMap(() => join(c1) as any)
            .flatMap(() => join(c2) as any)
            .map(() => "done"),
        ),
      ),
    );

    const root = runFiber(program as any);
    for (let i = 0; i < 10 && gate === null; i++) await tick();
    expect(gate).not.toBeNull();

    const snap = root.childrenSnapshot();
    expect(snap).toHaveLength(2);
    expect(snap).toContain(captured[0]!);
    expect(snap).toContain(captured[1]!);

    // snapshot is a copy — mutating it does not affect the fiber
    (snap as Fiber<any>[]).pop();
    expect(root.childCount).toBe(2);

    await run(gate.succeed(undefined) as any);
    const exit = await root.await();
    expect(exit._tag).toBe("Success");
    expect(root.childrenSnapshot()).toHaveLength(0);
  });
});

interface Db {
  query(sql: string): Eff<string, never>;
}
const Db = service<Db>("Db");

describe("Layer.memoize (standalone function)", () => {
  test("reuses one layer build within a scope", async () => {
    let builds = 0;
    const DbLive = Layer.memoize(
      sync(() => {
        builds++;
        return { Db: { query: (s: string) => sync(() => `db:${builds}:${s}`) } as Db };
      }) as any,
    );

    const program = eff(function* () {
      const db1 = yield* Db.get;
      const a = yield* db1.query("a");
      const db2 = yield* Db.get;
      const b = yield* db2.query("b");
      return [a, b];
    });

    const result = await run(program.with(Layer.merge(DbLive, DbLive) as any) as any);

    expect(result).toEqual(["db:1:a", "db:1:b"]);
    expect(builds).toBe(1);
  });

  test("memoization is per-scope, not global", async () => {
    let builds = 0;
    const DbLive = Layer.memoize(
      sync(() => {
        builds++;
        return { Db: { query: () => sync(() => `db:${builds}`) } as Db };
      }) as any,
    );

    const program = eff(function* () {
      const db = yield* Db.get;
      return yield* db.query("x");
    });

    expect(await run(program.with(DbLive as any) as any)).toBe("db:1");
    expect(await run(program.with(DbLive as any) as any)).toBe("db:2");
    expect(builds).toBe(2);
  });
});
