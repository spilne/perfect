import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  die,
  sync,
  sleep,
  acquireRelease,
  scoped,
  fork,
  join,
  run,
  runExit,
  Cause,
} from "../src";

describe("acquireRelease + scoped", () => {
  test("release runs on success", async () => {
    const log: string[] = [];

    const managed = acquireRelease(
      sync(() => {
        log.push("acquire");
        return "resource";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    );

    const program = scoped(managed.map((r) => `used:${r}`));

    expect(await run(program)).toBe("used:resource");
    expect(log).toEqual(["acquire", "release:resource"]);
  });

  test("release runs on failure", async () => {
    const log: string[] = [];

    const managed = acquireRelease(
      sync(() => {
        log.push("acquire");
        return "conn";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    );

    const program = scoped(managed.flatMap(() => fail("boom")));

    await expect(run(program)).rejects.toBe("boom");
    expect(log).toEqual(["acquire", "release:conn"]);
  });

  test("multiple resources release in reverse order", async () => {
    const log: string[] = [];

    const res = (name: string) =>
      acquireRelease(
        sync(() => {
          log.push(`acquire:${name}`);
          return name;
        }),
        (r) =>
          sync(() => {
            log.push(`release:${r}`);
          }),
      );

    const program = scoped(
      res("A").flatMap((a) => res("B").flatMap((b) => res("C").map((c) => [a, b, c]))),
    );

    expect(await run(program)).toEqual(["A", "B", "C"]);
    expect(log).toEqual([
      "acquire:A",
      "acquire:B",
      "acquire:C",
      "release:C",
      "release:B",
      "release:A",
    ]);
  });

  test("resource with async acquire", async () => {
    const log: string[] = [];

    const managed = acquireRelease(
      sleep(10).flatMap(() =>
        sync(() => {
          log.push("acquired");
          return "db";
        }),
      ),
      (r) =>
        sync(() => {
          log.push(`released:${r}`);
        }),
    );

    const program = scoped(managed.map((r) => `got:${r}`));
    expect(await run(program)).toBe("got:db");
    expect(log).toEqual(["acquired", "released:db"]);
  });

  test("release failure after scoped success becomes the scope failure", async () => {
    const program = scoped(acquireRelease(succeed("resource"), () => die("release failed")));

    const exit = await runExit(program);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.pretty(exit.cause)).toBe("Die(release failed)");
    }
  });

  test("release failure after scoped failure is sequentially composed", async () => {
    const program = scoped(
      acquireRelease(succeed("resource"), () => die("release failed")).flatMap(() =>
        fail("body failed"),
      ),
    );

    const exit = await runExit(program);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.pretty(exit.cause)).toBe("(Fail(body failed) ; Die(release failed))");
    }
  });
});

describe("scheduler interleaving", () => {
  test("fibers interleave on op budget", async () => {
    const log: string[] = [];

    // build a chain that exceeds the op budget
    const longChain = (name: string, n: number) => {
      let eff = sync(() => {
        log.push(`${name}:start`);
      });
      for (let i = 0; i < n; i++) {
        eff = eff.flatMap(() => succeed(undefined));
      }
      return eff.flatMap(() =>
        sync(() => {
          log.push(`${name}:end`);
        }),
      );
    };

    // 3000 ops each — should exceed 2048 budget and yield
    const program = fork(longChain("A", 3000)).flatMap((fiberA) =>
      fork(longChain("B", 3000)).flatMap((fiberB) => join(fiberA).flatMap(() => join(fiberB))),
    );

    await run(program);

    // both should start and end
    expect(log).toContain("A:start");
    expect(log).toContain("A:end");
    expect(log).toContain("B:start");
    expect(log).toContain("B:end");
  });

  test("structured concurrency — children cancel on parent complete", async () => {
    let childRan = false;

    const child = sleep(100).flatMap(() =>
      sync(() => {
        childRan = true;
      }),
    );
    const parent = fork(child).flatMap(() => succeed("done"));

    await run(parent);
    // parent completes, child gets interrupted
    await new Promise((r) => setTimeout(r, 150));
    expect(childRan).toBe(false);
  });
});

describe("acquireRelease without scoped()", () => {
  test("release runs at fiber completion on success", async () => {
    const log: string[] = [];

    const program = acquireRelease(
      sync(() => {
        log.push("acquire");
        return "res";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    ).map((r) => `used:${r}`);

    expect(await run(program)).toBe("used:res");
    expect(log).toEqual(["acquire", "release:res"]);
  });

  test("release runs at fiber completion on failure", async () => {
    const log: string[] = [];

    const program = acquireRelease(
      sync(() => {
        log.push("acquire");
        return "conn";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    ).flatMap(() => fail("boom"));

    await expect(run(program)).rejects.toBe("boom");
    expect(log).toEqual(["acquire", "release:conn"]);
  });

  test("release in a forked child runs when the child completes", async () => {
    const log: string[] = [];

    const child = acquireRelease(
      sync(() => {
        log.push("acquire");
        return "child-res";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    ).map(() => "child-done");

    const program = fork(child).flatMap((f) => join(f));

    expect(await run(program)).toBe("child-done");
    expect(log).toEqual(["acquire", "release:child-res"]);
  });

  test("release runs when the fiber is interrupted while suspended", async () => {
    const log: string[] = [];

    const child = acquireRelease(
      sync(() => {
        log.push("acquire");
        return "res";
      }),
      (r) =>
        sync(() => {
          log.push(`release:${r}`);
        }),
    ).flatMap(() => sleep(5000));

    // fork, give the child time to acquire and suspend in sleep, then let the
    // parent complete — structured cancellation interrupts the child
    const program = fork(child).flatMap(() => sleep(20));

    await run(program);
    await new Promise((r) => setTimeout(r, 20));
    expect(log).toEqual(["acquire", "release:res"]);
  });
});
