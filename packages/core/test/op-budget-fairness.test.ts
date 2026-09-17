import { describe, expect, test } from "bun:test";
import { type Eff, forEachPar, runFiber, succeed, suspend, sync, yieldNow } from "../src";
import { DEFAULT_BUDGET, type Scheduler } from "../src/scheduler";

// Runs every queued slice in order, like the default scheduler without timers.
class FifoScheduler implements Scheduler {
  readonly queue: Array<() => void> = [];

  schedule(task: () => void): void {
    this.queue.push(task);
  }

  flush(): void {
    while (this.queue.length > 0) this.queue.shift()!();
  }

  shutdown(): void {
    this.queue.length = 0;
  }
}

// The most callbacks `build`'s effect runs between two turns of a competing
// fiber that only yields.
function maxCallbacksBetweenTurns(build: (note: () => void) => Eff<unknown, never>): number {
  let turn = 0;
  let lastTurn = -1;
  let run = 0;
  let max = 0;
  let done = false;
  const note = () => {
    if (turn === lastTurn) run++;
    else {
      lastTurn = turn;
      run = 1;
    }
    max = Math.max(max, run);
  };
  const scheduler = new FifoScheduler();
  const fiber = runFiber(build(note), scheduler);
  fiber.onComplete(() => {
    done = true;
  });
  const spin = (): Eff<void, never> =>
    sync(() => {
      turn++;
      return done;
    }).flatMap((finished) => (finished ? succeed(undefined) : yieldNow.flatMap(() => spin())));
  runFiber(spin(), scheduler);
  scheduler.flush();
  return max;
}

// The most callbacks `build`'s effect runs within one scheduler task.
function maxCallbacksPerTask(build: (note: () => void) => Eff<unknown, never>): number {
  let run = 0;
  let max = 0;
  const note = () => {
    run++;
    if (run > max) max = run;
  };
  const scheduler = new FifoScheduler();
  runFiber(build(note), scheduler);
  while (scheduler.queue.length > 0) {
    run = 0;
    scheduler.queue.shift()!();
  }
  return max;
}

const N = 100_000;

describe("op budget fairness", () => {
  test("a deep left-nested .map chain yields to other fibers while unwinding", () => {
    const max = maxCallbacksBetweenTurns((note) => {
      let effect: Eff<number, never> = succeed(0);
      for (let i = 0; i < N; i++) {
        effect = effect.map((x) => {
          note();
          return x + 1;
        });
      }
      return effect;
    });
    expect(max).toBeLessThanOrEqual(2 * DEFAULT_BUDGET);
  });

  test("non-tail recursion with .map yields while unwinding", () => {
    const recurse = (n: number, note: () => void): Eff<number, never> =>
      n === 0
        ? succeed(0)
        : suspend(() => recurse(n - 1, note)).map((x) => {
            note();
            return x + 1;
          });
    expect(maxCallbacksBetweenTurns((note) => recurse(N, note))).toBeLessThanOrEqual(
      2 * DEFAULT_BUDGET,
    );
  });

  // forEachPar starts children inline, so its fill runs child slices inside
  // its own scheduler task. Measured per task: the fill spends one budget, and
  // the child it started last may use up one more.
  describe("forEachPar", () => {
    const items = (n: number) => Array.from({ length: n }, (_, i) => i);

    for (const concurrency of [1, 8, "unbounded"] as const) {
      test(`100k synchronous children yield (concurrency ${concurrency})`, () => {
        const max = maxCallbacksPerTask((note) =>
          forEachPar(items(N), (i) => sync(() => (note(), i)), { concurrency }),
        );
        expect(max).toBeLessThanOrEqual(2 * DEFAULT_BUDGET);
      });

      test(`100k already-successful items yield (concurrency ${concurrency})`, () => {
        const max = maxCallbacksPerTask((note) =>
          forEachPar(
            items(N),
            (i) => {
              note();
              return succeed(i);
            },
            { concurrency },
          ),
        );
        expect(max).toBeLessThanOrEqual(2 * DEFAULT_BUDGET);
      });

      test(`children unwinding long .map chains yield (concurrency ${concurrency})`, () => {
        const max = maxCallbacksPerTask((note) =>
          forEachPar(
            items(100),
            (i) => {
              let effect: Eff<number, never> = sync(() => i);
              for (let step = 0; step < 1000; step++) {
                effect = effect.map((x) => {
                  note();
                  return x;
                });
              }
              return effect;
            },
            { concurrency },
          ),
        );
        expect(max).toBeLessThanOrEqual(2 * DEFAULT_BUDGET);
      });
    }
  });
});
