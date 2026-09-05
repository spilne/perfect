import { describe, test, expect } from "bun:test";
import { sync, sleep, fork, forkDaemon, run, Queue } from "../src";

describe("Queue", () => {
  test("unbounded offer + take", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q
        .offer(1)
        .flatMap(() =>
          q.offer(2).flatMap(() => q.take().flatMap((a) => q.take().map((b) => [a, b]))),
        ),
    );
    expect(await run(program.orDie())).toEqual([1, 2]);
  });

  test("take blocks until offer", async () => {
    const program = Queue.unbounded<string>().flatMap((q) =>
      fork(sleep(20).flatMap(() => q.offer("delayed"))).flatMap(() => q.take()),
    );
    expect(await run(program.orDie())).toBe("delayed");
  });

  test("bounded queue blocks offer when full", async () => {
    const log: string[] = [];
    const program = Queue.bounded<number>(2).flatMap((q) =>
      q.offer(1).flatMap(() =>
        q.offer(2).flatMap(() =>
          // third offer should block (capacity=2)
          fork(q.offer(3).flatMap(() => sync(() => log.push("offered:3")))).flatMap(() =>
            sleep(20).flatMap(() =>
              q.take().flatMap((a) =>
                sleep(20).flatMap(() =>
                  q.take().flatMap((b) =>
                    q.take().map((c) => {
                      log.push(`taken:${a},${b},${c}`);
                      return [a, b, c];
                    }),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
    expect(await run(program.orDie())).toEqual([1, 2, 3]);
  });

  test("takeAll", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q.offer(1).flatMap(() => q.offer(2).flatMap(() => q.offer(3).flatMap(() => q.takeAll()))),
    );
    expect(await run(program.orDie())).toEqual([1, 2, 3]);
  });

  test("size", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      q.offer(1).flatMap(() => q.offer(2).flatMap(() => q.size)),
    );
    expect(await run(program.orDie())).toBe(2);
  });

  test("interrupted take does not consume a later offer", async () => {
    const program = Queue.unbounded<number>().flatMap((q) =>
      forkDaemon(q.take()).flatMap((waiter) =>
        sleep(1).flatMap(() =>
          sync(() => waiter.interrupt()).flatMap(() => q.offer(123).flatMap(() => q.size)),
        ),
      ),
    );
    expect(await run(program.orDie())).toBe(1);
  });
});
