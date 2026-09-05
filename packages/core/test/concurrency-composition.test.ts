import { describe, test, expect } from "bun:test";
import { succeed, suspend, sleep, fork, all, run, Deferred, Queue, Semaphore, Ref } from "../src";

describe("concurrency composition", () => {
  test("producer/consumer with Queue + Semaphore", async () => {
    const program = Queue.bounded<number>(5).flatMap((q) =>
      Semaphore.make(2).flatMap((sem) =>
        Ref.make<number[]>([]).flatMap((results) => {
          const producer = all(Array.from({ length: 10 }, (_, i) => q.offer(i))).flatMap(() =>
            q.shutdown(),
          );

          const consumer = (function consume(): any {
            return q
              .take()
              .flatMap((n) => sem.withPermit(results.update((r) => [...r, n])))
              .flatMap(() => suspend(() => consume()))
              .catch(() => succeed(undefined)); // shutdown
          })();

          return fork(producer)
            .flatMap(() => fork(consumer))
            .flatMap(() => fork(consumer))
            .flatMap(() => sleep(200))
            .flatMap(() => results.get)
            .map((r) => r.sort((a, b) => a - b));
        }),
      ),
    );

    expect(await run(program)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  test("Deferred as coordination", async () => {
    const program = Deferred.make<void>().flatMap((gate) =>
      Ref.make("waiting").flatMap((state) => {
        const worker = gate.await.flatMap(() => state.set("started"));
        return fork(worker)
          .flatMap(() => sleep(20))
          .flatMap(() => state.get)
          .flatMap((before) =>
            gate
              .succeed(undefined)
              .flatMap(() => sleep(20).flatMap(() => state.get.map((after) => [before, after]))),
          );
      }),
    );
    expect(await run(program)).toEqual(["waiting", "started"]);
  });
});
