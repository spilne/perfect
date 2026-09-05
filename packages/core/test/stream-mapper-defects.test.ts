import { expect, test } from "bun:test";
import { runFiber, sync, SyncScheduler } from "../src";
import { Stream } from "../src/stream";

for (const method of ["parEvalMap", "parEvalMapUnordered"] as const) {
  test(`${method} propagates callback defects and finalizes the source`, () => {
    const scheduler = new SyncScheduler();
    const error = new Error("mapper failed");
    let finalized = 0;
    const fiber = runFiber(
      Stream.of(1, 2)
        .onFinalize(
          sync(() => {
            finalized++;
          }),
        )
        [method](2, () => {
          throw error;
        })
        .toArray(),
      scheduler,
    );
    scheduler.flush();
    expect(fiber.result).toEqual({ ok: false, cause: { _tag: "Die", defect: error } });
    expect(finalized).toBe(1);
    expect(fiber.childCount).toBe(0);
  });
}
