import { expect, test } from "bun:test";
import { runFiber, runSync, SyncScheduler } from "../src";
import { InProcessDeferred, type DeferredState } from "../src/deferred";

test("canceled waits release their registrations while the deferred stays pending", () => {
  const deferred = new InProcessDeferred<number, string>();
  const scheduler = new SyncScheduler();
  const active = runFiber(deferred.await.orDie(), scheduler);
  scheduler.flush();
  for (let i = 0; i < 10_000; i++) {
    const canceled = runFiber(deferred.await.orDie(), scheduler);
    scheduler.flush();
    canceled.interrupt();
    scheduler.flush();
  }
  const state = Reflect.get(deferred, "state") as DeferredState<number, string>;
  expect(state._tag).toBe("Pending");
  if (state._tag === "Pending") expect(state.waiters.size).toBe(1);
  runSync(deferred.succeed(42));
  scheduler.flush();
  expect(active.result).toEqual({ ok: true, value: 42 });
  if (state._tag === "Pending") expect(state.waiters.size).toBe(0);
});

test("canceling one waiter preserves failure delivery to the remaining waiters", () => {
  const deferred = new InProcessDeferred<number, string>();
  const scheduler = new SyncScheduler();
  const first = runFiber(
    deferred.await.catch((error) => {
      throw error;
    }),
    scheduler,
  );
  const canceled = runFiber(deferred.await.orDie(), scheduler);
  const last = runFiber(deferred.await.orDie(), scheduler);
  scheduler.flush();
  canceled.interrupt();
  scheduler.flush();
  runSync(deferred.fail("failed"));
  scheduler.flush();
  expect(canceled.interrupted).toBe(true);
  expect(first.result).toEqual({ ok: false, cause: { _tag: "Die", defect: "failed" } });
  expect(last.result).toEqual(first.result);
});
