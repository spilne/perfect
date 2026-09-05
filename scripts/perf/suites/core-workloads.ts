import {
  Clock,
  Deferred,
  RateLimiter,
  Stream,
  SyncScheduler,
  TestClock,
  provide,
  runFiber,
  runSync,
} from "../../../packages/core/src";
import { Fiber } from "../../../packages/core/src/fiber";

export function groupedSingletons(n: number): number {
  return runSync(
    Stream.unfold(0, (i) => (i < n ? [i, i + 1] : null))
      .grouped(n)
      .fold(0, (count, chunk) => count + chunk.length),
  );
}

export function fillSlidingWindow(n: number): number {
  const clock = new TestClock();
  const limiter = runSync(
    provide(RateLimiter.slidingWindow({ limit: n, windowMs: 1000 }), Clock, clock),
  );
  const acquire = provide(limiter.tryAcquire, Clock, clock);
  let accepted = 0;
  for (let i = 0; i < n; i++) if (runSync(acquire)) accepted++;
  return accepted;
}

export function completeChildren(n: number): number {
  const parent = new Fiber<number>();
  const children = Array.from({ length: n }, () => new Fiber<number>());
  for (const child of children) parent.addChild(child);
  for (let i = n - 1; i >= 0; i--) children[i]!.complete({ ok: true, value: i });
  return parent.childCount;
}

export function cancelDeferredWaiters(n: number): number {
  const deferred = runSync(Deferred.make<number>());
  const scheduler = new SyncScheduler();
  let canceled = 0;
  for (let i = 0; i < n; i++) {
    const fiber = runFiber(deferred.await.orDie(), scheduler);
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();
    if (fiber.interrupted) canceled++;
  }
  runSync(deferred.succeed(1));
  return canceled;
}
