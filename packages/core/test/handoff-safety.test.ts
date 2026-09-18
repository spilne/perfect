import { describe, expect, test } from "bun:test";
import {
  type Eff,
  type Fiber,
  Cause,
  Chunk,
  Clock,
  Pool,
  PoolClosed,
  Queue,
  RetryPolicy,
  Semaphore,
  Stream,
  SyncScheduler,
  TestClock,
  all,
  async,
  die,
  ensuring,
  fail,
  forkDaemon,
  provide,
  race,
  run,
  runFiber,
  runSync,
  sleep,
  succeed,
  suspend,
  sync,
  timeoutOption,
  uninterruptible,
} from "../src";
import { DEFAULT_BUDGET, type Scheduler } from "../src/scheduler";

// Runs queued loop slices one at a time so a test can act between them.
class StepScheduler implements Scheduler {
  readonly queue: Array<() => void> = [];

  schedule(task: () => void): void {
    this.queue.push(task);
  }

  step(): void {
    this.queue.shift()?.();
  }

  flush(): void {
    while (this.queue.length > 0) this.queue.shift()!();
  }

  shutdown(): void {
    this.queue.length = 0;
  }
}

const interrupted = { ok: false, cause: { _tag: "Interrupt" } };

// Lengths of a flatMap chain around the one that spends the op budget, so a
// sweep puts the budget pause on every step just after the chain. Measured
// rather than computed, so it does not depend on which loop steps count.
function aroundBudget(radius: number): number[] {
  const probe = 200;
  let chain: Eff<unknown, never> = succeed(0);
  for (let i = 0; i < probe; i++) chain = chain.flatMap((x) => succeed(x));
  const scheduler = new SyncScheduler();
  const fiber = runFiber(chain, scheduler);
  scheduler.flush();
  const center = Math.round((DEFAULT_BUDGET * probe) / fiber.opCount);
  return Array.from({ length: 2 * radius + 1 }, (_, i) => center - radius + i);
}

// An async wait that exposes its resume, so a test decides when and with what
// it resumes.
function manualWait<A>(): {
  wait: Eff<A, never>;
  resume: (value: Eff<A, never>, onDiscard?: () => void) => void;
} {
  let resume: ((value: Eff<A, never>, onDiscard?: () => void) => void) | undefined;
  return {
    wait: async<A>((r) => {
      resume = r as typeof resume;
    }) as Eff<A, never>,
    resume: (value, onDiscard) => resume!(value, onDiscard),
  };
}

describe("async resume with onDiscard", () => {
  test("a fiber interrupted after the resume but before it runs discards the value once", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    let ran = 0;
    let discarded = 0;
    const fiber = runFiber(
      manual.wait.map((n) => {
        ran += n;
      }),
      scheduler,
    );
    scheduler.flush();

    manual.resume(
      sync(() => 1),
      () => void discarded++,
    );
    expect(fiber.status).toBe("ready");
    fiber.interrupt();
    fiber.interrupt();
    scheduler.flush();

    expect({ ran, discarded }).toEqual({ ran: 0, discarded: 1 });
    expect(fiber.result).toEqual(interrupted);
  });

  test("a value the fiber started running is never discarded", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    const later = manualWait<void>();
    let got = 0;
    let discarded = 0;
    const fiber = runFiber(
      manual.wait.flatMap((n) =>
        sync(() => {
          got = n;
        }).flatMap(() => later.wait),
      ),
      scheduler,
    );
    scheduler.flush();

    manual.resume(succeed(7), () => void discarded++);
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    expect({ got, discarded }).toEqual({ got: 7, discarded: 0 });
    expect(fiber.result).toEqual(interrupted);
  });

  test("a resume that arrives after the fiber stopped waiting is discarded at once", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    let discarded = 0;
    const fiber = runFiber(manual.wait, scheduler);
    scheduler.flush();
    fiber.interrupt();
    scheduler.flush();

    manual.resume(succeed(1), () => void discarded++);

    expect(discarded).toBe(1);
    expect(fiber.result).toEqual(interrupted);
  });

  test("only the first of two resumes is delivered; the second is discarded", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    const discarded: number[] = [];
    const fiber = runFiber(manual.wait, scheduler);
    scheduler.flush();

    manual.resume(succeed(1), () => void discarded.push(1));
    manual.resume(succeed(2), () => void discarded.push(2));
    scheduler.flush();

    expect(discarded).toEqual([2]);
    expect(fiber.result).toEqual({ ok: true, value: 1 });
  });

  test("an uninterruptible waiter receives the value and fails with the interrupt afterwards", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    let got = 0;
    let discarded = 0;
    const fiber = runFiber(
      uninterruptible(
        manual.wait.map((n) => {
          got = n;
        }),
      ),
      scheduler,
    );
    scheduler.flush();

    fiber.interrupt();
    manual.resume(succeed(3), () => void discarded++);
    fiber.interrupt();
    scheduler.flush();

    expect({ got, discarded }).toEqual({ got: 3, discarded: 0 });
    expect(fiber.result).toEqual(interrupted);
  });

  test("a resume whose run was dropped by scheduler.shutdown() is discarded on interrupt", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    let discarded = 0;
    const fiber = runFiber(
      manual.wait.map((n) => n + 1),
      scheduler,
    );
    scheduler.flush();

    manual.resume(succeed(1), () => void discarded++);
    scheduler.shutdown();
    expect(discarded).toBe(0);
    fiber.interrupt();
    scheduler.flush();

    expect(discarded).toBe(1);
    expect(fiber.result).toEqual(interrupted);
  });

  test("a fiber with nothing left to finalize completes on interrupt and still discards", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<number>();
    let discarded = 0;
    const fiber = runFiber(manual.wait, scheduler);
    scheduler.flush();

    manual.resume(succeed(1), () => void discarded++);
    fiber.interrupt();

    expect(discarded).toBe(1);
    expect(fiber.result).toEqual(interrupted);
  });

  test("an op-budget pause never separates a queue take's fast path from its continuation", () => {
    // The sweep moves the budget boundary across every step of the take and
    // the map that records its value.
    let pausedOnValue = 0;
    for (const length of aroundBudget(12)) {
      for (let pad = 0; pad < 3; pad++) {
        const scheduler = new StepScheduler();
        const queue = runSync(Queue.unbounded<number>());
        runSync(queue.offer(1));
        const taken: number[] = [];
        let body: Eff<unknown, never> = succeed(0);
        for (let i = 0; i < pad; i++) body = succeed(body) as Eff<unknown, never>;
        for (let i = 0; i < length; i++) body = body.flatMap((x) => succeed(x));
        const fiber = runFiber(
          body.flatMap(() => queue.take().map((n) => void taken.push(n))),
          scheduler,
        );
        scheduler.step();
        if (fiber.status === "ready") {
          if (fiber.valueInFlight) pausedOnValue++;
          fiber.interrupt();
        }
        scheduler.flush();

        expect({ length, pad, conserved: taken.length + runSync(queue.size) }).toEqual({
          length,
          pad,
          conserved: 1,
        });
      }
    }
    expect(pausedOnValue).toBeGreaterThan(0);
  });
});

describe("an interrupt during an op-budget pause on a value", () => {
  // Starts `build(chain)` with chains of increasing length until the first
  // slice ends paused on the chain's result, with only the `frames` frames
  // `build` added after the chain left to run.
  const pausedOnValue = (params: {
    frames: number;
    build: (chain: Eff<number, never>) => Eff<unknown, never>;
  }): { fiber: Fiber<unknown>; scheduler: StepScheduler } => {
    for (const length of aroundBudget(12)) {
      for (let pad = 0; pad < 3; pad++) {
        let chain: Eff<number, never> = succeed(0);
        for (let i = 0; i < length; i++) chain = chain.flatMap((x) => succeed(x));
        for (let i = 0; i < pad; i++)
          chain = chain.flatMap((x) => succeed(succeed(x)).flatMap((y) => y));
        const scheduler = new StepScheduler();
        const fiber = runFiber(params.build(chain), scheduler);
        scheduler.step();
        let depth = 0;
        for (let frame = fiber.stack; frame !== null; frame = frame.next) depth++;
        if (fiber.status === "ready" && fiber.valueInFlight && depth === params.frames) {
          return { fiber, scheduler };
        }
      }
    }
    throw new Error("no chain length paused on the chain's result");
  };

  test("lands at the next effect, after the value reached its continuation", () => {
    const log: string[] = [];
    const { fiber, scheduler } = pausedOnValue({
      frames: 2,
      build: (chain) =>
        chain
          .map((n) => {
            log.push(`received ${n}`);
          })
          .flatMap(() => sync(() => void log.push("next effect"))),
    });

    // Shorter chains tried before this one ran to completion.
    log.length = 0;
    fiber.interrupt();
    scheduler.flush();

    expect(log).toEqual(["received 0"]);
    expect(fiber.result).toEqual(interrupted);
  });

  test("lets a fiber whose value is its result complete normally", () => {
    const { fiber, scheduler } = pausedOnValue({
      frames: 1,
      build: (chain) => chain.map((n) => n + 1),
    });

    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: true, value: 1 });
  });

  test("still lands after scheduler.shutdown() dropped the paused run", () => {
    const log: string[] = [];
    const { fiber, scheduler } = pausedOnValue({
      frames: 2,
      build: (chain) =>
        chain.map(() => void log.push("received")).flatMap(() => sync(() => void log.push("next"))),
    });

    log.length = 0;
    scheduler.shutdown();
    fiber.interrupt();
    scheduler.flush();

    expect(log).toEqual(["received"]);
    expect(fiber.result).toEqual(interrupted);
  });
});

describe("interrupting a Ready fiber keeps the failure it was about to raise", () => {
  const defect = new Error("defect");

  test("an async resume with a defect, interrupted before the fiber runs", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<void>();
    const fiber = runFiber(manual.wait, scheduler);
    scheduler.flush();

    manual.resume(die(defect));
    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.die(defect), Cause.interrupt()),
    });
  });

  test("the same with finalizers still to run", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<void>();
    let finalized = 0;
    const fiber = runFiber(
      ensuring(
        manual.wait.map(() => "unreachable"),
        sync(() => void finalized++),
      ),
      scheduler,
    );
    scheduler.flush();

    manual.resume(fail("typed"));
    fiber.interrupt();
    scheduler.flush();

    expect(finalized).toBe(1);
    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.fail("typed"), Cause.interrupt()),
    });
  });

  test("a child defect all() delivered, interrupted before the parent runs", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<void>();
    const fiber = runFiber(
      all([manual.wait.flatMap(() => die(defect)), async<void>(() => () => {})]),
      scheduler,
    );
    scheduler.flush();

    manual.resume(succeed(undefined));
    while (fiber.status !== "ready" && scheduler.queue.length > 0) scheduler.step();
    expect(fiber.status).toBe("ready");
    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.die(defect), Cause.interrupt()),
    });
  });

  test("a failed race winner, interrupted before the parent runs", () => {
    const scheduler = new StepScheduler();
    const manual = manualWait<void>();
    const fiber = runFiber(
      race([manual.wait.flatMap(() => fail("lost")), async<void>(() => () => {})]).map(() => 0),
      scheduler,
    );
    scheduler.flush();

    manual.resume(succeed(undefined));
    while (fiber.status !== "ready" && scheduler.queue.length > 0) scheduler.step();
    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual({
      ok: false,
      cause: Cause.then(Cause.fail("lost"), Cause.interrupt()),
    });
  });

  test("an op-budget pause on a failure keeps it", () => {
    let pausedOnFailure = 0;
    for (const length of aroundBudget(12)) {
      for (let pad = 0; pad < 3; pad++) {
        const scheduler = new StepScheduler();
        let body: Eff<unknown, never> = succeed(0);
        for (let i = 0; i < pad; i++) body = succeed(body) as Eff<unknown, never>;
        for (let i = 0; i < length; i++) body = body.flatMap((x) => succeed(x));
        const fiber = runFiber(
          body.flatMap(() => die(defect)),
          scheduler,
        );
        scheduler.step();
        const pending = fiber.current;
        if (fiber.status === "ready") fiber.interrupt();
        scheduler.flush();

        const result = fiber.result!;
        expect(result.ok).toBe(false);
        if (pending instanceof Object && (pending as { op?: unknown }).op === die(defect).op) {
          pausedOnFailure++;
          expect(!result.ok && Cause.defects(result.cause)).toEqual([defect]);
        }
      }
    }
    expect(pausedOnFailure).toBeGreaterThan(0);
  });

  test("a failure the fiber already handled is not raised again by a later interrupt", () => {
    const scheduler = new StepScheduler();
    const first = manualWait<void>();
    const second = manualWait<void>();
    const fiber = runFiber(
      first.wait.catch(() => succeed(undefined)).flatMap(() => second.wait),
      scheduler,
    );
    scheduler.flush();
    first.resume(fail("handled") as Eff<void, never>);
    scheduler.flush();
    expect(fiber.status).toBe("suspended");

    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: false, cause: Cause.interrupt() });
  });
});

describe("Queue handoff", () => {
  test("an item handed to a taker interrupted before it runs goes back to the queue", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.unbounded<string>());
    const taker = runFiber(queue.take(), scheduler);
    scheduler.flush();

    runSync(queue.offer("a"));
    expect(taker.status).toBe("ready");
    taker.interrupt();
    scheduler.flush();

    expect(taker.result).toEqual(interrupted);
    expect(runSync(queue.size)).toBe(1);
    expect(runSync(queue.take())).toBe("a");
  });

  test("the item goes to the next waiting taker", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.unbounded<string>());
    const first = runFiber(queue.take(), scheduler);
    const second = runFiber(queue.take(), scheduler);
    scheduler.flush();

    runSync(queue.offer("a"));
    first.interrupt();
    scheduler.flush();

    expect(first.result).toEqual(interrupted);
    expect(second.result).toEqual({ ok: true, value: "a" });
    expect(runSync(queue.size)).toBe(0);
  });

  for (const order of ["in handoff order", "in reverse handoff order"]) {
    test(`items given back ${order} return to the queue in FIFO order`, () => {
      const scheduler = new StepScheduler();
      const queue = runSync(Queue.unbounded<number>());
      const takers = [0, 1, 2].map(() => runFiber(queue.take(), scheduler));
      scheduler.flush();

      for (const item of [1, 2, 3, 4, 5]) runSync(queue.offer(item));
      const interruptOrder = order === "in handoff order" ? takers : [...takers].reverse();
      for (const taker of interruptOrder) taker.interrupt();
      scheduler.flush();

      expect(runSync(queue.takeAll())).toEqual([1, 2, 3, 4, 5]);
    });
  }

  test("all([take, take]) interrupted after both were handed items keeps FIFO order", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.unbounded<number>());
    const parent = runFiber(all([queue.take(), queue.take()]), scheduler);
    scheduler.flush();

    runSync(queue.offer(1));
    runSync(queue.offer(2));
    runSync(queue.offer(3));
    parent.interrupt();
    scheduler.flush();

    expect(parent.result).toEqual(interrupted);
    expect(runSync(queue.takeAll())).toEqual([1, 2, 3]);
  });

  test("a given-back item handed on and given back again keeps its place", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.unbounded<number>());
    const first = runFiber(queue.take(), scheduler);
    const second = runFiber(queue.take(), scheduler);
    const third = runFiber(queue.take(), scheduler);
    scheduler.flush();

    runSync(queue.offer(1));
    runSync(queue.offer(2));
    // 1 goes on to the third taker; then both remaining handoffs come back.
    first.interrupt();
    third.interrupt();
    second.interrupt();
    runSync(queue.offer(3));
    scheduler.flush();

    expect(runSync(queue.takeAll())).toEqual([1, 2, 3]);
  });

  test("a bounded queue overshoots its capacity by at most the interrupted handoffs", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.bounded<number>(2));
    const takers = Array.from({ length: 5 }, () => runFiber(queue.take(), scheduler));
    scheduler.flush();
    for (let item = 0; item < 5; item++) runSync(queue.offer(item));
    runSync(queue.offer(10));
    runSync(queue.offer(11));
    const offerScheduler = new StepScheduler();
    const blocked = runFiber(queue.offer(12), offerScheduler);
    offerScheduler.flush();
    expect(blocked.status).toBe("suspended");

    for (const taker of takers) taker.interrupt();
    scheduler.flush();

    expect(runSync(queue.size)).toBe(2 + 5);
    expect(runSync(queue.takeAll())).toEqual([0, 1, 2, 3, 4, 10, 11, 12]);
  });

  test("a returned item keeps its place ahead of later offers", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.unbounded<number>());
    const taker = runFiber(queue.take(), scheduler);
    scheduler.flush();

    runSync(queue.offer(1));
    runSync(queue.offer(2));
    taker.interrupt();
    scheduler.flush();

    expect(runSync(queue.takeAll())).toEqual([1, 2]);
  });

  test("a returned item is still taken after the queue is closed", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.unbounded<number>());
    const taker = runFiber(queue.take(), scheduler);
    scheduler.flush();

    runSync(queue.offer(1));
    runSync(queue.close());
    taker.interrupt();
    scheduler.flush();

    expect(runSync(queue.take())).toBe(1);
  });

  test("a returned item can overfill a bounded queue; a blocked offer waits for room", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.bounded<number>(1));
    const taker = runFiber(queue.take(), scheduler);
    scheduler.flush();
    runSync(queue.offer(1));
    runSync(queue.offer(2));
    // Its own scheduler, so starting it does not also run the taker.
    const offerScheduler = new StepScheduler();
    const blocked = runFiber(queue.offer(3), offerScheduler);
    offerScheduler.flush();
    expect(blocked.status).toBe("suspended");

    taker.interrupt();
    scheduler.flush();
    expect(runSync(queue.size)).toBe(2);

    expect(runSync(queue.take())).toBe(1);
    expect(blocked.status).toBe("suspended");
    expect(runSync(queue.take())).toBe(2);
    offerScheduler.flush();
    expect(blocked.result).toEqual({ ok: true, value: true });
    expect(runSync(queue.takeAll())).toEqual([3]);
  });

  test("an offer admitted by a take and then interrupted enqueues its value once", () => {
    const scheduler = new StepScheduler();
    const queue = runSync(Queue.bounded<number>(1));
    runSync(queue.offer(1));
    const offerer = runFiber(queue.offer(2), scheduler);
    scheduler.flush();

    expect(runSync(queue.take())).toBe(1);
    expect(offerer.status).toBe("ready");
    offerer.interrupt();
    scheduler.flush();

    expect(offerer.result).toEqual(interrupted);
    expect(runSync(queue.takeAll())).toEqual([2]);
  });

  test("timeoutOption(take) losing a tie to its timer leaves the item in the queue", () => {
    const scheduler = new SyncScheduler();
    const clock = new TestClock();
    const queue = runSync(Queue.unbounded<number>());
    const fiber = runFiber(provide(timeoutOption(queue.take(), 5), Clock, clock), scheduler);
    scheduler.flush();

    // Both sides resume at the same instant, the timer first: it wins the
    // race and interrupts the take, whose fiber has not run yet.
    clock.advance(5);
    runSync(queue.offer(1));
    scheduler.flush();

    expect(fiber.result).toEqual({ ok: true, value: undefined });
    expect(runSync(queue.take())).toBe(1);
  });
});

describe("Semaphore handoff", () => {
  test("a permit granted at once to a fiber interrupted before it runs is returned", () => {
    const scheduler = new StepScheduler();
    const semaphore = runSync(Semaphore.make(1));
    const fiber = runFiber(semaphore.withPermit(sync(() => 1)), scheduler);
    scheduler.step();
    expect(fiber.status).toBe("ready");

    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
    expect(runSync(semaphore.available)).toBe(1);
  });

  test("a permit released to a waiter interrupted before it runs goes to the next waiter", () => {
    const scheduler = new StepScheduler();
    const semaphore = runSync(Semaphore.make(1));
    const release = manualWait<void>();
    const holder = runFiber(semaphore.withPermit(release.wait), scheduler);
    const waiter = runFiber(semaphore.withPermit(succeed("waiter")), scheduler);
    const next = runFiber(semaphore.withPermit(succeed("next")), scheduler);
    scheduler.flush();

    release.resume(succeed(undefined));
    scheduler.step();
    expect(holder.result).toEqual({ ok: true, value: undefined });
    expect(waiter.status).toBe("ready");
    waiter.interrupt();
    scheduler.flush();

    expect(waiter.result).toEqual(interrupted);
    expect(next.result).toEqual({ ok: true, value: "next" });
    expect(runSync(semaphore.available)).toBe(1);
  });

  test("weighted permits come back together", () => {
    const scheduler = new StepScheduler();
    const semaphore = runSync(Semaphore.make(3));
    const fiber = runFiber(semaphore.withPermits(2, succeed(1)), scheduler);
    scheduler.step();

    fiber.interrupt();
    scheduler.flush();

    expect(runSync(semaphore.available)).toBe(3);
  });

  test("an uninterruptible withPermit keeps a granted permit and releases it once", () => {
    const scheduler = new StepScheduler();
    const semaphore = runSync(Semaphore.make(1));
    let ran = 0;
    const fiber = runFiber(
      uninterruptible(semaphore.withPermit(sync(() => void ran++))),
      scheduler,
    );
    scheduler.step();

    fiber.interrupt();
    scheduler.flush();

    expect(ran).toBe(1);
    expect(fiber.result).toEqual(interrupted);
    expect(runSync(semaphore.available)).toBe(1);
  });
});

describe("Pool handoff", () => {
  const makePool = (params: {
    size: number;
    released?: number[];
    acquire?: Eff<number, never>;
    validate?: (resource: number) => Eff<boolean, never>;
  }) => {
    let created = 0;
    return runSync(
      Pool.make<number>({
        size: params.size,
        acquire: params.acquire ?? sync(() => ++created),
        release: (resource) =>
          sync(() => {
            params.released?.push(resource);
          }),
        validate: params.validate,
      }),
    );
  };

  test("an idle resource handed to a fiber interrupted before it runs stays idle", () => {
    const scheduler = new StepScheduler();
    const pool = makePool({ size: 1 });
    runSync(pool.use(succeed));
    const fiber = runFiber(pool.use(succeed), scheduler);
    scheduler.step();
    expect(fiber.status).toBe("ready");

    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
    expect({ inUse: runSync(pool.inUse), idle: runSync(pool.idle) }).toEqual({ inUse: 0, idle: 1 });
  });

  test("a released resource granted to an interrupted waiter goes to the next waiter", () => {
    const scheduler = new StepScheduler();
    const pool = makePool({ size: 1 });
    const release = manualWait<void>();
    const holder = runFiber(
      pool.use(() => release.wait),
      scheduler,
    );
    scheduler.flush();
    const waiter = runFiber(pool.use(succeed), scheduler);
    const next = runFiber(pool.use(succeed), scheduler);
    scheduler.flush();

    release.resume(succeed(undefined));
    scheduler.step();
    expect(holder.result?.ok).toBe(true);
    expect(waiter.status).toBe("ready");
    waiter.interrupt();
    scheduler.flush();

    expect(waiter.result).toEqual(interrupted);
    expect(next.result).toEqual({ ok: true, value: 1 });
    expect({ inUse: runSync(pool.inUse), idle: runSync(pool.idle) }).toEqual({ inUse: 0, idle: 1 });
  });

  test("a granted resource is released once when the pool shuts down before the interrupt", () => {
    const scheduler = new StepScheduler();
    const released: number[] = [];
    const pool = makePool({ size: 1, released });
    const release = manualWait<void>();
    runFiber(
      pool.use(() => release.wait),
      scheduler,
    );
    scheduler.flush();
    const waiter = runFiber(pool.use(succeed), scheduler);
    scheduler.flush();

    release.resume(succeed(undefined));
    scheduler.step();
    expect(waiter.status).toBe("ready");
    runSync(pool.shutdown());
    waiter.interrupt();
    scheduler.flush();

    expect(waiter.result).toEqual(interrupted);
    expect(released).toEqual([1]);
    expect({ inUse: runSync(pool.inUse), idle: runSync(pool.idle) }).toEqual({ inUse: 0, idle: 0 });
  });

  test("a failed create wakes the next waiter when the woken one was interrupted first", () => {
    const scheduler = new StepScheduler();
    let attempts = 0;
    const firstCreate = manualWait<number>();
    const pool = makePool({
      size: 1,
      acquire: suspend(() => (++attempts === 1 ? firstCreate.wait : succeed(attempts))),
    });
    const creator = runFiber(pool.use(succeed), scheduler);
    scheduler.flush();
    const woken = runFiber(pool.use(succeed), scheduler);
    const next = runFiber(pool.use(succeed), scheduler);
    scheduler.flush();

    firstCreate.resume(die("connect failed"));
    scheduler.step();
    expect(creator.result?.ok).toBe(false);
    expect(woken.status).toBe("ready");
    woken.interrupt();
    scheduler.flush();

    expect(woken.result).toEqual(interrupted);
    expect(next.result).toEqual({ ok: true, value: 2 });
    expect({ inUse: runSync(pool.inUse), idle: runSync(pool.idle) }).toEqual({ inUse: 0, idle: 1 });
  });

  test("a use interrupted while validating a reused resource returns it", () => {
    const scheduler = new StepScheduler();
    const check = manualWait<boolean>();
    let validating = false;
    const pool = makePool({
      size: 1,
      validate: () => (validating ? check.wait : succeed(true)),
    });
    runSync(pool.use(succeed));
    validating = true;
    const fiber = runFiber(pool.use(succeed), scheduler);
    scheduler.flush();
    expect(fiber.status).toBe("suspended");

    fiber.interrupt();
    scheduler.flush();

    expect(fiber.result).toEqual(interrupted);
    expect({ inUse: runSync(pool.inUse), idle: runSync(pool.idle) }).toEqual({ inUse: 0, idle: 1 });
  });

  test("a waiter still sees PoolClosed after shutdown", async () => {
    const pool = makePool({ size: 1 });
    const holder = await run(forkDaemon(pool.use(() => sleep(20))));
    await run(sleep(1));
    const waiting = run(pool.use(succeed));
    await run(sleep(1));
    await run(pool.shutdown());
    await expect(waiting).rejects.toBeInstanceOf(PoolClosed);
    holder.interrupt();
  });
});

describe("push-source bridges", () => {
  const pullAfterFirst = <A>(stream: Stream<A>) =>
    runSync(stream.step as Eff<any, never>) as { _tag: "Emit"; chunk: Chunk<A>; next: Stream<A> };

  test("Stream.fromCallback keeps a value handed to a pull interrupted before it runs", () => {
    const scheduler = new StepScheduler();
    let emit!: (value: number) => void;
    const stream = Stream.fromCallback<number>((e) => {
      emit = e;
      e(0);
    });
    const first = pullAfterFirst(stream);
    expect(first.chunk.toArray()).toEqual([0]);

    const pull = runFiber(first.next.step as Eff<any, never>, scheduler);
    scheduler.flush();
    emit(1);
    pull.interrupt();
    scheduler.flush();
    expect(pull.result).toEqual(interrupted);

    const retried = runSync(first.next.step as Eff<any, never>) as { chunk: Chunk<number> };
    expect(retried.chunk.toArray()).toEqual([1]);
  });

  test("Stream.async keeps a value handed to a pull interrupted before it runs", () => {
    const scheduler = new StepScheduler();
    let emit!: (value: number) => void;
    const stream = Stream.async<number, never>((e) =>
      sync(() => {
        emit = e;
        e(0);
      }),
    );
    const first = pullAfterFirst(stream);

    const pull = runFiber(first.next.step as Eff<any, never>, scheduler);
    scheduler.flush();
    emit(1);
    emit(2);
    pull.interrupt();
    scheduler.flush();

    const retried = runSync(first.next.step as Eff<any, never>) as { chunk: Chunk<number> };
    expect(retried.chunk.toArray()).toEqual([1, 2]);
  });

  test("Stream.asyncChunks keeps a chunk handed to a pull interrupted before it runs", () => {
    const scheduler = new StepScheduler();
    let emit!: (chunk: Chunk<number>) => void;
    const stream = Stream.asyncChunks<number, never>((e) =>
      sync(() => {
        emit = e;
        e(Chunk.fromArray([0]));
      }),
    );
    const first = pullAfterFirst(stream);

    const pull = runFiber(first.next.step as Eff<any, never>, scheduler);
    scheduler.flush();
    emit(Chunk.fromArray([1, 2]));
    pull.interrupt();
    scheduler.flush();

    const retried = runSync(first.next.step as Eff<any, never>) as { chunk: Chunk<number> };
    expect(retried.chunk.toArray()).toEqual([1, 2]);
  });
});

describe("stream operators at timer ties", () => {
  const runVirtual = <A>(effect: Eff<A, unknown>) => {
    const scheduler = new SyncScheduler();
    const clock = new TestClock();
    const fiber: Fiber<A> = runFiber(provide(effect, Clock, clock) as Eff<A, never>, scheduler);
    scheduler.flush();
    while (fiber.result === null && clock.now() < 500) {
      clock.advance(1);
      scheduler.flush();
    }
    return fiber.result;
  };

  test("debounce keeps a value that arrives as the quiet window closes", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .debounce(5);
    expect(runVirtual(stream.toArray())).toEqual({ ok: true, value: [1, 2] });
  });

  test("groupWithin keeps an item that arrives at the window deadline", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 2)))
      .groupWithin(10, 5)
      .map((group) => group.toArray());
    expect(runVirtual(stream.toArray())).toEqual({ ok: true, value: [[1], [2]] });
  });

  test("sample ends when its source ends exactly on a sampling boundary", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(20).map(() => 0)).filter(() => false))
      .sample(5);
    expect(runVirtual(stream.toArray())).toEqual({ ok: true, value: [1] });
  });

  test("audit ends when its source ends exactly as the window closes", () => {
    const stream = Stream.of(1)
      .concat(Stream.fromEffect(sleep(5).map(() => 0)).filter(() => false))
      .audit(5);
    expect(runVirtual(stream.toArray())).toEqual({ ok: true, value: [1] });
  });

  for (const timeoutMs of [3, 4, 5, 6]) {
    test(`fromQueue(...).timeout(${timeoutMs}).retry() loses nothing when arrivals tie with the timeout`, () => {
      const producer = (queue: Queue<number>) =>
        [0, 1, 2, 3]
          .reduce<Eff<unknown, never>>(
            (acc, n) => acc.flatMap(() => sleep(6)).flatMap(() => queue.offer(n).orDie()),
            succeed(undefined),
          )
          .flatMap(() => queue.close());
      const program = Queue.unbounded<number>().flatMap((queue) =>
        forkDaemon(producer(queue)).flatMap(() =>
          Stream.fromQueue(queue).timeout(timeoutMs).retry(RetryPolicy.recurs(1_000)).toArray(),
        ),
      );
      expect(runVirtual(program)).toEqual({ ok: true, value: [0, 1, 2, 3] });
    });
  }

  test("real clock: fromQueue(...).timeout(ms).retry() with arrivals on the timeout loses nothing", async () => {
    const program = Queue.unbounded<number>().flatMap((queue) =>
      forkDaemon(
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
          .reduce<Eff<unknown, never>>(
            (acc, n) => acc.flatMap(() => sleep(10)).flatMap(() => queue.offer(n).orDie()),
            succeed(undefined),
          )
          .flatMap(() => queue.close()),
      ).flatMap(() =>
        Stream.fromQueue(queue).timeout(10).retry(RetryPolicy.recurs(10_000)).toArray(),
      ),
    );
    expect(await run(program)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });
});
