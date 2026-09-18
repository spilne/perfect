// Seeded interruption fuzz. Each iteration builds a random program from
// finalizers, scopes, masks, error handlers and concurrency operators over
// manually driven leaves (gates, promises, TestClock sleeps, deferreds), then
// runs a random sequence of scheduler steps, leaf completions and interrupts
// against it and checks the invariants below.
//
//   INTERRUPTION_FUZZ_ITERATIONS=50000 bun test test/interruption-fuzz.test.ts
//   INTERRUPTION_FUZZ_SEED=7 INTERRUPTION_FUZZ_ONLY=123 INTERRUPTION_FUZZ_VERBOSE=1 ...
import { expect, test } from "bun:test";
import {
  type Eff,
  type Fiber,
  Cause,
  Clock,
  Pool,
  Queue,
  Semaphore,
  TestClock,
  acquireRelease,
  addFiberSupervisor,
  all,
  async,
  die,
  ensuring,
  fail,
  failCause,
  fork,
  join,
  onExit,
  provide,
  race,
  runFiber,
  runSync,
  scoped,
  succeed,
  sync,
  timeoutOption,
  tryPromise,
  uninterruptible,
  yieldNow,
} from "../src";
import { InProcessDeferred } from "../src/deferred";
import type { Scheduler } from "../src/scheduler";

const ITERATIONS = Number(process.env.INTERRUPTION_FUZZ_ITERATIONS ?? 5000);
const SEEDS = process.env.INTERRUPTION_FUZZ_SEED
  ? [Number(process.env.INTERRUPTION_FUZZ_SEED)]
  : [1, 2, 3];
const ONLY = process.env.INTERRUPTION_FUZZ_ONLY;
const VERBOSE = Boolean(process.env.INTERRUPTION_FUZZ_VERBOSE);

function mulberry32(seed: number): () => number {
  let a = seed | 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

class StepScheduler implements Scheduler {
  readonly queue: Array<() => void> = [];

  schedule(task: () => void): void {
    this.queue.push(task);
  }

  step(): void {
    this.queue.shift()?.();
  }

  flush(): void {
    let steps = 0;
    while (this.queue.length > 0) {
      if (++steps > 5_000_000) throw new Error("scheduler livelock");
      this.queue.shift()!();
    }
  }

  shutdown(): void {
    this.queue.length = 0;
  }
}

const CHAINS: Eff<number, never>[] = [350, 690, 1100].map((length) => {
  let chain: Eff<number, never> = succeed(0);
  for (let i = 0; i < length; i++) chain = chain.flatMap((x) => succeed(x));
  return chain;
});

// A finalizer ("fin": ensuring or onExit), a release ("rel"), or a group
// that finalizers run for: a scope or a fiber.
type NodeKind = "fin" | "rel" | "scope" | "fiber";

// Enclosing groups of a piece of code. A cross-fiber ancestor is outside a
// fork: a parent does not wait for a forked fiber before running its own
// finalizers, so those orderings are not checked. Children of all(), race()
// and timeoutOption() are awaited, so their ancestors are checked like code on
// the same fiber: no parent finalizer starts before the children are done.
interface Ancestor {
  readonly id: number;
  readonly crossFiber: boolean;
}

interface Node {
  readonly id: number;
  readonly kind: NodeKind;
  readonly ancestors: readonly Ancestor[];
  readonly group: number;
  starts: number;
  dones: number;
  entered: number;
}

interface Ctx {
  readonly ancestors: readonly Ancestor[];
  readonly scopeGroup: number;
  // Code holding a permit or a pool resource takes neither again, so programs
  // cannot deadlock on the shared semaphore and pool.
  readonly holdsShared: boolean;
}

interface Gate {
  readonly resume: (effect: Eff<any, any>) => void;
  readonly inFinalizer: boolean;
  cancelled: boolean;
  fired: boolean;
}

interface ManualPromise {
  readonly resolve: (value: number) => void;
  readonly reject: (error: unknown) => void;
  readonly inFinalizer: boolean;
  settled: boolean;
}

// Violation kind -> "seed:iteration" pairs that hit it.
type Violations = Map<string, string[]>;

async function runIteration(params: {
  iteration: number;
  seed: number;
  violations: Violations;
}): Promise<void> {
  const { iteration, seed, violations } = params;
  const rnd = mulberry32(seed * 100_003 + iteration);
  const ri = (n: number) => Math.floor(rnd() * n);
  const log = (...args: unknown[]) => {
    if (VERBOSE) console.log(...args);
  };
  const seen = new Set<string>();
  const violate = (kind: string, detail?: string) => {
    log("VIOLATION", kind, detail ?? "");
    if (seen.has(kind)) return;
    seen.add(kind);
    const hits = violations.get(kind) ?? [];
    hits.push(`${seed}:${iteration}`);
    violations.set(kind, hits);
  };

  const scheduler = new StepScheduler();
  const clock = new TestClock();
  const deferreds = [0, 1].map(() => ({ deferred: new InProcessDeferred<number>(), done: false }));

  // Shared primitives whose items, permits and resources must be conserved.
  const PERMITS = 2;
  const semaphore = runSync(Semaphore.make(PERMITS));
  let permitHolders = 0;
  const QUEUE_CAPACITY = 2;
  const queue = runSync(Queue.bounded<number>(QUEUE_CAPACITY));
  let nextItem = 0;
  // Items offered by the program or from outside, and whether that offer
  // reported success; an interrupted offer may or may not have enqueued.
  const offers = new Map<number, boolean>();
  // How often each item came out of the queue: taken by the program, drained
  // from outside, or left over at the end.
  const received = new Map<number, number>();
  const receive = (item: number) => {
    received.set(item, (received.get(item) ?? 0) + 1);
  };
  const offerFromOutside = () => {
    if (runSync(queue.size) >= QUEUE_CAPACITY) return;
    const item = nextItem++;
    offers.set(item, false);
    runSync(queue.offer(item));
    offers.set(item, true);
  };
  const drainFromOutside = () => {
    for (const item of runSync(queue.takeAll())) receive(item);
  };
  const POOL_SIZE = 2;
  let createdResources = 0;
  const releasedResources = new Map<number, number>();
  const resourcesInUse = new Set<number>();
  const pool = runSync(
    Pool.make<number>({
      size: POOL_SIZE,
      acquire: sync(() => ++createdResources),
      release: (resource) =>
        sync(() => {
          releasedResources.set(resource, (releasedResources.get(resource) ?? 0) + 1);
        }),
    }),
  );
  const gates: Gate[] = [];
  const promises: ManualPromise[] = [];
  const nodes: Node[] = [];
  const started = new Set<number>();
  const inProgress = new Set<Node>();
  const fibers = new Set<Fiber<any>>();
  const ends = new Map<Fiber<any>, number>();
  let nodeBudget = 28;

  const makeNode = (kind: NodeKind, ancestors: readonly Ancestor[], group = -1): Node => {
    const node: Node = {
      id: nodes.length,
      kind,
      ancestors,
      group,
      starts: 0,
      dones: 0,
      entered: 0,
    };
    nodes.push(node);
    return node;
  };
  const checkRunsInside = (ancestors: readonly Ancestor[], where: string) => {
    for (const ancestor of ancestors) {
      if (!ancestor.crossFiber && started.has(ancestor.id)) {
        violate(
          "code ran after its enclosing finalizer, scope close or fiber completion started",
          where,
        );
      }
    }
  };
  const checkNothingRunningInside = (groupId: number, isFiber: boolean) => {
    for (const running of inProgress) {
      const ancestor = running.ancestors.find((a) => a.id === groupId);
      if (ancestor !== undefined) {
        if (!ancestor.crossFiber) violate("outer finalizer started while an inner one was running");
      } else if (isFiber && running.group === groupId && running.kind === "rel") {
        violate("fiber completed while its fiber-scope release was running");
      }
    }
  };
  const finalizerStarted = (node: Node) => {
    node.starts++;
    if (node.starts > 1) violate(`${node.kind} started twice`);
    checkRunsInside(node.ancestors, `${node.kind} ${node.id}`);
    const groupId = node.kind === "rel" ? node.group : node.id;
    if (!started.has(groupId)) {
      started.add(groupId);
      checkNothingRunningInside(groupId, false);
    }
    inProgress.add(node);
  };
  const finalizerDone = (node: Node) => {
    node.dones++;
    if (node.dones > node.starts) violate("finalizer finished without starting");
    inProgress.delete(node);
  };

  const gateLeaf = (params: {
    cancellable: boolean;
    inFinalizer: boolean;
    holder?: { gate: Gate | null };
  }): Eff<any, any> =>
    async<any, any>((resume) => {
      const gate: Gate = {
        resume,
        inFinalizer: params.inFinalizer,
        cancelled: false,
        fired: false,
      };
      if (params.holder) params.holder.gate = gate;
      gates.push(gate);
      if (params.cancellable) {
        return () => {
          gate.cancelled = true;
        };
      }
    });
  const promiseLeaf = (params: {
    inFinalizer: boolean;
    holder?: { promise: ManualPromise | null };
  }): Eff<any, any> =>
    tryPromise(
      () =>
        new Promise<number>((resolve, reject) => {
          const promise = { resolve, reject, inFinalizer: params.inFinalizer, settled: false };
          if (params.holder) params.holder.promise = promise;
          promises.push(promise);
        }),
      (e) => e,
    );
  const timedSleep = (ms: number, holder: { deadline: number }): Eff<any, any> =>
    sync(() => {
      holder.deadline = clock.now() + ms;
    }).flatMap(() => clock.sleep(ms));

  // What a finalizer waits on, and how to tell that the wait really finished.
  const finalizerWait = (): { effect: Eff<any, any>; finished: () => boolean } => {
    switch (ri(10)) {
      case 0:
        return { effect: succeed(0), finished: () => true };
      case 1:
      case 2: {
        const holder = { gate: null as Gate | null };
        return {
          effect: gateLeaf({ cancellable: ri(2) === 0, inFinalizer: true, holder }),
          finished: () => holder.gate?.fired === true,
        };
      }
      case 3: {
        const holder = { deadline: Infinity };
        return {
          effect: timedSleep(1 + ri(40), holder),
          finished: () => clock.now() >= holder.deadline,
        };
      }
      case 4: {
        const holder = { promise: null as ManualPromise | null };
        return {
          effect: promiseLeaf({ inFinalizer: true, holder }),
          finished: () => holder.promise?.settled === true,
        };
      }
      case 5:
        return { effect: yieldNow, finished: () => true };
      case 6:
        return { effect: CHAINS[ri(CHAINS.length)]!, finished: () => true };
      case 7: {
        const entry = deferreds[ri(2)]!;
        return { effect: entry.deferred.await, finished: () => entry.done };
      }
      case 8: {
        const holder = { gate: null as Gate | null };
        const sleep = { deadline: Infinity };
        return {
          effect: all([
            gateLeaf({ cancellable: true, inFinalizer: true, holder }),
            timedSleep(1 + ri(20), sleep),
          ]),
          finished: () => holder.gate?.fired === true && clock.now() >= sleep.deadline,
        };
      }
      default: {
        const holder = { gate: null as Gate | null };
        const sleep = { deadline: Infinity };
        return {
          effect: gateLeaf({ cancellable: ri(2) === 0, inFinalizer: true, holder }).flatMap(() =>
            timedSleep(1 + ri(10), sleep),
          ),
          finished: () => holder.gate?.fired === true && clock.now() >= sleep.deadline,
        };
      }
    }
  };
  const finalizer = (node: Node): Eff<any, any> => {
    const { effect, finished } = finalizerWait();
    return sync(() => finalizerStarted(node))
      .flatMap(() => effect)
      .flatMap(() =>
        sync(() => {
          if (!finished()) violate("finalizer wait returned before its event fired");
          finalizerDone(node);
        }),
      )
      .catchAllCause((cause) => sync(() => finalizerDone(node)).flatMap(() => failCause(cause)));
  };

  const leaf = (ctx: Ctx): Eff<any, any> => {
    let effect: Eff<any, any>;
    switch (ri(13)) {
      case 0:
      case 9:
        effect = gateLeaf({ cancellable: true, inFinalizer: false });
        break;
      case 1:
        effect = gateLeaf({ cancellable: false, inFinalizer: false });
        break;
      case 2:
        effect = promiseLeaf({ inFinalizer: false });
        break;
      case 3:
      case 10:
        effect = clock.sleep(1 + ri(60));
        break;
      case 4:
        effect = CHAINS[ri(CHAINS.length)]!;
        break;
      case 5:
        effect = yieldNow;
        break;
      case 6:
        effect = deferreds[ri(2)]!.deferred.await;
        break;
      case 7:
        effect = fail("E");
        break;
      case 11: {
        const item = nextItem++;
        // The map records the result in the same step the offer returns it.
        effect = sync(() => void offers.set(item, false))
          .flatMap(() => queue.offer(item))
          .map(() => void offers.set(item, true));
        break;
      }
      case 12:
        effect = queue.take().map(receive);
        break;
      default:
        effect = ri(4) === 0 ? die("D") : succeed(0);
    }
    const { ancestors } = ctx;
    return sync(() => checkRunsInside(ancestors, "leaf start"))
      .flatMap(() => effect)
      .flatMap(() => sync(() => checkRunsInside(ancestors, "leaf end")));
  };

  const childCtx = (ctx: Ctx, params: { awaited: boolean }): Ctx => {
    const group = makeNode(
      "fiber",
      params.awaited ? ctx.ancestors : ctx.ancestors.map((a) => ({ id: a.id, crossFiber: true })),
    );
    return {
      ancestors: [...group.ancestors, { id: group.id, crossFiber: false }],
      scopeGroup: group.id,
      holdsShared: ctx.holdsShared,
    };
  };

  const acquireReleaseNode = (ctx: Ctx): Eff<any, any> => {
    const owner = nodes[ctx.scopeGroup]!;
    const release = makeNode("rel", owner.ancestors, owner.id);
    const acquireWait =
      ri(3) === 0 ? gateLeaf({ cancellable: ri(2) === 0, inFinalizer: false }) : succeed(0);
    const { ancestors } = ctx;
    return acquireRelease(
      acquireWait.flatMap(() =>
        sync(() => {
          checkRunsInside(ancestors, "acquire");
          release.entered++;
          return release.id;
        }),
      ),
      () => finalizer(release),
    );
  };

  const finalizerNode = (params: {
    ctx: Ctx;
    depth: number;
    attach: (body: Eff<any, any>, fin: Eff<any, any>) => Eff<any, any>;
  }): Eff<any, any> => {
    const { ctx, depth, attach } = params;
    const node = makeNode("fin", ctx.ancestors);
    const inner: Ctx = {
      ...ctx,
      ancestors: [...ctx.ancestors, { id: node.id, crossFiber: false }],
    };
    const { ancestors } = ctx;
    return attach(
      sync(() => {
        checkRunsInside(ancestors, "finalized body");
        node.entered++;
      }).flatMap(() => generate(depth - 1, inner)),
      finalizer(node),
    );
  };

  const withPermit = (depth: number, ctx: Ctx): Eff<any, any> => {
    if (ctx.holdsShared) return uninterruptible(generate(depth - 1, ctx));
    let holding = false;
    return semaphore.withPermit(
      ensuring(
        sync(() => {
          holding = true;
          if (++permitHolders > PERMITS) violate("more permit holders than permits");
        }).flatMap(() => generate(depth - 1, { ...ctx, holdsShared: true })),
        sync(() => {
          if (holding) permitHolders--;
        }),
      ),
    );
  };

  const withResource = (depth: number, ctx: Ctx): Eff<any, any> => {
    if (ctx.holdsShared) return generate(depth - 1, ctx);
    return pool.use((resource) => {
      let holding = false;
      return ensuring(
        sync(() => {
          if (resourcesInUse.has(resource)) violate("a pool resource was used twice at once");
          if (releasedResources.has(resource)) violate("a released pool resource was used");
          resourcesInUse.add(resource);
          holding = true;
        }).flatMap(() => generate(depth - 1, { ...ctx, holdsShared: true })),
        sync(() => {
          if (holding) resourcesInUse.delete(resource);
        }),
      );
    });
  };

  const generate = (depth: number, ctx: Ctx): Eff<any, any> => {
    if (depth <= 0 || nodeBudget <= 0 || ri(5) === 0) return leaf(ctx);
    nodeBudget--;
    switch (ri(17)) {
      case 0:
      case 1:
        return generate(depth - 1, ctx).flatMap(() => generate(depth - 1, ctx));
      case 2:
        return generate(depth - 1, ctx).catch(() => succeed(0));
      case 3:
        return finalizerNode({ ctx, depth, attach: (body, fin) => ensuring(body, fin) });
      case 4:
        return finalizerNode({ ctx, depth, attach: (body, fin) => onExit(body, () => fin) });
      case 5: {
        const group = makeNode("scope", ctx.ancestors);
        const inner: Ctx = {
          ...ctx,
          ancestors: [...ctx.ancestors, { id: group.id, crossFiber: false }],
          scopeGroup: group.id,
        };
        return scoped(acquireReleaseNode(inner).flatMap(() => generate(depth - 1, inner)));
      }
      case 6:
        return acquireReleaseNode(ctx).flatMap(() => generate(depth - 1, ctx));
      case 7:
        return uninterruptible(generate(depth - 1, ctx));
      case 8:
        return all([
          generate(depth - 1, childCtx(ctx, { awaited: true })),
          generate(depth - 1, childCtx(ctx, { awaited: true })),
        ]);
      case 9:
        return race([
          generate(depth - 1, childCtx(ctx, { awaited: true })),
          generate(depth - 1, childCtx(ctx, { awaited: true })),
        ]);
      case 10:
        return timeoutOption(generate(depth - 1, childCtx(ctx, { awaited: true })), 1 + ri(80));
      case 11:
        return fork(generate(depth - 1, childCtx(ctx, { awaited: false })))
          .flatMap((fiber) => join(fiber))
          .catch(() => succeed(0));
      case 12:
        return generate(depth - 1, ctx).catchAllCause(() => succeed(0));
      case 13:
        return generate(depth - 1, ctx).exit();
      case 15:
        return withPermit(depth, ctx);
      case 16:
        return withResource(depth, ctx);
      default:
        return finalizerNode({ ctx, depth, attach: (body, fin) => ensuring(body, fin) });
    }
  };

  const rootGroup = makeNode("fiber", []);
  const program = generate(4, {
    ancestors: [{ id: rootGroup.id, crossFiber: false }],
    scopeGroup: rootGroup.id,
    holdsShared: false,
  });

  const stopSupervisor = addFiberSupervisor({
    onStart: (fiber) => void fibers.add(fiber),
    onEnd: (fiber) => void ends.set(fiber, (ends.get(fiber) ?? 0) + 1),
  });

  let bodyDone = false;
  let interruptedBeforeBodyDone = false;
  let root: Fiber<any> | undefined;
  try {
    root = runFiber(
      provide(
        program
          .flatMap(() =>
            sync(() => {
              bodyDone = true;
            }),
          )
          .catchAllCause((cause) =>
            sync(() => {
              bodyDone = true;
            }).flatMap(() => failCause(cause)),
          ),
        Clock,
        clock,
      ) as Eff<unknown, never>,
      scheduler,
    );
    const rootFiber = root;
    rootFiber.onComplete(() => {
      started.add(rootGroup.id);
      checkNothingRunningInside(rootGroup.id, true);
    });

    const settleMicrotasks = async () => {
      for (let i = 0; i < 3; i++) await Promise.resolve();
    };
    const openGate = (gate: Gate, allowFailure: boolean) => {
      gate.fired = true;
      const r = rnd();
      gate.resume(
        !allowFailure || gate.inFinalizer || r < 0.8
          ? succeed(1)
          : r < 0.95
            ? fail("G")
            : die("GD"),
      );
    };
    const interruptRoot = () => {
      if (rootFiber.status !== "done" && !bodyDone) interruptedBeforeBodyDone = true;
      rootFiber.interrupt();
    };

    const actions = ri(90);
    for (let i = 0; i < actions; i++) {
      const action = ri(100);
      log(`action ${i}: ${action} root=${rootFiber.status}`);
      if (action < 42) {
        scheduler.step();
      } else if (action < 57) {
        const open = gates.filter((g) => !g.fired && !g.cancelled);
        if (open.length > 0) openGate(open[ri(open.length)]!, true);
      } else if (action < 64) {
        const open = promises.filter((p) => !p.settled);
        if (open.length > 0) {
          const promise = open[ri(open.length)]!;
          promise.settled = true;
          if (!promise.inFinalizer && ri(5) === 0) promise.reject("P");
          else promise.resolve(1);
          await settleMicrotasks();
        }
      } else if (action < 71) {
        clock.advance(1 + ri(50));
      } else if (action < 73) {
        const entry = deferreds[ri(2)]!;
        if (!entry.done) {
          entry.done = true;
          runSync(entry.deferred.succeed(1));
        }
      } else if (action < 80) {
        interruptRoot();
      } else if (action < 88) {
        const live = [...fibers].filter((f) => f !== rootFiber && f.status !== "done");
        if (live.length > 0) live[ri(live.length)]!.interrupt();
      } else if (action < 90) {
        scheduler.flush();
      } else if (action < 94) {
        offerFromOutside();
      } else if (action < 96) {
        drainFromOutside();
      } else {
        await settleMicrotasks();
      }
    }
    if (ri(2) === 0) interruptRoot();

    for (let round = 0; round < 2000; round++) {
      let progressed = false;
      if (scheduler.queue.length > 0) {
        scheduler.flush();
        progressed = true;
      }
      for (const gate of gates.slice()) {
        if (!gate.fired && !gate.cancelled) {
          openGate(gate, false);
          progressed = true;
        }
      }
      for (const promise of promises.slice()) {
        if (!promise.settled) {
          promise.settled = true;
          promise.resolve(1);
          progressed = true;
        }
      }
      if (clock.pendingCount > 0) {
        clock.advance(10_000_000);
        progressed = true;
      }
      for (const entry of deferreds) {
        if (!entry.done) {
          entry.done = true;
          runSync(entry.deferred.succeed(1));
          progressed = true;
        }
      }
      await settleMicrotasks();
      // Takes and offers of the program wait on each other; feed and drain
      // the queue from outside until every fiber has completed.
      if (!progressed && [...fibers].some((fiber) => fiber.status !== "done")) {
        drainFromOutside();
        offerFromOutside();
        progressed = scheduler.queue.length > 0;
      }
      if (!progressed && scheduler.queue.length === 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
        const more =
          scheduler.queue.length > 0 ||
          gates.some((g) => !g.fired && !g.cancelled) ||
          promises.some((p) => !p.settled) ||
          clock.pendingCount > 0;
        if (!more) break;
      }
    }
  } catch (error) {
    violate("interpreter or driver threw", String(error));
  } finally {
    stopSupervisor();
  }

  if (root === undefined) return;
  if (root.status !== "done") violate("root fiber never completed");
  for (const fiber of fibers) {
    if (fiber.status !== "done") violate("a fiber never completed");
    else if ((ends.get(fiber) ?? 0) !== 1) violate("supervisor onEnd did not fire exactly once");
  }
  for (const node of nodes) {
    if (node.kind === "fin") {
      if (node.entered > 0 && node.starts === 0)
        violate("finalized body entered but finalizer never ran");
      if (node.dones < node.starts) violate("finalizer started but never finished");
    }
    if (node.kind === "rel") {
      if (node.entered > 0 && node.starts === 0) violate("resource acquired but never released");
      if (node.starts > 0 && node.entered === 0) violate("release ran without an acquire");
      if (node.dones < node.starts) violate("release started but never finished");
    }
  }
  if (runSync(semaphore.available) !== PERMITS) {
    violate("semaphore permits were not all returned");
  }
  drainFromOutside();
  for (const [item, count] of received) {
    if (count > 1) violate("a queue item was received more than once");
    if (!offers.has(item)) violate("a queue item was received that was never offered");
  }
  for (const [item, succeeded] of offers) {
    if (succeeded && received.get(item) !== 1)
      violate("a successfully offered queue item was lost");
  }
  if (runSync(pool.inUse) !== 0) violate("pool resources still in use after the program ended");
  runSync(pool.shutdown());
  for (let resource = 1; resource <= createdResources; resource++) {
    const count = releasedResources.get(resource) ?? 0;
    if (count !== 1) violate(`a pool resource was released ${count} times`);
  }
  if (interruptedBeforeBodyDone && root.result !== null) {
    if (root.result.ok || !Cause.hasInterrupt(root.result.cause)) {
      violate(
        "root interrupted before its body finished, but its result is not an interruption",
        root.result.ok ? "ok" : Cause.pretty(root.result.cause),
      );
    }
  }
}

test("interruption invariants hold across seeded random programs", async () => {
  const violations: Violations = new Map();
  let unhandled = 0;
  const onUnhandled = () => void unhandled++;
  process.on("unhandledRejection", onUnhandled);
  try {
    for (const seed of SEEDS) {
      if (ONLY !== undefined) {
        await runIteration({ iteration: Number(ONLY), seed, violations });
        continue;
      }
      for (let iteration = 0; iteration < ITERATIONS; iteration++) {
        await runIteration({ iteration, seed, violations });
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }

  const report = Object.fromEntries(
    [...violations].map(([kind, hits]) => [kind, hits.slice(0, 8)]),
  );
  expect({ violations: report, unhandled }).toEqual({ violations: {}, unhandled: 0 });
}, 120_000);
