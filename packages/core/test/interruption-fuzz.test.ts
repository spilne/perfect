// Seeded interruption fuzz. Each iteration builds a random program from
// finalizers, scopes, masks, error handlers, generators, concurrency operators
// and parallel stream stages over manually driven leaves (gates, promises,
// TestClock sleeps, deferreds), then runs a random sequence of scheduler
// steps, leaf completions and interrupts against it and checks the invariants
// below.
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
  forEachPar,
  async,
  die,
  eff,
  ensuring,
  fail,
  failCause,
  fork,
  interruptible,
  join,
  onExit,
  provide,
  race,
  runFiber,
  runSync,
  scoped,
  succeed,
  suspend,
  sync,
  timeoutOption,
  tryPromise,
  uninterruptible,
  uninterruptibleMask,
  yieldNow,
} from "../src";
import { Stream } from "../src/stream";
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

// A finalizer ("fin": ensuring or onExit), a release ("rel"), a generator's
// `finally` block ("genFinally"), or a group that finalizers run for: a scope
// or a fiber. A `finally` block only runs as a finalizer when the fiber is
// interrupted; otherwise it is ordinary interruptible code, so it may be cut
// short and is not tracked as in progress.
type NodeKind = "fin" | "rel" | "genFinally" | "scope" | "fiber";

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
    runSync(queue.offer(item).orDie());
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
  // A one-resource pool whose validate may reject a reused resource or wait
  // on a gate, and whose release may wait on a gate or fail. Uses of it from
  // parallel branches wait for each other, which puts a waiter behind a use
  // that is releasing a rejected resource.
  let validatingCreated = 0;
  // The final shutdown runs outside the scheduler, so its releases neither
  // wait nor fail.
  let validatingPoolClosing = false;
  const validatingReleased = new Map<number, number>();
  const validatingPool = runSync(
    Pool.make<number>({
      size: 1,
      acquire: sync(() => {
        if (validatingCreated - validatingReleased.size >= 1) {
          violate("the validating pool held more resources than its size");
        }
        return ++validatingCreated;
      }),
      release: (resource) =>
        suspend(() => {
          const record = sync(() => {
            validatingReleased.set(resource, (validatingReleased.get(resource) ?? 0) + 1);
          });
          if (validatingPoolClosing) return record;
          switch (ri(4)) {
            case 0:
              return gateLeaf({ cancellable: false, inFinalizer: true }).flatMap(() => record);
            case 1:
              return record.flatMap(() => die("release failed"));
            default:
              return record;
          }
        }),
      validate: () =>
        suspend(() => {
          switch (ri(4)) {
            case 0:
              return gateLeaf({ cancellable: true, inFinalizer: false }).map(() => ri(2) === 0);
            case 1:
              return succeed(false);
            default:
              return succeed(true);
          }
        }),
    }),
  );
  const gates: Gate[] = [];
  const promises: ManualPromise[] = [];
  const nodes: Node[] = [];
  const started = new Set<number>();
  const inProgress = new Set<Node>();
  const fibers = new Set<Fiber<any>>();
  // Effects the program itself runs on a child fiber (all, race, timeout,
  // fork). Only those fibers are interrupted directly; a stream's internal
  // driver and worker fibers are only ever interrupted through the stream.
  const childEffects = new WeakSet<object>();
  const interruptibleChildren = new Set<Fiber<any>>();
  const generatorRuns: Array<{ started: number; finished: number }> = [];
  const child = (effect: Eff<any, any>): Eff<any, any> => {
    childEffects.add(effect);
    return effect;
  };
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
    if (node.kind !== "genFinally") inProgress.add(node);
  };
  // Called from inside error handlers. The only fiber running is the one
  // executing the handler; it must not be interrupted and interruptible.
  const checkHandlerMayRun = () => {
    const running = [...fibers].filter((fiber) => fiber.status === "running");
    if (running.length === 1 && running[0]!.interrupting && running[0]!.interruptible) {
      violate("an error handler ran in an interrupted, interruptible fiber");
    }
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
    switch (ri(12)) {
      case 10: {
        // Interrupted at once when the finalizer runs for an interrupted fiber.
        const holder = { gate: null as Gate | null };
        return {
          effect: interruptible(gateLeaf({ cancellable: true, inFinalizer: true, holder })),
          finished: () => holder.gate?.fired === true,
        };
      }
      case 11: {
        const holder = { gate: null as Gate | null };
        return {
          effect: uninterruptibleMask((restore) =>
            restore(gateLeaf({ cancellable: true, inFinalizer: true, holder })),
          ),
          finished: () => holder.gate?.fired === true,
        };
      }
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

  const generatorNode = (ctx: Ctx, depth: number): Eff<any, any> => {
    const node = makeNode("genFinally", ctx.ancestors);
    const inner: Ctx = {
      ...ctx,
      ancestors: [...ctx.ancestors, { id: node.id, crossFiber: false }],
    };
    const { ancestors } = ctx;
    const body = generate(depth - 1, inner);
    const { effect, finished } = finalizerWait();
    return eff(function* () {
      try {
        yield* sync(() => {
          checkRunsInside(ancestors, "generator body");
          node.entered++;
        });
        yield* body;
      } finally {
        yield* sync(() => finalizerStarted(node));
        yield* effect;
        yield* sync(() => {
          if (!finished()) violate("finally wait returned before its event fired");
          node.dones++;
        });
      }
    });
  };

  const streamNode = (ctx: Ctx, depth: number): Eff<any, any> => {
    const items = 1 + ri(3);
    const failAt = ri(items + 2);
    const sourceCtx = childCtx(ctx, { awaited: false });
    const source = Stream.unfoldEffect(0, (n: number) =>
      leaf(sourceCtx).flatMap(() =>
        n === failAt
          ? fail("S")
          : n >= items
            ? succeed(null)
            : succeed([n, n + 1] as [number, number]),
      ),
    );
    const concurrency = 1 + ri(3);
    // Each item runs on its own worker fiber, built when the item arrives.
    const worker = () => generate(depth - 1, childCtx(ctx, { awaited: false }));
    const stage =
      ri(2) === 0
        ? source.parEvalMap(concurrency, worker)
        : source.parEvalMapUnordered(concurrency, worker);
    return stage.drain();
  };

  // forEachPar over child programs, from an array, from a generator that may
  // throw, or with an f that may throw or fail. Children are awaited like
  // all()'s. Every generator it starts must finish: run out, throw, or be
  // closed when the traversal stops early.
  const forEachParNode = (ctx: Ctx, depth: number): Eff<any, any> => {
    const count = 1 + ri(4);
    const bodies = Array.from({ length: count }, () =>
      child(generate(depth - 1, childCtx(ctx, { awaited: true }))),
    );
    const choice = ri(4);
    const concurrency = choice === 3 ? ("unbounded" as const) : choice + 1;
    const stopAt = ri(count + 2);
    switch (ri(3)) {
      case 0:
        return forEachPar(bodies, (body) => body, { concurrency });
      case 1: {
        const iteration = { started: 0, finished: 0 };
        generatorRuns.push(iteration);
        const items = {
          *[Symbol.iterator]() {
            iteration.started++;
            try {
              for (let i = 0; i < count; i++) {
                if (i === stopAt) throw new Error("iterator failed");
                yield bodies[i]!;
              }
            } finally {
              iteration.finished++;
            }
          },
        };
        return forEachPar(items, (body) => body, { concurrency });
      }
      default:
        return forEachPar(
          bodies,
          (body, index) => {
            if (index !== stopAt) return body;
            if (ri(2) === 0) throw new Error("f failed");
            return fail("F");
          },
          { concurrency },
        );
    }
  };

  const generate = (depth: number, ctx: Ctx): Eff<any, any> => {
    if (depth <= 0 || nodeBudget <= 0 || ri(5) === 0) return leaf(ctx);
    nodeBudget--;
    switch (ri(26)) {
      case 0:
      case 1:
        return generate(depth - 1, ctx).flatMap(() => generate(depth - 1, ctx));
      case 19:
      case 20:
      case 21:
      case 22:
      case 23: {
        if (ctx.holdsShared) return generate(depth - 1, ctx);
        // One use leaves a resource idle, then two parallel uses contend for
        // it: one validates the reused resource while the other waits.
        const use = (body: Eff<any, any>) =>
          validatingPool.use(() => body).catchAllCause(() => succeed(0));
        const holding = (child: Ctx): Ctx => ({ ...child, holdsShared: true });
        return use(leaf(holding(ctx))).flatMap(() =>
          all([
            child(use(generate(depth - 1, holding(childCtx(ctx, { awaited: true }))))),
            child(use(generate(depth - 1, holding(childCtx(ctx, { awaited: true }))))),
          ]),
        );
      }
      case 2:
        return generate(depth - 1, ctx).catch(() => {
          checkHandlerMayRun();
          return succeed(0);
        });
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
          child(generate(depth - 1, childCtx(ctx, { awaited: true }))),
          child(generate(depth - 1, childCtx(ctx, { awaited: true }))),
        ]);
      case 9:
        return race([
          child(generate(depth - 1, childCtx(ctx, { awaited: true }))),
          child(generate(depth - 1, childCtx(ctx, { awaited: true }))),
        ]);
      case 10:
        return timeoutOption(
          child(generate(depth - 1, childCtx(ctx, { awaited: true }))),
          1 + ri(80),
        );
      case 11:
        return fork(child(generate(depth - 1, childCtx(ctx, { awaited: false }))))
          .flatMap((fiber) => join(fiber))
          .catch(() => succeed(0));
      case 12:
        return generate(depth - 1, ctx).catchAllCause(() => {
          checkHandlerMayRun();
          return succeed(0);
        });
      case 13:
        return generate(depth - 1, ctx).exit();
      case 15:
        return withPermit(depth, ctx);
      case 16:
        return withResource(depth, ctx);
      case 17:
        return generatorNode(ctx, depth);
      case 18:
        return streamNode(ctx, depth);
      case 24:
      case 25:
        return forEachParNode(ctx, depth);
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
    onStart: (fiber) => {
      fibers.add(fiber);
      const effect = (fiber as any).current;
      if (typeof effect === "object" && effect !== null && childEffects.has(effect)) {
        interruptibleChildren.add(fiber);
      }
    },
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
        const live = [...interruptibleChildren].filter((f) => f.status !== "done");
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
    if (node.kind === "genFinally" && node.entered > 0 && node.starts === 0) {
      violate("generator body entered but its finally never ran");
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
  for (const iteration of generatorRuns) {
    if (iteration.finished !== iteration.started) {
      violate("a forEachPar iterator was left open");
    }
  }
  if (runSync(validatingPool.inUse) !== 0) {
    violate("validating pool resources still in use after the program ended");
  }
  validatingPoolClosing = true;
  runSync(validatingPool.shutdown());
  for (let resource = 1; resource <= validatingCreated; resource++) {
    const count = validatingReleased.get(resource) ?? 0;
    if (count !== 1) violate(`a validating pool resource was released ${count} times`);
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
