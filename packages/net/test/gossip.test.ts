import { afterEach, describe, expect, test } from "bun:test";
import {
  Clock,
  Random,
  TestClock,
  TestRandom,
  eff,
  provide,
  runFiber,
  scoped,
  sleep,
  sync,
  type Eff,
  type Fiber,
} from "@spilne/perfect-core";
import {
  Gossip,
  SimulatedNetwork,
  Tcp,
  Transport,
  type Address,
  type GossipOptions,
  type MembershipEvent,
} from "../src";

type Meta = { readonly zone: string };

const timing = { intervalMs: 100, pingTimeoutMs: 30, suspicionMs: 500 };

/**
 * A cluster on a simulated network, run on a test clock. Each member runs
 * on its own fiber, so a test can stop one (it leaves) or cut it off (it
 * stops answering, as if it crashed).
 */
class Cluster {
  readonly clock = new TestClock();
  readonly members = new Map<string, Gossip<Meta>>();
  readonly events: (MembershipEvent<Meta> & { seenBy: string })[] = [];
  private readonly fibers = new Map<string, Fiber<void, never>>();
  private network!: SimulatedNetwork;

  async start(): Promise<void> {
    this.run(SimulatedNetwork.make().flatMap((network) => sync(() => (this.network = network))));
    await this.settle();
  }

  async add(id: string, seeds: string[] = [], options: Partial<GossipOptions<Meta>> = {}) {
    const address = (name: string): Address => ({ host: name, port: 7946 });
    const member = scoped(
      provide(
        Gossip.join<Meta>({
          id,
          address: address(id),
          seeds: seeds.map(address),
          metadata: { zone: "a" },
          ...timing,
          ...options,
        }),
        Transport,
        this.network.node(id),
      ).flatMap((gossip) => {
        this.members.set(id, gossip);
        return gossip.changes
          .forEach((event) =>
            sync(() => {
              this.events.push({ ...event, seenBy: id });
            }),
          )
          .flatMap(() => sleep(1e12));
      }),
    );
    this.fibers.set(id, this.run(member.orDie() as Eff<void, never>));
    await this.settle();
  }

  /** Stop a member; it leaves the cluster politely. */
  async stop(id: string): Promise<void> {
    this.fibers.get(id)!.interrupt();
    this.members.delete(id);
    await this.settle();
  }

  /** Cut a member off from everyone, like a crash. */
  isolate(id: string): void {
    for (const other of this.members.keys()) if (other !== id) this.network.partition(id, other);
  }

  heal(id: string): void {
    for (const other of this.members.keys()) if (other !== id) this.network.heal(id, other);
  }

  /** Let `ms` of test time pass, in small steps. */
  async pass(ms: number): Promise<void> {
    for (let t = 0; t < ms; t += 10) {
      this.clock.advance(10);
      await this.settle();
    }
  }

  /** The ids each member sees, with their state. */
  async view(id: string): Promise<string[]> {
    const members = await this.run(this.members.get(id)!.members).await();
    if (members._tag !== "Success") throw new Error("members failed");
    return members.value.map((m) => (m.state === "alive" ? m.id : `${m.id}?`));
  }

  stopAll(): void {
    for (const fiber of this.fibers.values()) fiber.interrupt();
  }

  private run<A>(program: Eff<A, never>): Fiber<A, never> {
    return runFiber(provide(provide(program, Clock, this.clock), Random, new TestRandom(3)));
  }

  private settle(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 1));
  }
}

let cluster: Cluster;
const newCluster = async () => {
  cluster = new Cluster();
  await cluster.start();
  return cluster;
};
afterEach(() => cluster?.stopAll());

describe("Gossip", () => {
  test("members find each other through one seed", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.pass(1_000);
    for (const id of ["a", "b", "c"]) expect(await c.view(id)).toEqual(["a", "b", "c"]);
  });

  test("a member that stops leaves, and the others hear right away", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.pass(1_000);
    await c.stop("c");
    await c.pass(200);
    expect(await c.view("a")).toEqual(["a", "b"]);
    expect(await c.view("b")).toEqual(["a", "b"]);
    const left = c.events.filter((e) => e.type === "left" && e.member.id === "c");
    expect(left.map((e) => e.member.state)).toContain("left");
  });

  test("a member that stops answering is suspected, then declared dead", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.pass(1_000);
    c.isolate("c");
    await c.pass(400);
    // Someone noticed by now; c is suspect before it is dead.
    const suspected = c.events.some(
      (e) => e.type === "updated" && e.member.id === "c" && e.member.state === "suspect",
    );
    expect(suspected).toBe(true);
    await c.pass(1_500);
    expect(await c.view("a")).toEqual(["a", "b"]);
    expect(await c.view("b")).toEqual(["a", "b"]);
    expect(
      c.events.some((e) => e.type === "left" && e.member.id === "c" && e.member.state === "dead"),
    ).toBe(true);
  });

  test("a suspected member that is in fact alive clears its name", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.pass(1_000);
    // A short outage, shorter than the suspicion timeout.
    c.isolate("c");
    await c.pass(250);
    c.heal("c");
    await c.pass(1_500);
    for (const id of ["a", "b", "c"]) expect(await c.view(id)).toEqual(["a", "b", "c"]);
    expect(c.events.some((e) => e.type === "left")).toBe(false);
  });

  test("a bad link between two members alone doesn't get anyone suspected", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.pass(1_000);
    // a and b can't talk, but both can talk to c, who checks for them.
    cluster["network"].partition("a", "b");
    await c.pass(2_000);
    expect(c.events.filter((e) => e.type !== "joined")).toEqual([]);
  });

  test("metadata changes spread", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.pass(500);
    const update = runFiber(
      provide(c.members.get("b")!.setMetadata({ zone: "b" }), Clock, c.clock),
    );
    await update.await();
    await c.pass(500);
    const seen = c.events.filter((e) => e.type === "updated" && e.seenBy === "a");
    expect(seen.map((e) => e.member.metadata)).toEqual([{ zone: "b" }]);
  });

  test("a member that comes back after dying joins again", async () => {
    const c = await newCluster();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.pass(500);
    await c.stop("b");
    await c.pass(200);
    expect(await c.view("a")).toEqual(["a"]);
    await c.add("b", ["a"]);
    await c.pass(500);
    expect(await c.view("a")).toEqual(["a", "b"]);
  });
});

describe("Gossip over TCP", () => {
  test("members find each other on real sockets", async () => {
    const tcp = { intervalMs: 20, pingTimeoutMs: 200, suspicionMs: 1_000 };
    const views = await scoped(
      eff(function* () {
        const join = (id: string, seeds: Address[]) =>
          provide(
            Gossip.join<Meta>({
              id,
              address: { host: "127.0.0.1", port: 0 },
              seeds,
              metadata: { zone: "a" },
              ...tcp,
            }),
            Transport,
            Tcp,
          );
        const a = yield* join("a", []);
        const seed = (yield* a.self).address;
        const b = yield* join("b", [seed]);
        const c = yield* join("c", [seed]);
        // Wait until everyone knows everyone, or give up after a while.
        const ids = (g: Gossip<Meta>) => g.members.map((ms) => ms.map((m) => m.id).join(","));
        for (let i = 0; i < 100; i++) {
          const all = [yield* ids(a), yield* ids(b), yield* ids(c)];
          if (all.every((v) => v === "a,b,c")) return all;
          yield* sleep(20);
        }
        return [yield* ids(a), yield* ids(b), yield* ids(c)];
      }),
    )
      .orDie()
      .run();
    expect(views).toEqual(["a,b,c", "a,b,c", "a,b,c"]);
  });
});
