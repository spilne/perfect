// Gossip against hostile or unlucky conditions: bad datagrams, seeds that
// start late, SWIM precedence, table growth, stopping, authentication.

import { afterEach, describe, expect, test } from "bun:test";
import {
  Clock,
  Random,
  TestClock,
  TestRandom,
  provide,
  runFiber,
  scoped,
  sleep,
  succeed,
  sync,
  type Eff,
  type Fiber,
} from "@spilne/perfect-core";
import {
  BinaryCodec,
  Gossip,
  GossipAuth,
  SimulatedNetwork,
  Transport,
  type Address,
  type GossipOptions,
  type MembershipEvent,
} from "../src";

type Meta = { readonly zone: string };
type Wire = Record<string, unknown>;

// For tests that inject members that don't exist: nobody probes them.
const quiet = { intervalMs: 3_600_000 };
const timing = { intervalMs: 100, pingTimeoutMs: 30, suspicionMs: 500 };
const address = (name: string): Address => ({ host: name, port: 7946 });
const encoder = new TextEncoder();
const json = (value: unknown) => encoder.encode(JSON.stringify(value));

const entry = (id: string, extra: Wire = {}): Wire => ({
  id,
  address: { host: id, port: 7946 },
  state: "alive",
  incarnation: 0,
  metadata: { zone: "x" },
  ...extra,
});
const ping = (members: unknown): Wire => ({ type: "ping", members });

/** A cluster on a simulated network and a test clock, with a way to send raw bytes to a member. */
class Harness {
  readonly clock = new TestClock();
  readonly members = new Map<string, Gossip<Meta>>();
  readonly events: (MembershipEvent<Meta> & { seenBy: string })[] = [];
  readonly fibers = new Map<string, Fiber<void, never>>();
  network!: SimulatedNetwork;
  random = 3;

  async start(): Promise<void> {
    this.run(SimulatedNetwork.make().flatMap((network) => sync(() => (this.network = network))));
    await this.settle();
  }

  async add(id: string, seeds: string[] = [], options: Partial<GossipOptions<Meta>> = {}) {
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
          .forEach((event) => sync(() => void this.events.push({ ...event, seenBy: id })))
          .flatMap(() => sleep(1e12));
      }),
    );
    this.fibers.set(id, this.run(member.orDie() as Eff<void, never>));
    await this.settle();
  }

  async stop(id: string): Promise<void> {
    this.fibers.get(id)!.interrupt();
    this.members.delete(id);
    await this.settle();
  }

  async pass(ms: number): Promise<void> {
    for (let t = 0; t < ms; t += 10) {
      this.clock.advance(10);
      await this.settle();
    }
  }

  async view(id: string): Promise<string[]> {
    const members = await this.run(this.members.get(id)!.members).await();
    if (members._tag !== "Success") throw new Error("members failed");
    return members.value.map((m) => (m.state === "alive" ? m.id : `${m.id}?`));
  }

  async stats(id: string) {
    const result = await this.run(this.members.get(id)!.stats).await();
    if (result._tag !== "Success") throw new Error("stats failed");
    return result.value;
  }

  async self(id: string) {
    const result = await this.run(this.members.get(id)!.self).await();
    if (result._tag !== "Success") throw new Error("self failed");
    return result.value;
  }

  /** Send raw bytes to a member as a stranger would; resolves to its decoded reply, if any. */
  async inject(to: string, payload: Uint8Array): Promise<Wire | undefined> {
    const exchange = scoped(
      this.network
        .node("stranger")
        .connect(address(to), { codec: BinaryCodec.bytes })
        .flatMap((c) => c.send(payload).flatMap(() => c.messages.take(1).toArray())),
    ).catch(() => succeed([] as Uint8Array[]));
    const fiber = this.run(exchange as Eff<Uint8Array[], never>);
    await this.pass(60);
    const result = await fiber.await();
    const [reply] = result._tag === "Success" ? result.value : [];
    if (!reply) return undefined;
    try {
      return JSON.parse(new TextDecoder().decode(reply)) as Wire;
    } catch {
      return undefined;
    }
  }

  async digestOf(id: string): Promise<Wire[]> {
    const reply = await this.inject(id, json(ping([])));
    return ((reply?.members as Wire[] | undefined) ?? []) as Wire[];
  }

  stopAll(): void {
    for (const fiber of this.fibers.values()) fiber.interrupt();
  }

  private run<A>(program: Eff<A, never>): Fiber<A, never> {
    return runFiber(
      provide(provide(program, Clock, this.clock), Random, new TestRandom(this.random)),
    );
  }

  private settle(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 1));
  }
}

let h: Harness;
const newHarness = async () => {
  h = new Harness();
  await h.start();
  return h;
};
afterEach(() => h?.stopAll());

describe("hostile messages", () => {
  const bad: [string, () => Uint8Array][] = [
    ["not JSON", () => encoder.encode("{nope")],
    ["bytes that are not UTF-8", () => new Uint8Array([0xff, 0xfe, 0xfd])],
    ["a number instead of an object", () => json(7)],
    ["null", () => json(null)],
    ["an unknown message type", () => json({ type: "boom", members: [] })],
    ["members that are not a list", () => json(ping("everyone"))],
    ["a member that is null", () => json(ping([entry("v"), null]))],
    ["a member without an id", () => json(ping([entry("v"), entry("w", { id: undefined })]))],
    ["an empty id", () => json(ping([entry("v"), entry("w", { id: "" })]))],
    ["an id that is a number", () => json(ping([entry("v"), entry("w", { id: 5 })]))],
    ["a huge id", () => json(ping([entry("v"), entry("w", { id: "x".repeat(10_000) })]))],
    ["an unknown state", () => json(ping([entry("v"), entry("w", { state: "zombie" })]))],
    ["a negative incarnation", () => json(ping([entry("v"), entry("w", { incarnation: -1 })]))],
    ["a fractional incarnation", () => json(ping([entry("v"), entry("w", { incarnation: 1.5 })]))],
    ["an incarnation as text", () => json(ping([entry("v"), entry("w", { incarnation: "9" })]))],
    [
      "an incarnation too big to increase",
      () => json(ping([entry("v"), entry("w", { incarnation: 1e300 })])),
    ],
    ["an address that is text", () => json(ping([entry("v"), entry("w", { address: "w:1" })]))],
    [
      "a port out of range",
      () => json(ping([entry("v"), entry("w", { address: { host: "w", port: 70_000 } })])),
    ],
    [
      "a fractional port",
      () => json(ping([entry("v"), entry("w", { address: { host: "w", port: 1.5 } })])),
    ],
    ["a ping-req without a target", () => json({ type: "ping-req", members: [entry("v")] })],
    [
      "a ping-req to a nonsense target",
      () => json({ type: "ping-req", target: { host: 3 }, members: [entry("v")] }),
    ],
  ];

  for (const [name, bytes] of bad) {
    test(`${name} is rejected whole and changes nothing`, async () => {
      const c = await newHarness();
      await c.add("a", [], quiet);
      const before = await c.stats("a");
      const reply = await c.inject("a", bytes());
      expect(reply).toBeUndefined();
      expect(await c.view("a")).toEqual(["a"]);
      expect(c.events).toEqual([]);
      const after = await c.stats("a");
      expect(after.rejectedMessages).toBe(before.rejectedMessages + 1);
      expect(after.tableSize).toBe(0);
      // Still healthy: a good message gets an answer and is learned.
      const good = await c.inject("a", json(ping([entry("v")])));
      expect(good?.type).toBe("ack");
      expect(await c.view("a")).toEqual(["a", "v"]);
    });
  }

  test("a well-formed message is accepted (control for the rejections above)", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    const reply = await c.inject("a", json(ping([entry("v"), entry("w")])));
    expect(reply?.type).toBe("ack");
    expect(await c.view("a")).toEqual(["a", "v", "w"]);
    expect((await c.stats("a")).rejectedMessages).toBe(0);
  });

  test("rejections are counted by reason", async () => {
    const c = await newHarness();
    await c.add("a", [], {
      ...quiet,
      maxMessageBytes: 2_048,
      maxDigestMembers: 2,
      maxMembersPerMessage: 3,
    });
    await c.inject("a", encoder.encode("{nope"));
    await c.inject("a", json(ping([entry("v"), { id: 1 }])));
    await c.inject("a", json(ping([entry("1"), entry("2"), entry("3"), entry("4")])));
    await c.inject("a", new Uint8Array(5_000).fill(32));
    const stats = await c.stats("a");
    expect(stats.rejected).toEqual({
      undecodable: 1,
      malformed: 1,
      "too-many-members": 1,
      "too-large": 1,
      unauthenticated: 0,
    });
    expect(stats.rejectedMessages).toBe(4);
    expect(await c.view("a")).toEqual(["a"]);
  });

  test("a valid entry in front of a bad one is not merged", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    await c.inject("a", json(ping([entry("good"), entry("also-good"), { id: "bad", state: 3 }])));
    expect(await c.view("a")).toEqual(["a"]);
    expect(c.events).toEqual([]);
  });

  test("a message over maxMessageBytes is refused and an oversized flood leaves the table alone", async () => {
    const c = await newHarness();
    await c.add("a", [], { ...quiet, maxMessageBytes: 4_096 });
    const members = Array.from({ length: 100 }, (_, n) => entry(`m${n}`));
    expect(JSON.stringify(ping(members)).length).toBeGreaterThan(4_096);
    await c.inject("a", json(ping(members)));
    expect(await c.view("a")).toEqual(["a"]);
    expect((await c.stats("a")).rejected["too-large"]).toBe(1);
  });

  test("a table that is full ignores news about new members and counts it", async () => {
    const c = await newHarness();
    await c.add("a", [], { ...quiet, maxMembers: 5 });
    const members = Array.from({ length: 12 }, (_, n) => entry(`m${n}`));
    await c.inject("a", json(ping(members)));
    const stats = await c.stats("a");
    expect(stats.tableSize).toBe(5);
    expect(stats.droppedMembers).toBe(7);
  });

  test("options that would make the member reject its own messages are refused", () => {
    expect(() =>
      Gossip.join<Meta>({
        id: "a",
        address: address("a"),
        metadata: { zone: "a" },
        maxDigestMembers: 10,
        maxMembersPerMessage: 5,
      }),
    ).toThrow(RangeError);
  });
});

describe("seeds", () => {
  test("a seed that starts late is joined even when other members are already known", async () => {
    const c = await newHarness();
    await c.add("b");
    // `a` knows `b` through the cluster it formed, and lists `late` as a seed too.
    await c.add("a", ["b", "late"]);
    await c.pass(1_000);
    expect(await c.view("a")).toEqual(["a", "b"]);
    await c.add("late");
    await c.pass(2_000);
    for (const id of ["a", "b", "late"]) expect(await c.view(id)).toEqual(["a", "b", "late"]);
  });

  test("a seed that never starts is never reported as a member, suspected or left", async () => {
    const c = await newHarness();
    await c.add("a", ["ghost"]);
    await c.add("b", ["a", "ghost"]);
    await c.pass(5_000);
    expect(c.events.filter((e) => e.member.id === "ghost")).toEqual([]);
    expect(await c.view("a")).toEqual(["a", "b"]);
    await c.stop("b");
    await c.pass(1_000);
    expect(c.events.filter((e) => e.member.id === "ghost")).toEqual([]);
    expect((await c.stats("a")).tableSize).toBe(1);
  });

  test("two clusters that declared each other dead find each other again through their seeds", async () => {
    const c = await newHarness();
    await c.add("a", ["c"]);
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.add("d", ["c"]);
    await c.pass(1_000);
    for (const id of ["a", "b", "c", "d"]) expect(await c.view(id)).toEqual(["a", "b", "c", "d"]);
    for (const left of ["a", "b"]) for (const right of ["c", "d"]) c.network.partition(left, right);
    await c.pass(10_000);
    expect(await c.view("a")).toEqual(["a", "b"]);
    expect(await c.view("c")).toEqual(["c", "d"]);
    for (const left of ["a", "b"]) for (const right of ["c", "d"]) c.network.heal(left, right);
    await c.pass(5_000);
    for (const id of ["a", "b", "c", "d"]) expect(await c.view(id)).toEqual(["a", "b", "c", "d"]);
  });

  test("a seed that restarts is rejoined", async () => {
    const c = await newHarness();
    await c.add("seed");
    await c.add("b", ["seed"]);
    await c.pass(1_000);
    c.network.partition("seed", "b");
    await c.pass(8_000);
    expect(await c.view("b")).toEqual(["b"]);
    c.network.heal("seed", "b");
    await c.pass(2_000);
    expect(await c.view("b")).toEqual(["b", "seed"]);
  });
});

describe("suspicion at equal incarnation", () => {
  const stateOf = async (c: Harness, id: string, who: string) => {
    const reply = await c.digestOf(who);
    return reply.find((m) => m.id === id);
  };

  test("worse news wins at the same incarnation, higher incarnation overrules", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    await c.inject("a", json(ping([entry("v")])));
    expect((await stateOf(c, "v", "a"))?.state).toBe("alive");

    await c.inject("a", json(ping([entry("v", { state: "suspect" })])));
    expect((await stateOf(c, "v", "a"))?.state).toBe("suspect");

    // Alive at the same incarnation does not clear a suspicion...
    await c.inject("a", json(ping([entry("v", { state: "alive" })])));
    expect((await stateOf(c, "v", "a"))?.state).toBe("suspect");

    // ...but alive at a higher one does.
    await c.inject("a", json(ping([entry("v", { state: "alive", incarnation: 1 })])));
    expect(await stateOf(c, "v", "a")).toMatchObject({ state: "alive", incarnation: 1 });

    await c.inject("a", json(ping([entry("v", { state: "suspect", incarnation: 1 })])));
    await c.inject("a", json(ping([entry("v", { state: "dead", incarnation: 1 })])));
    expect(await stateOf(c, "v", "a")).toMatchObject({ state: "dead", incarnation: 1 });

    // Dead stays dead against suspect and alive at that incarnation.
    await c.inject("a", json(ping([entry("v", { state: "suspect", incarnation: 1 })])));
    await c.inject("a", json(ping([entry("v", { state: "alive", incarnation: 1 })])));
    expect((await stateOf(c, "v", "a"))?.state).toBe("dead");

    // A higher incarnation brings it back, with a joined event.
    await c.inject("a", json(ping([entry("v", { state: "alive", incarnation: 2 })])));
    expect(await stateOf(c, "v", "a")).toMatchObject({ state: "alive", incarnation: 2 });
    expect(c.events.map((e) => `${e.type}:${e.member.state}`)).toEqual([
      "joined:alive",
      "updated:suspect",
      "updated:alive",
      "updated:suspect",
      "left:dead",
      "joined:alive",
    ]);
  });

  test("a member refutes suspicion and death by outbidding the incarnation, also at equal incarnation", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    const own = (state: string, incarnation: number) =>
      json(ping([{ ...entry("a", { state, incarnation }) }]));
    await c.inject("a", own("suspect", 0));
    expect((await c.self("a")).incarnation).toBe(1);
    // Suspected again at the new incarnation: outbid again.
    await c.inject("a", own("suspect", 1));
    expect((await c.self("a")).incarnation).toBe(2);
    // Old news about itself changes nothing.
    await c.inject("a", own("dead", 0));
    expect((await c.self("a")).incarnation).toBe(2);
    await c.inject("a", own("dead", 2));
    expect((await c.self("a")).incarnation).toBe(3);
  });

  test("news that a member is alive at a higher incarnation than its own is outbid too", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    // A previous run of this member got to incarnation 7 and the others still remember it.
    await c.inject("a", json(ping([entry("a", { incarnation: 7, metadata: { zone: "old" } })])));
    expect((await c.self("a")).incarnation).toBe(8);
    const reply = await c.digestOf("a");
    expect(reply.find((m) => m.id === "a")).toMatchObject({
      incarnation: 8,
      metadata: { zone: "a" },
    });
  });

  test("alive news about itself at its own incarnation changes nothing", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    await c.inject("a", json(ping([entry("a", { incarnation: 0 })])));
    expect((await c.self("a")).incarnation).toBe(0);
  });
});

describe("indirect probes", () => {
  test("healthy members behind a lossy link are not declared dead, and a real crash still is", async () => {
    const c = await newHarness();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.add("d", ["a"]);
    await c.pass(1_000);
    // Half of what goes between a and b is lost; the rest of the network is fine.
    c.network.link("a", "b", { lossRate: 0.5 });
    await c.pass(6_000);
    expect(c.events.filter((e) => e.type === "left")).toEqual([]);
    for (const id of ["a", "b", "c", "d"]) expect(await c.view(id)).toEqual(["a", "b", "c", "d"]);

    // The earlier successful indirect probes must not vouch for a member that then crashes.
    for (const other of ["a", "b", "d"]) c.network.partition("c", other);
    await c.pass(4_000);
    expect(await c.view("a")).toEqual(["a", "b", "d"]);
    expect(
      c.events.some((e) => e.type === "left" && e.member.id === "c" && e.member.state === "dead"),
    ).toBe(true);
  });
});

describe("table size", () => {
  test("dead and departed members are forgotten after the retention time", async () => {
    const c = await newHarness();
    await c.add("a", [], { deadRetentionMs: 2_000 });
    await c.add("b", ["a"]);
    await c.add("c", ["a"]);
    await c.pass(1_000);
    expect((await c.stats("a")).tableSize).toBe(2);
    await c.stop("c");
    await c.pass(200);
    expect((await c.stats("a")).tableSize).toBe(2);
    expect((await c.digestOf("a")).map((m) => m.id)).toContain("c");
    await c.pass(2_500);
    expect((await c.stats("a")).tableSize).toBe(1);
    expect((await c.digestOf("a")).map((m) => m.id).sort()).toEqual(["a", "b"]);
  });

  test("the digest is capped and rotates so every member is still passed on", async () => {
    const c = await newHarness();
    await c.add("a", [], { ...quiet, maxDigestMembers: 8, maxMembersPerMessage: 64 });
    const members = Array.from({ length: 40 }, (_, n) => entry(`m${n}`));
    await c.inject("a", json(ping(members.slice(0, 20))));
    await c.inject("a", json(ping(members.slice(20))));
    expect((await c.stats("a")).tableSize).toBe(40);
    const seen = new Set<string>();
    for (let round = 0; round < 12; round++) {
      const digest = await c.digestOf("a");
      expect(digest.length).toBeLessThanOrEqual(8);
      for (const m of digest) seen.add(m.id as string);
    }
    expect(seen.size).toBe(41);
  });

  test("a recent change is passed on before the rotation reaches it", async () => {
    const c = await newHarness();
    await c.add("a", [], { ...quiet, maxDigestMembers: 4, maxMembersPerMessage: 64 });
    await c.inject("a", json(ping(Array.from({ length: 30 }, (_, n) => entry(`m${n}`)))));
    await c.pass(5_000);
    await c.inject("a", json(ping([entry("m17", { state: "suspect" })])));
    const digest = await c.digestOf("a");
    expect(digest.find((m) => m.id === "m17")?.state).toBe("suspect");
  });

  test("a cluster larger than the digest still converges", async () => {
    const c = await newHarness();
    await c.add("n0", [], { maxDigestMembers: 3 });
    for (let n = 1; n < 8; n++) await c.add(`n${n}`, ["n0"], { maxDigestMembers: 3 });
    await c.pass(10_000);
    const expected = Array.from({ length: 8 }, (_, n) => `n${n}`);
    for (const id of expected) expect(await c.view(id)).toEqual(expected);
  });
});

describe("stopping", () => {
  test("a stopped member clears its timers, closes its listener and emits nothing more", async () => {
    const c = await newHarness();
    await c.add("a");
    await c.add("b", ["a"]);
    await c.pass(1_000);
    const mark = c.events.length;
    await c.stop("b");
    await c.pass(1_000);
    const afterStop = c.events.slice(mark).filter((e) => e.seenBy === "b");
    expect(afterStop).toEqual([]);
    const refused = await c.inject("b", json(ping([])));
    expect(refused).toBeUndefined();
    // Only a's own timers remain: its loop's sleep (one) and whatever probe is in flight.
    expect(c.clock.pendingCount).toBeLessThanOrEqual(2);
    await c.stop("a");
    await c.pass(2_000);
    expect(c.clock.pendingCount).toBe(0);
  });

  test("while stopping, what the goodbye pings learn is not merged into the table", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    await c.add("b", ["a"], quiet);
    await c.pass(500);
    // An observer outside a's scope, which still listens while a shuts down.
    const outside: string[] = [];
    runFiber(
      provide(
        c.members
          .get("a")!
          .changes.forEach((event) => sync(() => void outside.push(event.member.id))),
        Clock,
        c.clock,
      ),
    );
    await c.inject("a", json(ping([entry("control")])));
    expect(outside).toEqual(["control"]);
    outside.length = 0;
    // Neither member probes, so only a's goodbye ping can bring it the news b has about z.
    await c.inject("b", json(ping([entry("z")])));
    const a = c.members.get("a")!;
    c.fibers.get("a")!.interrupt();
    await c.pass(500);
    expect(outside).toEqual([]);
    const view = await runFiber(provide(a.members, Clock, c.clock)).await();
    expect(view._tag === "Success" && view.value.map((m) => m.id)).toEqual(["b", "control"]);
  });

  test("seed discovery never overlaps itself and stops with the member", async () => {
    const c = await newHarness();
    let running = 0;
    let peak = 0;
    let runs = 0;
    const slowSeeds = sync(() => {
      running++;
      runs++;
      peak = Math.max(peak, running);
    })
      .flatMap(() => sleep(250))
      .flatMap(() => sync(() => void running--))
      .map((): readonly Address[] => [address("nobody")]);
    await c.add("a", [], { seeds: slowSeeds });
    await c.pass(3_000);
    expect(peak).toBe(1);
    expect(runs).toBeGreaterThan(2);
    await c.stop("a");
    const runsAtStop = runs;
    await c.pass(3_000);
    expect(runs).toBe(runsAtStop);
    expect(c.clock.pendingCount).toBe(0);
  });
});

describe("authenticated gossip", () => {
  const key = "0123456789abcdef-cluster-key";
  const auth = GossipAuth.hmacSha256(key);
  const signed = (message: unknown, authenticator = auth) => {
    const payload = json(message);
    const tag = authenticator.sign(payload);
    const bytes = new Uint8Array(tag.length + payload.length);
    bytes.set(tag);
    bytes.set(payload, tag.length);
    return bytes;
  };

  test("without an authenticator anyone can forge membership news (the threat model)", async () => {
    const c = await newHarness();
    await c.add("a", [], quiet);
    await c.inject("a", json(ping([entry("forged")])));
    expect(await c.view("a")).toEqual(["a", "forged"]);
  });

  test("members with the same key form a cluster", async () => {
    const c = await newHarness();
    await c.add("a", [], { authenticator: auth });
    await c.add("b", ["a"], { authenticator: GossipAuth.hmacSha256(key) });
    await c.add("c", ["a"], { authenticator: auth });
    await c.pass(1_500);
    for (const id of ["a", "b", "c"]) expect(await c.view(id)).toEqual(["a", "b", "c"]);
    expect((await c.stats("a")).rejectedMessages).toBe(0);
  });

  test("unsigned, wrongly signed and tampered messages are rejected before parsing", async () => {
    const c = await newHarness();
    await c.add("a", [], { ...quiet, authenticator: auth });
    const message = ping([entry("forged")]);
    const wrongKey = GossipAuth.hmacSha256("another-key-another-key");
    const tampered = signed(message);
    tampered[tampered.length - 3] = tampered[tampered.length - 3]! ^ 1;
    const shortOfATag = json(message).subarray(0, 10);
    for (const bytes of [json(message), signed(message, wrongKey), tampered, shortOfATag]) {
      expect(await c.inject("a", bytes)).toBeUndefined();
    }
    expect(await c.view("a")).toEqual(["a"]);
    expect((await c.stats("a")).rejected.unauthenticated).toBe(4);

    // A correctly signed one is accepted and its reply is signed too.
    const reply = await c.inject("a", signed(message));
    expect(reply).toBeUndefined();
    expect(await c.view("a")).toEqual(["a", "forged"]);
  });

  test("a member with another key stays out of the cluster", async () => {
    const c = await newHarness();
    await c.add("a", [], { authenticator: auth });
    await c.add("b", ["a"], { authenticator: auth });
    await c.add("intruder", ["a"], {
      authenticator: GossipAuth.hmacSha256("a-different-secret-key"),
    });
    await c.pass(2_000);
    expect(await c.view("a")).toEqual(["a", "b"]);
    expect(await c.view("intruder")).toEqual(["intruder"]);
  });

  test("keys that are too short are refused", () => {
    expect(() => GossipAuth.hmacSha256("short")).toThrow(RangeError);
  });
});
