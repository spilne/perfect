// A cluster: members that find each other by gossip, and a hash ring that
// splits the work between them.
//
// Run: bun packages/net/examples/03-cluster.ts

import { eff, provide, scoped, sleep } from "@spilne/perfect-core";
import { Discovery, Gossip, HashRing, Tcp, Transport, type Address } from "../src";
import { assertEq } from "./_assert";

// >>> example: hash-ring
// Which worker handles which customer? The ring says, and adding a worker
// only moves the customers that the new worker takes over.
const ring = HashRing.of(["worker-1", "worker-2", "worker-3"]);
const customers = Array.from({ length: 1_000 }, (_, i) => `customer-${i}`);

const owner = ring.owner("customer-42"); // one of the workers
const [primary, backup] = ring.owners("customer-42", 2); // for a replica
const moves = ring.moved(ring.with("worker-4"), customers);
assertEq(primary, owner);
assertEq(backup !== primary, true);
assertEq(
  moves.every((move) => move.to === "worker-4"),
  true,
);
// <<< example

// >>> example: gossip
// Three members on this machine. Each knows only the first one, and learns
// about the others by gossip. The ring follows the membership.
type Meta = { readonly role: string };

const join = (id: string, seeds: readonly Address[]) =>
  provide(
    Gossip.join<Meta>({
      id,
      address: { host: "127.0.0.1", port: 0 },
      seeds: Discovery.of(seeds),
      metadata: { role: "worker" },
      intervalMs: 50,
    }),
    Transport,
    Tcp,
  );

const members = await scoped(
  eff(function* () {
    const first = yield* join("worker-1", []);
    const seed = (yield* first.self).address;
    yield* join("worker-2", [seed]);
    yield* join("worker-3", [seed]);
    yield* sleep(500);

    const members = yield* first.members;
    const ring = HashRing.of(members.map((member) => member.id));
    return { ids: ring.nodes, owner: ring.owner("customer-42") };
  }),
)
  .orDie()
  .run();
assertEq(members.ids, ["worker-1", "worker-2", "worker-3"]);
assertEq(members.owner, owner);
// <<< example
