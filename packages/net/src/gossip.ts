// Cluster membership by gossip, following SWIM ("Scalable Weakly-consistent
// Infection-style process group Membership", Das, Gupta and Motivala).
//
//   const gossip = yield* Gossip.join({
//     id: "worker-1",
//     address: { host: "10.0.0.1", port: 7946 },
//     seeds: [{ host: "10.0.0.2", port: 7946 }],
//     metadata: { zone: "a" },
//   });
//   yield* gossip.members;                  // who is in the cluster now
//   gossip.changes.forEach(...)             // joined, updated, left
//
// How it works: every `intervalMs` a member pings one other member, picked
// in turn from a shuffled list. If no answer comes within `pingTimeoutMs`,
// it asks up to `indirectChecks` other members to ping it instead, so that a
// bad link between two members alone doesn't get anyone thrown out. If none
// of them gets an answer either, the member is suspected. A suspected member
// that doesn't prove it is alive within `suspicionMs` is declared dead.
//
// Every message carries the sender's whole member list, and the receiver
// merges it into its own, so news spreads through the cluster in a few
// rounds. Each member has an incarnation number that only it increases:
// when it hears that it is suspected, it increases the number and tells
// everyone it is alive, and the higher number wins over the suspicion.
// A full member list per message is simple and fine for clusters of up to a
// few hundred members.
//
// Messages travel over the Transport service, so a cluster runs on TCP in
// production and on a SimulatedNetwork in tests.

import {
  PubSub,
  currentClock,
  currentRandom,
  acquireRelease,
  all,
  awaitFiber,
  forkDaemon,
  scoped,
  succeed,
  suspend,
  sync,
  timeoutOption,
  type Eff,
  type Needs,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import { BinaryCodec } from "./codec.js";
import type { Connection } from "./connection.js";
import type { ListenError } from "./errors.js";
import { Transport, formatAddress, type Address } from "./transport.js";

/** alive → suspect → dead, or left when the member said goodbye. */
export type MemberState = "alive" | "suspect" | "dead" | "left";

/** A member of the cluster, as this member knows it. */
export interface Member<Meta> {
  /** Unique in the cluster. */
  readonly id: string;
  /** Where the member listens for gossip. */
  readonly address: Address;
  /** `alive` or `suspect` while it is in the cluster. */
  readonly state: MemberState;
  /** Raised by the member itself to overrule older news about it. */
  readonly incarnation: number;
  /** What the member tells about itself (see `setMetadata`). */
  readonly metadata: Meta;
}

export type MembershipEvent<Meta> =
  /** A member is new, or came back after being dead. */
  | { readonly type: "joined"; readonly member: Member<Meta> }
  /** A member was suspected, recovered, or changed its metadata. */
  | { readonly type: "updated"; readonly member: Member<Meta> }
  /** A member died or left (see `member.state`). */
  | { readonly type: "left"; readonly member: Member<Meta> };

/** Options for `Gossip.join`. Only `id`, `address` and `metadata` are required. */
export interface GossipOptions<Meta> {
  /** This member's id, unique in the cluster. */
  readonly id: string;
  /** Where to listen for gossip. */
  readonly address: Address;
  /**
   * Where the others reach this member, if not at `address` (for example
   * when listening on 0.0.0.0). Default: the address actually listened on.
   */
  readonly advertise?: Address;
  /**
   * Members to contact to join the cluster, or an effect that finds them
   * (see `Discovery`). Contacted again whenever no other member is known.
   */
  readonly seeds?: readonly Address[] | Eff<readonly Address[], never>;
  /** Anything the others should know about this member. Sent as JSON. */
  readonly metadata: Meta;
  /** How often to check one member. Default: 1000 ms. */
  readonly intervalMs?: number;
  /** How long to wait for a member to answer. Default: 300 ms. */
  readonly pingTimeoutMs?: number;
  /** How many members to ask to check a member that didn't answer. Default: 3. */
  readonly indirectChecks?: number;
  /** How long a suspected member has to show it is alive. Default: 5000 ms. */
  readonly suspicionMs?: number;
}

/** This member's view of the cluster, from `Gossip.join`. */
export interface Gossip<Meta> {
  /** This member as the others see it. */
  readonly self: Eff<Member<Meta>, never>;
  /** The members that are alive or suspected, this one included, by id. */
  readonly members: Eff<Member<Meta>[], never>;
  /** Membership changes from the moment the stream is started. */
  readonly changes: Stream<MembershipEvent<Meta>, never>;
  /** Change this member's metadata; the others learn it by gossip. */
  setMetadata(metadata: Meta): Eff<void, never>;
}

type Message<Meta> =
  | { readonly type: "ping"; readonly members: Member<Meta>[] }
  /** Please ping `target` for me. */
  | { readonly type: "ping-req"; readonly target: Address; readonly members: Member<Meta>[] }
  | { readonly type: "ack"; readonly members: Member<Meta>[] }
  /** I couldn't reach the target of a ping-req either. */
  | { readonly type: "nack"; readonly members: Member<Meta>[] };

const isAddressList = (
  seeds: readonly Address[] | Eff<readonly Address[], never>,
): seeds is readonly Address[] => Array.isArray(seeds);

const isLive = (state: MemberState) => state === "alive" || state === "suspect";

// Within one incarnation, worse news wins: a member suspected at
// incarnation 3 stays suspected until it says it is alive at 4.
const severity: Record<MemberState, number> = { alive: 0, suspect: 1, dead: 2, left: 2 };

/** Whether news about a member replaces what we know. */
function overrides<Meta>(news: Member<Meta>, known: Member<Meta>): boolean {
  if (news.incarnation !== known.incarnation) return news.incarnation > known.incarnation;
  return severity[news.state] > severity[known.state];
}

const sameMetadata = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/** What we know about another member, and since when. */
interface Known<Meta> {
  member: Member<Meta>;
  /** When we started suspecting it (our clock). */
  suspectedAt?: number;
  /** When we learned it is dead or left, to forget it after a while. */
  goneAt?: number;
}

/** Cluster membership by gossip (SWIM). See the top of this file for how it works. */
export const Gossip = {
  /**
   * Join the cluster: listen for gossip, contact the seeds, and keep
   * checking the members. When the surrounding scope ends, this member
   * tells a few others that it leaves, and stops.
   */
  join<Meta>(
    options: GossipOptions<Meta>,
  ): Eff<Gossip<Meta>, Throws<ListenError> | Needs<Transport, "Transport">> {
    const intervalMs = options.intervalMs ?? 1_000;
    const pingTimeoutMs = options.pingTimeoutMs ?? 300;
    const indirectChecks = options.indirectChecks ?? 3;
    const suspicionMs = options.suspicionMs ?? 5_000;
    // Dead members are remembered long enough for the news to reach
    // everyone; a member forgotten too early could come back from stale gossip.
    const forgetAfterMs = Math.max(suspicionMs * 6, intervalMs * 30);
    const codec = BinaryCodec.json<Message<Meta>>();
    const seeds = options.seeds ?? [];
    const findSeeds = isAddressList(seeds) ? succeed(seeds) : seeds;

    return Transport.get.flatMap((transport) =>
      currentClock.flatMap((clock) =>
        currentRandom.flatMap((random) =>
          PubSub.unbounded<MembershipEvent<Meta>>().flatMap((events) => {
            let self: Member<Meta> = {
              id: options.id,
              address: options.advertise ?? options.address,
              state: "alive",
              incarnation: 0,
              metadata: options.metadata,
            };
            const known = new Map<string, Known<Meta>>();
            // Members still to check this round, in random order.
            let toCheck: string[] = [];

            const liveOthers = () =>
              [...known.values()].map((k) => k.member).filter((m) => isLive(m.state));

            const memberList = (): Member<Meta>[] => [
              self,
              ...[...known.values()].map((k) => k.member),
            ];

            /** Take in news about a member; returns the event it causes, if any. */
            const learn = (news: Member<Meta>): MembershipEvent<Meta> | undefined => {
              if (news.id === self.id) {
                // Someone thinks we're suspect or gone: say we're alive, louder.
                if (news.state !== "alive" && news.incarnation >= self.incarnation) {
                  self = { ...self, incarnation: news.incarnation + 1 };
                }
                return undefined;
              }
              const entry = known.get(news.id);
              if (!entry) {
                // Don't learn about members that are already gone.
                if (!isLive(news.state)) return undefined;
                known.set(news.id, {
                  member: news,
                  ...(news.state === "suspect" ? { suspectedAt: clock.now() } : {}),
                });
                return { type: "joined", member: news };
              }
              const before = entry.member;
              if (!overrides(news, before)) return undefined;
              entry.member = news;
              if (news.state === "suspect") {
                if (before.state !== "suspect") entry.suspectedAt = clock.now();
              } else {
                delete entry.suspectedAt;
              }
              if (isLive(news.state)) delete entry.goneAt;
              else if (isLive(before.state)) entry.goneAt = clock.now();

              if (isLive(before.state) && !isLive(news.state)) {
                return { type: "left", member: news };
              }
              if (!isLive(before.state) && isLive(news.state)) {
                return { type: "joined", member: news };
              }
              const changed =
                before.state !== news.state || !sameMetadata(before.metadata, news.metadata);
              return changed ? { type: "updated", member: news } : undefined;
            };

            const announce = (changes: (MembershipEvent<Meta> | undefined)[]) =>
              all(
                changes
                  .filter((c): c is MembershipEvent<Meta> => c !== undefined)
                  .map((c) => events.publish(c)),
              ).map(() => undefined);

            const merge = (members: readonly Member<Meta>[]) =>
              suspend(() => announce(members.map(learn)));

            /** Send one message and wait for the answer, or give up after `ms`. */
            const ask = (
              address: Address,
              message: Message<Meta>,
              ms: number,
            ): Eff<Message<Meta> | undefined, never> =>
              timeoutOption(
                scoped(
                  transport
                    .connect(address, { codec, timeoutMs: ms })
                    .flatMap((c) => c.send(message).flatMap(() => c.messages.take(1).toArray())),
                )
                  .map((replies) => replies[0])
                  .catch(() => succeed(undefined)),
                ms,
              ).flatMap((reply) =>
                reply ? merge(reply.members).map(() => reply) : succeed(reply),
              );

            const ping = (address: Address, ms: number) =>
              ask(address, { type: "ping", members: memberList() }, ms).map(
                (reply) => reply?.type === "ack",
              );

            // Answer pings, and ping on others' behalf.
            const handle = (connection: Connection<Message<Meta>>) =>
              connection.messages.forEach((message) =>
                merge(message.members).flatMap(() => {
                  if (message.type === "ping") {
                    return connection.send({ type: "ack", members: memberList() });
                  }
                  if (message.type === "ping-req") {
                    return ping(message.target, pingTimeoutMs).flatMap((reached) =>
                      connection.send({ type: reached ? "ack" : "nack", members: memberList() }),
                    );
                  }
                  return succeed(undefined);
                }),
              );

            /** Check one member, directly and then through others. */
            const check = (target: Member<Meta>): Eff<void, never> =>
              ping(target.address, pingTimeoutMs).flatMap((answered) => {
                if (answered) return succeed(undefined);
                return random
                  .shuffle(liveOthers().filter((m) => m.id !== target.id))
                  .flatMap((helpers) =>
                    all(
                      helpers
                        .slice(0, indirectChecks)
                        .map((helper) =>
                          ask(
                            helper.address,
                            { type: "ping-req", target: target.address, members: memberList() },
                            pingTimeoutMs * 2,
                          ),
                        ),
                    ),
                  )
                  .flatMap((replies) => {
                    if (replies.some((reply) => reply?.type === "ack")) return succeed(undefined);
                    // Still the same news as when we started? Then suspect it.
                    const current = known.get(target.id)?.member;
                    if (!current || current.state !== "alive") return succeed(undefined);
                    return announce([learn({ ...current, state: "suspect" })]);
                  });
              });

            /** Suspects that ran out of time die; long-dead members are forgotten. */
            const expire = suspend(() => {
              const now = clock.now();
              const changes: (MembershipEvent<Meta> | undefined)[] = [];
              for (const [id, entry] of known) {
                if (entry.suspectedAt !== undefined && now - entry.suspectedAt >= suspicionMs) {
                  changes.push(learn({ ...entry.member, state: "dead" }));
                } else if (entry.goneAt !== undefined && now - entry.goneAt >= forgetAfterMs) {
                  known.delete(id);
                }
              }
              return announce(changes);
            });

            const contactSeeds = findSeeds.flatMap((addresses) =>
              all(
                addresses
                  .filter((a) => formatAddress(a) !== formatAddress(self.address))
                  .map((address) => ping(address, pingTimeoutMs)),
              ),
            );

            const nextTarget: Eff<Member<Meta> | undefined, never> = suspend(() => {
              if (toCheck.length === 0) {
                return random.shuffle(liveOthers().map((m) => m.id)).map((ids) => {
                  toCheck = ids;
                  return takeTarget();
                });
              }
              return succeed(takeTarget());
            });
            const takeTarget = (): Member<Meta> | undefined => {
              while (toCheck.length > 0) {
                const member = known.get(toCheck.shift()!)?.member;
                if (member && isLive(member.state)) return member;
              }
              return undefined;
            };

            const round: Eff<void, never> = expire.flatMap(() =>
              liveOthers().length === 0
                ? contactSeeds.map(() => undefined)
                : nextTarget.flatMap((target) => (target ? check(target) : succeed(undefined))),
            );

            const loop: Eff<never, never> = round
              .flatMap(() => clock.sleep(intervalMs))
              .flatMap(() => loop);

            // Say goodbye to a few members, so they don't have to find out.
            const leave = suspend(() => {
              self = { ...self, state: "left", incarnation: self.incarnation + 1 };
              return random
                .shuffle(liveOthers())
                .flatMap((members) =>
                  all(members.slice(0, 3).map((m) => ping(m.address, pingTimeoutMs))),
                );
            });

            const gossip: Gossip<Meta> = {
              self: sync(() => self),
              members: sync(() =>
                memberList()
                  .filter((m) => isLive(m.state))
                  .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)),
              ),
              changes: Stream.fromEffect(events.subscribe).flatMap((stream) =>
                stream.catch(() => Stream.empty()),
              ),
              setMetadata: (metadata) =>
                sync(() => {
                  self = { ...self, metadata, incarnation: self.incarnation + 1 };
                }),
            };

            return transport
              .listen(options.address, { codec, handle })
              .flatMap((listener) => {
                if (!options.advertise) self = { ...self, address: listener.address };
                return acquireRelease(
                  // Contact the seeds right away, then keep going on our own.
                  contactSeeds.flatMap(() =>
                    forkDaemon(clock.sleep(intervalMs).flatMap(() => loop)),
                  ),
                  (fiber) =>
                    sync(() => fiber.interrupt())
                      .flatMap(() => awaitFiber(fiber))
                      .flatMap(() => leave)
                      .flatMap(() => events.shutdown()),
                );
              })
              .map(() => gossip);
          }),
        ),
      ),
    );
  },
} as const;
