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
// Every message carries a list of members, and the receiver merges it into
// its own, so news spreads through the cluster in a few rounds. Each member
// has an incarnation number that only it increases: when it hears that it is
// suspected, it increases the number and tells everyone it is alive, and the
// higher number wins over the suspicion. At the same incarnation, worse news
// wins: dead and left over suspect over alive.
//
// The list is a bounded sample of the table, not all of it: up to
// `maxDigestMembers` entries per message (default 128), made of the members
// that changed most recently and a window that rotates over the rest, so
// every entry is passed on within a few rounds and message size doesn't
// grow with the cluster. The table of known members is bounded too
// (`maxMembers`), and members that died or left are forgotten after
// `deadRetentionMs`. A cluster of a few thousand members works; beyond
// that, news takes longer to spread, and a sample that rotates over tens of
// thousands of entries takes minutes to cover them all.
//
// Nothing in a message is trusted. A message that is too big, too long,
// not valid JSON or not shaped as expected is dropped whole before anything in
// it is used, and counted (see `Gossip.stats`). There is no authentication
// unless you pass an `authenticator` (see `GossipAuth`): without one, anyone
// who can reach a member's port can add members and declare others dead.
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
import type { BinaryCodec } from "./codec.js";
import type { Connection } from "./connection.js";
import type { ConnectError, ListenError, NetError } from "./errors.js";
import { FrameError, FrameTooBig } from "./frames.js";
import type { GossipAuthenticator } from "./gossip-auth.js";
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
  /**
   * How long a dead or departed member is remembered, so that its death
   * spreads before it is forgotten. Default: six times `suspicionMs`, at
   * least 30 intervals.
   */
  readonly deadRetentionMs?: number;
  /**
   * How often to try the seeds that aren't members while other members are
   * known, so a seed that starts late, or a cluster that split, is found.
   * Default: five intervals, at least `suspicionMs`.
   */
  readonly seedRefreshMs?: number;
  /** Members listed in one message, at most. Default: 128. */
  readonly maxDigestMembers?: number;
  /**
   * Members a received message may list. Longer messages are rejected whole.
   * Default: 512; at least `maxDigestMembers + 1`.
   */
  readonly maxMembersPerMessage?: number;
  /** The largest message accepted, in bytes. Default: 1 MiB. */
  readonly maxMessageBytes?: number;
  /**
   * Members remembered, dead ones included. News about new members is
   * ignored when the table is full (and dead entries can't be dropped to
   * make room). Default: 10 000.
   */
  readonly maxMembers?: number;
  /**
   * Gossip connections open at once, at most; more are closed on arrival.
   * Default: 1024.
   */
  readonly maxInboundConnections?: number;
  /**
   * Signs the messages this member sends and rejects the ones that aren't
   * signed by the same cluster key. See `GossipAuth`.
   */
  readonly authenticator?: GossipAuthenticator;
}

/** Why a received message was dropped. */
export type GossipRejection =
  /** Not valid UTF-8 or JSON, or cut off. */
  | "undecodable"
  | "too-large"
  /** Valid JSON of the wrong shape, or with values out of range. */
  | "malformed"
  | "too-many-members"
  /** Its tag is missing or wrong (only with an `authenticator`). */
  | "unauthenticated";

/** Counters of a running member, from `Gossip.stats`. */
export interface GossipStats {
  readonly rejectedMessages: number;
  readonly rejected: Readonly<Record<GossipRejection, number>>;
  /** Members that were ignored because the table was full. */
  readonly droppedMembers: number;
  /** Members remembered right now, dead ones included, this one not. */
  readonly tableSize: number;
}

/** This member's view of the cluster, from `Gossip.join`. */
export interface Gossip<Meta> {
  /** This member as the others see it. */
  readonly self: Eff<Member<Meta>, never>;
  /** The members that are alive or suspected, this one included, by id. */
  readonly members: Eff<Member<Meta>[], never>;
  /** Membership changes from the moment the stream is started. */
  readonly changes: Stream<MembershipEvent<Meta>, never>;
  /** Counters of rejected messages and the size of the member table. */
  readonly stats: Eff<GossipStats, never>;
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

const encoder = new TextEncoder();
const utf8 = new TextDecoder("utf-8", { fatal: true });

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

const MAX_INCARNATION = 2 ** 40;
const MAX_ID_LENGTH = 256;
const MAX_HOST_LENGTH = 255;
const MAX_PATH_LENGTH = 1_024;

/** A received message that must be dropped; counted where it is thrown. */
class Rejected extends Error {
  constructor(
    readonly reason: GossipRejection,
    detail: string,
  ) {
    super(detail);
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const malformed = (detail: string) => new Rejected("malformed", detail);

function parseAddress(value: unknown): Address {
  if (!isRecord(value)) throw malformed("an address that is not an object");
  if (typeof value.path === "string") {
    if (value.path.length === 0 || value.path.length > MAX_PATH_LENGTH) {
      throw malformed("a socket path of the wrong length");
    }
    return { path: value.path };
  }
  const { host, port } = value;
  if (typeof host !== "string" || host.length === 0 || host.length > MAX_HOST_LENGTH) {
    throw malformed("an address without a usable host");
  }
  if (typeof port !== "number" || !Number.isInteger(port) || port < 1 || port > 65_535) {
    throw malformed("an address with a port out of range");
  }
  return { host, port };
}

const STATES: ReadonlySet<unknown> = new Set(["alive", "suspect", "dead", "left"]);

/** Copies only the known fields, so nothing else a peer sent gets into the table. */
function parseMember<Meta>(value: unknown): Member<Meta> {
  if (!isRecord(value)) throw malformed("a member that is not an object");
  const { id, state, incarnation } = value;
  if (typeof id !== "string" || id.length === 0 || id.length > MAX_ID_LENGTH) {
    throw malformed("a member id of the wrong type or length");
  }
  if (!STATES.has(state)) throw malformed("an unknown member state");
  if (
    typeof incarnation !== "number" ||
    !Number.isInteger(incarnation) ||
    incarnation < 0 ||
    incarnation > MAX_INCARNATION
  ) {
    throw malformed("an incarnation out of range");
  }
  return {
    id,
    address: parseAddress(value.address),
    state: state as MemberState,
    incarnation,
    metadata: value.metadata as Meta,
  };
}

function parseMessage<Meta>(params: {
  readonly value: unknown;
  readonly maxMembers: number;
}): Message<Meta> {
  const { value, maxMembers } = params;
  if (!isRecord(value)) throw malformed("a message that is not an object");
  if (!Array.isArray(value.members)) throw malformed("a message without a member list");
  if (value.members.length > maxMembers) {
    throw new Rejected(
      "too-many-members",
      `${value.members.length} members in one message (the limit is ${maxMembers})`,
    );
  }
  const members = value.members.map((member) => parseMember<Meta>(member));
  switch (value.type) {
    case "ping":
    case "ack":
    case "nack":
      return { type: value.type, members };
    case "ping-req":
      return { type: "ping-req", target: parseAddress(value.target), members };
    default:
      throw malformed("an unknown message type");
  }
}

/** What we know about another member, and since when. */
interface Known<Meta> {
  member: Member<Meta>;
  /** When we started suspecting it (our clock). */
  suspectedAt?: number;
  /** When we learned it is dead or left, to forget it after a while. */
  goneAt?: number;
  /** When its entry last changed, to pass recent news on first. */
  changedAt: number;
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
    const forgetAfterMs = options.deadRetentionMs ?? Math.max(suspicionMs * 6, intervalMs * 30);
    const seedRefreshMs = options.seedRefreshMs ?? Math.max(intervalMs * 5, suspicionMs);
    const maxDigestMembers = options.maxDigestMembers ?? 128;
    const maxMembersPerMessage = options.maxMembersPerMessage ?? 512;
    const maxMessageBytes = options.maxMessageBytes ?? 1024 * 1024;
    const maxMembers = options.maxMembers ?? 10_000;
    const authenticator = options.authenticator;
    for (const [name, value] of Object.entries({
      maxDigestMembers,
      maxMembersPerMessage,
      maxMessageBytes,
      maxMembers,
    })) {
      if (!Number.isInteger(value) || value < 1) {
        throw new RangeError(`${name} must be a positive integer, got ${value}`);
      }
    }
    if (maxMembersPerMessage < maxDigestMembers + 1) {
      throw new RangeError(
        "maxMembersPerMessage must allow the member itself plus maxDigestMembers others",
      );
    }
    const rejected: Record<GossipRejection, number> = {
      undecodable: 0,
      "too-large": 0,
      malformed: 0,
      "too-many-members": 0,
      unauthenticated: 0,
    };
    let droppedMembers = 0;
    const reject = (reason: GossipRejection, detail: string): never => {
      rejected[reason] += 1;
      throw new Rejected(reason, detail);
    };
    // Counts and throws; the connection turns the throw into a MessageError.
    const codec: BinaryCodec<Message<Meta>> = {
      encode: (message) => {
        const payload = encoder.encode(JSON.stringify(message));
        if (!authenticator) return payload;
        const tag = authenticator.sign(payload);
        const signed = new Uint8Array(tag.length + payload.length);
        signed.set(tag);
        signed.set(payload, tag.length);
        return signed;
      },
      decode: (bytes) => {
        let payload = bytes;
        if (authenticator) {
          const tagBytes = authenticator.tagBytes;
          if (bytes.length < tagBytes)
            return reject("unauthenticated", "message too short for its tag");
          payload = bytes.subarray(tagBytes);
          if (!authenticator.verify({ payload, tag: bytes.subarray(0, tagBytes) })) {
            return reject("unauthenticated", "wrong tag");
          }
        }
        let value: unknown;
        try {
          value = JSON.parse(utf8.decode(payload));
        } catch (error) {
          return reject("undecodable", String(error));
        }
        try {
          return parseMessage<Meta>({ value, maxMembers: maxMembersPerMessage });
        } catch (error) {
          if (error instanceof Rejected) return reject(error.reason, error.message);
          throw error;
        }
      },
    };
    /** A connection that failed in the framing (too big, cut off) is a rejection too. */
    const noteStreamError = (error: NetError | ConnectError) => {
      if (error._tag !== "MessageError" || !(error.cause instanceof FrameError)) return;
      rejected[error.cause instanceof FrameTooBig ? "too-large" : "undecodable"] += 1;
    };
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
            // Set when the member starts to stop: nothing is merged or published after.
            let stopped = false;
            // Where the rotating part of the digest starts.
            let cursor = 0;
            let lastSeedContact = -Infinity;

            const liveOthers = () =>
              [...known.values()].map((k) => k.member).filter((m) => isLive(m.state));

            /**
             * What to tell: this member, the entries that changed recently,
             * and a window that moves over the others, up to the cap.
             */
            const memberList = (): Member<Meta>[] => {
              const entries = [...known.values()];
              if (entries.length + 1 <= maxDigestMembers) {
                return [self, ...entries.map((k) => k.member)];
              }
              const budget = maxDigestMembers - 1;
              const now = clock.now();
              const recent = entries
                .filter((k) => now - k.changedAt < suspicionMs)
                .sort((a, b) => b.changedAt - a.changedAt)
                .slice(0, Math.floor(budget / 2));
              const chosen = new Set(recent);
              const rest = entries.filter((k) => !chosen.has(k));
              const take = Math.min(budget - recent.length, rest.length);
              const start = cursor % rest.length;
              const window = Array.from(
                { length: take },
                (_, i) => rest[(start + i) % rest.length]!,
              );
              cursor = (start + take) % rest.length;
              return [self, ...[...recent, ...window].map((k) => k.member)];
            };

            /** Forget the member that has been gone longest; false if none is gone. */
            const makeRoom = (): boolean => {
              let oldest: [string, number] | undefined;
              for (const [id, entry] of known) {
                if (entry.goneAt !== undefined && (!oldest || entry.goneAt < oldest[1])) {
                  oldest = [id, entry.goneAt];
                }
              }
              if (oldest) known.delete(oldest[0]);
              return oldest !== undefined;
            };

            /** Take in news about a member; returns the event it causes, if any. */
            const learn = (news: Member<Meta>): MembershipEvent<Meta> | undefined => {
              if (news.id === self.id) {
                // Someone thinks we're suspect or gone, or remembers a higher
                // incarnation from an earlier run: say we're alive, louder.
                const outranked =
                  news.incarnation > self.incarnation ||
                  (news.state !== "alive" && news.incarnation === self.incarnation);
                if (outranked && self.state === "alive") {
                  self = { ...self, incarnation: news.incarnation + 1 };
                }
                return undefined;
              }
              const entry = known.get(news.id);
              if (!entry) {
                // Don't learn about members that are already gone.
                if (!isLive(news.state)) return undefined;
                if (known.size >= maxMembers && !makeRoom()) {
                  droppedMembers += 1;
                  return undefined;
                }
                known.set(news.id, {
                  member: news,
                  changedAt: clock.now(),
                  ...(news.state === "suspect" ? { suspectedAt: clock.now() } : {}),
                });
                return { type: "joined", member: news };
              }
              const before = entry.member;
              if (!overrides(news, before)) return undefined;
              entry.member = news;
              entry.changedAt = clock.now();
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
              suspend(() => (stopped ? succeed(undefined) : announce(members.map(learn))));

            /** Send one message and wait for the answer, or give up after `ms`. */
            const ask = (
              address: Address,
              message: Message<Meta>,
              ms: number,
            ): Eff<Message<Meta> | undefined, never> =>
              timeoutOption(
                scoped(
                  transport
                    .connect(address, { codec, timeoutMs: ms, maxMessageBytes })
                    .flatMap((c) => c.send(message).flatMap(() => c.messages.take(1).toArray())),
                )
                  .map((replies) => replies[0])
                  .catch((error) => {
                    noteStreamError(error);
                    return succeed(undefined);
                  }),
                ms,
              ).flatMap((reply) =>
                reply ? merge(reply.members).map(() => reply) : succeed(reply),
              );

            const ping = (address: Address, ms: number) =>
              ask(address, { type: "ping", members: memberList() }, ms).map(
                (reply) => reply?.type === "ack",
              );

            // A request is one message; a connection that idles longer than a
            // few probe timeouts is dropped, so strangers can't hold them all open.
            const connectionTtlMs = pingTimeoutMs * 4 + 1_000;

            // Answer pings, and ping on others' behalf.
            const handle = (connection: Connection<Message<Meta>>) =>
              timeoutOption(
                connection.messages
                  .forEach((message) =>
                    merge(message.members).flatMap(() => {
                      if (message.type === "ping") {
                        return connection.send({ type: "ack", members: memberList() });
                      }
                      if (message.type === "ping-req") {
                        return ping(message.target, pingTimeoutMs).flatMap((reached) =>
                          connection.send({
                            type: reached ? "ack" : "nack",
                            members: memberList(),
                          }),
                        );
                      }
                      return succeed(undefined);
                    }),
                  )
                  // Peers that vanish or send rubbish are normal on a network.
                  .catch((error) => sync(() => noteStreamError(error))),
                connectionTtlMs,
              ).map(() => undefined);

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
                    if (stopped || !current || current.state !== "alive") return succeed(undefined);
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

            /** Ping the seeds that aren't live members (and aren't this member). */
            const contactSeeds: Eff<unknown, never> = suspend(() => {
              lastSeedContact = clock.now();
              return findSeeds.flatMap((addresses) => {
                const members = new Set(liveOthers().map((m) => formatAddress(m.address)));
                members.add(formatAddress(self.address));
                return all(
                  addresses
                    .filter((a) => !members.has(formatAddress(a)))
                    .map((address) => ping(address, pingTimeoutMs)),
                );
              });
            });

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

            // With nobody else known, look for seeds every round. Otherwise
            // look now and then: a seed that started late, or half of a
            // cluster that was cut off and forgotten, is still worth joining.
            const round: Eff<void, never> = expire.flatMap(() => {
              if (liveOthers().length === 0) return contactSeeds.map(() => undefined);
              const probe = nextTarget.flatMap((target) =>
                target ? check(target) : succeed(undefined),
              );
              return clock.now() - lastSeedContact >= seedRefreshMs
                ? all([contactSeeds.map(() => undefined), probe]).map(() => undefined)
                : probe;
            });

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
                [self, ...[...known.values()].map((k) => k.member)]
                  .filter((m) => isLive(m.state))
                  .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)),
              ),
              changes: Stream.fromEffect(events.subscribe).flatMap((stream) =>
                stream.catch(() => Stream.empty()),
              ),
              stats: sync(() => ({
                rejectedMessages: Object.values(rejected).reduce((sum, count) => sum + count, 0),
                rejected: { ...rejected },
                droppedMembers,
                tableSize: known.size,
              })),
              setMetadata: (metadata) =>
                sync(() => {
                  self = { ...self, metadata, incarnation: self.incarnation + 1 };
                }),
            };

            return transport
              .listen(options.address, {
                codec,
                handle,
                maxMessageBytes,
                maxConnections: options.maxInboundConnections ?? 1_024,
              })
              .flatMap((listener) => {
                if (!options.advertise) self = { ...self, address: listener.address };
                return acquireRelease(
                  // Contact the seeds right away, then keep going on our own.
                  contactSeeds.flatMap(() =>
                    forkDaemon(clock.sleep(intervalMs).flatMap(() => loop)),
                  ),
                  (fiber) =>
                    sync(() => {
                      stopped = true;
                      fiber.interrupt();
                    })
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
