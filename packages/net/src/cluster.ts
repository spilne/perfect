import {
  acquireRelease,
  fail,
  succeed,
  suspend,
  timeoutFail,
  type Eff,
  type Needs,
  type Throws,
  TaggedError,
} from "@spilne/perfect-core";
import { Gossip, type GossipOptions, type Member, type MembershipEvent } from "./gossip.js";
import { HashRing } from "./hash-ring.js";
import { type Address, type Transport } from "./transport.js";
import type { ListenError } from "./errors.js";
import type { Stream } from "@spilne/perfect-core/stream";

/** A service endpoint and the commands its protocol accepts. */
export interface ClusterService {
  readonly name: string;
  readonly address: Address;
  readonly version: string;
  readonly commands?: readonly string[];
}

export interface ClusterMetadata {
  readonly services: readonly ClusterService[];
  readonly draining: boolean;
}

export type ClusterMember = Member<ClusterMetadata>;

export interface ClusterOptions extends Omit<GossipOptions<ClusterMetadata>, "metadata"> {
  readonly services?: readonly ClusterService[];
}

export interface ServiceQuery {
  readonly name: string;
  readonly version?: string;
  readonly command?: string;
  /** Stable placement for a key; useful for affinity, not exclusive ownership. */
  readonly key?: string;
}

export interface ServicePeer {
  readonly member: ClusterMember;
  readonly service: ClusterService;
}

export class ServiceUnavailable extends TaggedError("ServiceUnavailable")<{
  readonly message: string;
}>() {}

export class ServiceTimeout extends TaggedError("ServiceTimeout")<{
  readonly message: string;
}>() {}

export interface ServiceCallOptions<E> {
  readonly timeoutMs: number;
  /** Attempts beyond the first are made only when retryOn accepts the error. */
  readonly maxAttempts?: number;
  readonly retryOn?: (error: E | ServiceTimeout) => boolean;
}

export interface Cluster {
  readonly self: Eff<ClusterMember, never>;
  readonly members: Eff<ClusterMember[], never>;
  readonly changes: Stream<MembershipEvent<ClusterMetadata>, never>;
  /** Only alive, non-draining members advertising the requested capability. */
  peers(query: ServiceQuery): Eff<ServicePeer[], never>;
  /** Round robin by default; a key chooses a stable member from a hash ring. */
  select(query: ServiceQuery): Eff<ServicePeer, Throws<ServiceUnavailable>>;
  /** Invoke a service with a deadline; retry only errors the caller declares safe. */
  call<A, E>(
    query: ServiceQuery,
    invoke: (peer: ServicePeer) => Eff<A, Throws<E>>,
    options: ServiceCallOptions<E>,
  ): Eff<A, Throws<E | ServiceTimeout | ServiceUnavailable>>;
  /** Stop advertising services before the surrounding scope closes. */
  readonly drain: Eff<void, never>;
}

function matches(service: ClusterService, query: ServiceQuery): boolean {
  return (
    service.name === query.name &&
    (query.version === undefined || service.version === query.version) &&
    (query.command === undefined || service.commands?.includes(query.command) === true)
  );
}

function join(
  options: ClusterOptions,
): Eff<Cluster, Throws<ListenError> | Needs<Transport, "Transport">> {
  const services = options.services ?? [];
  const names = new Set<string>();
  for (const service of services) {
    const identity = `${service.name}\u0000${service.version}`;
    if (!service.name || !service.version || names.has(identity)) {
      throw new TypeError("Cluster services need unique non-empty name/version pairs");
    }
    names.add(identity);
  }
  return Gossip.join({
    ...options,
    metadata: { services, draining: false },
  }).flatMap((gossip) => {
    let next = 0;
    let draining = false;
    const drain = suspend(() => {
      if (draining) return succeed(undefined);
      draining = true;
      return gossip.setMetadata({ services: [], draining: true });
    });
    const peers = (query: ServiceQuery): Eff<ServicePeer[], never> =>
      gossip.members.map((members) =>
        members.flatMap((member) =>
          member.state === "alive" && !member.metadata.draining
            ? member.metadata.services
                .filter((service) => matches(service, query))
                .map((service) => ({ member, service }))
            : [],
        ),
      );
    const cluster: Cluster = {
      self: gossip.self,
      members: gossip.members,
      changes: gossip.changes,
      peers,
      select: (query) =>
        peers(query).flatMap((found) => {
          if (found.length === 0) {
            return fail(new ServiceUnavailable({ message: `No live service ${query.name}` }));
          }
          if (query.key !== undefined) {
            const owner = HashRing.of(found.map((peer) => peer.member.id)).owner(query.key);
            return succeed(found.find((peer) => peer.member.id === owner)!);
          }
          const peer = found[next % found.length]!;
          next++;
          return succeed(peer);
        }),
      call: <A, E>(
        query: ServiceQuery,
        invoke: (peer: ServicePeer) => Eff<A, Throws<E>>,
        callOptions: ServiceCallOptions<E>,
      ): Eff<A, Throws<E | ServiceTimeout | ServiceUnavailable>> => {
        if (!Number.isFinite(callOptions.timeoutMs) || callOptions.timeoutMs < 0) {
          throw new RangeError("Service call timeoutMs must be a non-negative finite number");
        }
        const maxAttempts = callOptions.maxAttempts ?? 1;
        if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
          throw new RangeError("Service call maxAttempts must be a positive integer");
        }
        return peers(query).flatMap(
          (found): Eff<A, Throws<E | ServiceTimeout | ServiceUnavailable>> => {
            if (found.length === 0) {
              return fail(new ServiceUnavailable({ message: `No live service ${query.name}` }));
            }
            const owner =
              query.key === undefined
                ? undefined
                : HashRing.of(found.map((peer) => peer.member.id)).owner(query.key);
            const primary =
              owner === undefined
                ? next++ % found.length
                : found.findIndex((peer) => peer.member.id === owner);
            const attempt = (index: number): Eff<A, Throws<E | ServiceTimeout>> => {
              const peer = found[(primary + index) % found.length]!;
              return timeoutFail(
                invoke(peer),
                callOptions.timeoutMs,
                () =>
                  new ServiceTimeout({ message: `${query.name} timed out at ${peer.member.id}` }),
              ).catchSome((error) =>
                index + 1 < maxAttempts && callOptions.retryOn?.(error)
                  ? attempt(index + 1)
                  : undefined,
              );
            };
            return attempt(0);
          },
        );
      },
      drain,
    };
    return acquireRelease(succeed(cluster), () => drain);
  });
}

/** Fluent configuration for a node that advertises several services. */
export class ClusterBuilder {
  private readonly services: ClusterService[] = [];

  constructor(private readonly options: Omit<ClusterOptions, "services">) {}

  service(service: ClusterService): this {
    this.services.push(service);
    return this;
  }

  join(): Eff<Cluster, Throws<ListenError> | Needs<Transport, "Transport">> {
    return join({ ...this.options, services: [...this.services] });
  }
}

export const Cluster = {
  join,
  builder(options: Omit<ClusterOptions, "services">): ClusterBuilder {
    return new ClusterBuilder(options);
  },
} as const;
