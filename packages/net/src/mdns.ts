// Finding cluster members on the local network with multicast DNS (DNS-SD),
// with no DNS server or seed list. Needs the `multicast-dns` package.
//
//   yield* Mdns.announce({ service: "workers", id: "worker-1", address });
//   Gossip.join({ ..., seeds: Mdns.discover({ service: "workers" }) })
//
// Multicast usually works on a LAN and between processes on one machine,
// but is often blocked in clouds and containers; use DNS there.

import {
  Log,
  acquireRelease,
  async,
  fail,
  scoped,
  sleep,
  succeed,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import makeMdns from "multicast-dns";
import type { Discovery } from "./discovery.js";
import { ListenError } from "./errors.js";
import type { Address } from "./transport.js";

export interface MdnsOptions {
  /** UDP port. Default: 5353, the mDNS port. */
  readonly port?: number;
  /** Multicast group. Default: 224.0.0.251. */
  readonly ip?: string;
  /** Network interface to use. Default: all. */
  readonly interface?: string;
}

type Mdns = ReturnType<typeof makeMdns>;

/** `workers` → `_workers._tcp.local`, the DNS-SD name of a service. */
const serviceName = (service: string) => `_${service}._tcp.local`;

/** A multicast DNS socket, ready to use, closed when the scope ends. */
function open(options: MdnsOptions): Eff<Mdns, Throws<ListenError>> {
  return acquireRelease(
    async<Mdns, ListenError>((resume) => {
      const mdns = makeMdns({ ...options, loopback: true, reuseAddr: true });
      const failed = (error: Error) => {
        mdns.destroy();
        resume(
          fail(
            new ListenError({
              address: `mdns:${options.port ?? 5353}`,
              message: error.message,
              cause: error,
            }),
          ),
        );
      };
      mdns.once("error", failed);
      mdns.once("ready", () => {
        mdns.off("error", failed);
        // Later errors (a packet that couldn't be sent) aren't fatal.
        mdns.on(
          "error",
          (error: Error) => void Log.warn("mDNS error", { error: error.message }).run(),
        );
        resume(succeed(mdns));
      });
      return () => mdns.destroy();
    }),
    (mdns) => sync(() => mdns.destroy()),
  );
}

export const Mdns = {
  /**
   * Answer queries for `service` with this member's address, until the
   * surrounding scope ends.
   */
  announce(
    member: { readonly service: string; readonly id: string; readonly address: Address },
    options: MdnsOptions = {},
  ): Eff<void, Throws<ListenError>> {
    if (!("host" in member.address)) {
      return fail(
        new ListenError({
          address: member.id,
          message: "only host and port addresses can be announced",
        }),
      );
    }
    const { host, port } = member.address;
    const service = serviceName(member.service);
    const instance = `${member.id}.${service}`;
    return open(options).map((mdns) => {
      mdns.on("query", (query) => {
        const asked = query.questions.some(
          (q) => q.name === service && (q.type === "PTR" || (q.type as string) === "ANY"),
        );
        if (!asked) return;
        mdns.respond({
          answers: [{ name: service, type: "PTR", data: instance }],
          additionals: [{ name: instance, type: "SRV", data: { target: host, port } }],
        });
      });
    });
  },

  /**
   * Ask who offers `service`, and collect the answers that arrive within
   * `waitMs` (default 500 ms). Never fails; see `Discovery`.
   */
  discover(
    request: { readonly service: string; readonly waitMs?: number },
    options: MdnsOptions = {},
  ): Discovery {
    const service = serviceName(request.service);
    const ask = open(options).flatMap((mdns) => {
      // Fresh for every run, so members that went away aren't reported.
      const found = new Map<string, Address>();
      mdns.on("response", (response) => {
        for (const record of [...response.answers, ...(response.additionals ?? [])]) {
          if (record.type !== "SRV" || !record.name.endsWith(`.${service}`)) continue;
          found.set(record.name, { host: record.data.target, port: record.data.port });
        }
      });
      mdns.query({ questions: [{ name: service, type: "PTR" }] });
      return sleep(request.waitMs ?? 500).map((): readonly Address[] => [...found.values()]);
    });
    return scoped(ask).catch((error) =>
      Log.warn("mDNS discovery failed", { error: error.message }).map((): readonly Address[] => []),
    ) as Discovery;
  },
} as const;
