// Ways to find the other members of a cluster. A discovery is just an effect
// that returns addresses; Gossip uses one for its seeds, and runs it again
// whenever it knows no other member.
//
//   Gossip.join({ ..., seeds: Discovery.dns({ hostname: "workers.internal", port: 7946 }) })
//
// Discoveries never fail: when a lookup goes wrong they log a warning and
// return no addresses, and the caller tries again later.

import { Log, all, succeed, tryPromise, type Eff } from "@spilne/perfect-core";
import { promises as dns } from "node:dns";
import type { Address } from "./transport.js";

export type Discovery = Eff<readonly Address[], never>;

const quietly = (what: string, lookup: () => Promise<readonly Address[]>): Discovery =>
  tryPromise(lookup, (error) => error).catch((error) =>
    Log.warn(`${what} failed`, { error: String(error) }).map((): readonly Address[] => []),
  );

export const Discovery = {
  /** Always the same addresses. */
  of(addresses: readonly Address[]): Discovery {
    return succeed(addresses);
  },

  /**
   * Every address `hostname` resolves to, with `port`. Fits a Kubernetes
   * headless service or any DNS name with one record per member.
   */
  dns(options: { hostname: string; port: number }): Discovery {
    return quietly(`looking up ${options.hostname}`, async () => {
      const records = await dns.lookup(options.hostname, { all: true });
      return records.map((record) => ({ host: record.address, port: options.port }));
    });
  },

  /** The targets and ports of the SRV records of `name`. */
  srv(name: string): Discovery {
    return quietly(`looking up SRV records of ${name}`, async () => {
      const records = await dns.resolveSrv(name);
      return records.map((record) => ({ host: record.name, port: record.port }));
    });
  },

  /** The addresses all of `discoveries` find, without duplicates. */
  all(discoveries: readonly Discovery[]): Discovery {
    return all([...discoveries]).map((found) => {
      const unique = new Map<string, Address>();
      for (const address of found.flat()) unique.set(JSON.stringify(address), address);
      return [...unique.values()];
    });
  },
} as const;
