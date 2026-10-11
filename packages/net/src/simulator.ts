// A network inside the process, for tests. Each "node" gets a Transport;
// connections between nodes go through links with latency, message loss and
// partitions that the test controls. Nothing touches real sockets.
//
//   const network = yield* SimulatedNetwork.make();
//   network.link("a", "b", { latencyMs: { min: 5, max: 50 } });
//   yield* provide(server, Transport, network.node("b"));   // listens on b
//   yield* provide(client, Transport, network.node("a"));   // connects to { host: "b", port }
//   network.partition("a", "b");                            // now nothing gets through
//
// Delays use the Clock service and random choices the Random service of the
// code that called `make`, so with TestClock and TestRandom a test is fully
// deterministic. Connections use the same framing and codec code as real
// sockets.

import { Duplex } from "node:stream";
import {
  Clock,
  Random,
  acquireRelease,
  fail,
  runFiber,
  runSync,
  succeed,
  suspend,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { connectionOver, type Connection } from "./connection.js";
import { ConnectError, ListenError } from "./errors.js";
import {
  formatAddress,
  serveConnections,
  type Address,
  type ConnectOptions,
  type Listener,
  type ListenOptions,
  type Transport,
} from "./transport.js";

/** How a link between two nodes behaves. Applies in both directions. */
export interface LinkConditions {
  /** Delay for every message, in ms: a fixed number or a random range. Default: 0. */
  readonly latencyMs?: number | { readonly min: number; readonly max: number };
  /**
   * The chance (0 to 1) that a message is lost. Real TCP never loses
   * messages, it retries until the connection breaks; use this to test
   * code that must cope with lost messages, like heartbeats. Default: 0.
   */
  readonly lossRate?: number;
}

const pairKey = (a: string, b: string) => (a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`);

export class SimulatedNetwork {
  private readonly links = new Map<string, LinkConditions>();
  private readonly partitions = new Set<string>();
  private readonly listeners = new Map<string, (socket: Duplex, remote: string) => void>();
  private nextPort = 40_000;

  private constructor(
    private readonly clock: Clock,
    private readonly random: Random,
  ) {}

  /** A new, empty network that uses the caller's Clock and Random. */
  static make(): Eff<SimulatedNetwork, never> {
    return Clock.get.flatMap((clock) =>
      Random.get.map((random) => new SimulatedNetwork(clock, random)),
    ) as Eff<SimulatedNetwork, never>;
  }

  /** Set the conditions of the link between `a` and `b`. */
  link(a: string, b: string, conditions: LinkConditions): void {
    this.links.set(pairKey(a, b), conditions);
  }

  /** Cut `a` off from `b`: messages between them are lost and new connections fail. */
  partition(a: string, b: string): void {
    this.partitions.add(pairKey(a, b));
  }

  /** Undo a partition. */
  heal(a: string, b: string): void {
    this.partitions.delete(pairKey(a, b));
  }

  /** Whether `a` and `b` can reach each other right now. */
  reachable(a: string, b: string): boolean {
    return a === b || !this.partitions.has(pairKey(a, b));
  }

  /**
   * The Transport of node `name`. It listens on `{ host: name, port }` and
   * its connections come from `name`.
   */
  node(name: string): Transport {
    return {
      connect: <M>(address: Address, options: ConnectOptions<M>) =>
        acquireRelease(this.connect(name, address, options), (c) => c.close),
      listen: <M, S>(address: Address, options: ListenOptions<M, S>) =>
        this.listen(name, address, options),
    };
  }

  private connect<M>(
    from: string,
    address: Address,
    options: ConnectOptions<M>,
  ): Eff<Connection<M>, Throws<ConnectError>> {
    return suspend(() => {
      const where = formatAddress(address);
      const host = "path" in address ? from : address.host;
      if (!this.reachable(from, host)) {
        return fail(
          new ConnectError({ address: where, message: `${host} is unreachable from ${from}` }),
        );
      }
      const accept = this.listeners.get(where);
      if (!accept) {
        return fail(
          new ConnectError({ address: where, message: `connection refused by ${where}` }),
        );
      }
      const [client, server] = this.socketPair(from, host);
      accept(server, `${from}:${this.nextPort++}`);
      return succeed(connectionOver(client, where, options));
    });
  }

  private listen<M, S>(
    node: string,
    address: Address,
    options: ListenOptions<M, S>,
  ): Eff<Listener, Throws<ListenError> | Exclude<S, Throws<unknown>>> {
    return serveConnections({
      address,
      options,
      start: (accept) =>
        suspend(() => {
          const bound: Address =
            "path" in address
              ? address
              : { host: node, port: address.port === 0 ? this.nextPort++ : address.port };
          const where = formatAddress(bound);
          if (this.listeners.has(where)) {
            return fail(new ListenError({ address: where, message: `${where} is already in use` }));
          }
          this.listeners.set(where, accept);
          return succeed({ address: bound, stop: () => this.listeners.delete(where) });
        }),
    });
  }

  /** Two connected byte streams, one per side, that go through the a–b link. */
  private socketPair(a: string, b: string): [Duplex, Duplex] {
    let sideA!: Duplex;
    let sideB!: Duplex;
    const toB = this.direction(a, b, () => sideB);
    const toA = this.direction(b, a, () => sideA);
    sideA = this.socket(toB);
    sideB = this.socket(toA);
    return [sideA, sideB];
  }

  private socket(send: (chunk: Uint8Array | null) => void): Duplex {
    return new Duplex({
      // Like a real socket: when the other side is done, this side is too.
      allowHalfOpen: false,
      read() {},
      write(chunk: Uint8Array, _encoding, done) {
        send(chunk);
        done();
      },
      final(done) {
        send(null);
        done();
      },
      destroy(error, done) {
        send(null);
        done(error);
      },
    });
  }

  /**
   * Deliver from `from` to `to` with the link's conditions. Messages keep
   * their order (as on TCP): a message never overtakes an earlier one, even
   * when its random delay is shorter.
   */
  private direction(from: string, to: string, peer: () => Duplex) {
    let lastDelivery = -Infinity;
    let ended = false;
    // Messages on their way, in the order they were sent. Each message's
    // timer delivers the queue up to and including that message: everything
    // in front of it is due by then too, so the order never changes, and a
    // timer firing a moment early can't leave a message stuck.
    let sent = 0;
    let delivered = 0;
    const inFlight: (Uint8Array | null)[] = [];
    const deliverThrough = (number: number) => {
      while (delivered <= number && inFlight.length > 0) {
        const chunk = inFlight.shift()!;
        delivered += 1;
        const target = peer();
        if (!target.destroyed) target.push(chunk);
      }
    };
    return (chunk: Uint8Array | null) => {
      if (ended) return;
      const conditions = this.links.get(pairKey(from, to)) ?? {};
      if (chunk !== null) {
        if (!this.reachable(from, to)) return;
        const lossRate = conditions.lossRate ?? 0;
        if (lossRate > 0 && runSync(this.random.next()) < lossRate) return;
      } else {
        // Closing always gets through, even across a partition, so a test
        // that closes a connection doesn't hang waiting for the other side.
        ended = true;
      }
      const now = this.clock.now();
      const deliverAt = Math.max(now + this.latency(conditions), lastDelivery);
      lastDelivery = deliverAt;
      inFlight.push(chunk);
      const number = sent++;
      const wait = deliverAt - now;
      if (wait <= 0) queueMicrotask(() => deliverThrough(number));
      else runFiber(this.clock.sleep(wait).map(() => deliverThrough(number)));
    };
  }

  private latency(conditions: LinkConditions): number {
    const latency = conditions.latencyMs ?? 0;
    if (typeof latency === "number") return latency;
    return latency.min + runSync(this.random.next()) * (latency.max - latency.min);
  }
}
