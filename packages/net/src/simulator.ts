// A network inside the process, for tests. Each "node" gets a Transport;
// connections between nodes go through links with latency, message loss and
// partitions that the test controls. Nothing touches real sockets.
//
//   const network = yield* SimulatedNetwork.make();
//   network.link("a", "b", { latencyMs: { min: 5, max: 50 } });
//   yield* provide(server, Transport, network.node("b"));   // listens on b
//   yield* provide(client, Transport, network.node("a"));   // connects to { host: "b", port }
//   network.partition("a", "b");                            // now nothing gets through
//   network.block("a", "b");                                // only a -> b is cut
//   network.crash("b");                                     // b's connections close, its listeners go
//
// There is no TLS on it: connecting or listening with `tls` fails instead of
// pretending to be secure.
//
// Delays use the Clock service and random choices the Random service of the
// code that called `make`, so with TestClock and TestRandom a test is fully
// deterministic. Connections use the same framing and codec code as real
// sockets.

import { Duplex } from "node:stream";
import {
  currentClock,
  currentRandom,
  type Clock,
  type Random,
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
  /**
   * Whether messages arrive in the order they were sent. Default: true, as
   * on TCP. With `false` a message with a short random delay can overtake an
   * earlier one. Every send is one whole message, so this is for code that
   * treats the connection as message-oriented and must cope with reordering,
   * like a consensus protocol. The multiplexer and anything else that
   * relies on TCP's ordering will break on such a link.
   */
  readonly ordered?: boolean;
  /**
   * The chance (0 to 1) that a message arrives twice. The copy takes its own
   * random delay, so it can arrive later than messages sent after it.
   * Default: 0.
   */
  readonly duplicateRate?: number;
}

const pairKey = (a: string, b: string) => (a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`);

/**
 * A network in memory for tests: named nodes, links with latency and loss,
 * and partitions. `network.node(name)` is that node's `Transport`.
 */
export class SimulatedNetwork {
  private readonly links = new Map<string, LinkConditions>();
  private readonly partitions = new Set<string>();
  private readonly blocked = new Set<string>();
  private readonly down = new Set<string>();
  private readonly sockets = new Map<string, Set<Duplex>>();
  private readonly listeners = new Map<
    string,
    { readonly node: string; readonly accept: (socket: Duplex, remote: string) => void }
  >();
  private nextPort = 40_000;

  private constructor(
    private readonly clock: Clock,
    private readonly random: Random,
  ) {}

  /** A new, empty network that uses the caller's Clock and Random. */
  static make(): Eff<SimulatedNetwork, never> {
    return currentClock.flatMap((clock) =>
      currentRandom.map((random) => new SimulatedNetwork(clock, random)),
    );
  }

  /** Set the conditions of the link between `a` and `b`. */
  link(a: string, b: string, conditions: LinkConditions): void {
    const { latencyMs, lossRate, duplicateRate } = conditions;
    if (lossRate !== undefined && !(lossRate >= 0 && lossRate <= 1)) {
      throw new RangeError(`lossRate must be from 0 to 1, got ${lossRate}`);
    }
    if (duplicateRate !== undefined && !(duplicateRate >= 0 && duplicateRate <= 1)) {
      throw new RangeError(`duplicateRate must be from 0 to 1, got ${duplicateRate}`);
    }
    const [min, max] =
      typeof latencyMs === "object"
        ? [latencyMs.min, latencyMs.max]
        : [latencyMs ?? 0, latencyMs ?? 0];
    if (!(min >= 0 && max >= min && Number.isFinite(max))) {
      throw new RangeError(
        `latencyMs must be a finite number of ms from 0 up (or a min and max with min <= max), got ${JSON.stringify(latencyMs)}`,
      );
    }
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

  /**
   * Cut only the direction `from` -> `to`: `to` still reaches `from`. New
   * connections between them fail too, since a connection needs both ways.
   */
  block(from: string, to: string): void {
    this.blocked.add(`${from}\u0000${to}`);
  }

  /** Undo a `block`. */
  unblock(from: string, to: string): void {
    this.blocked.delete(`${from}\u0000${to}`);
  }

  /**
   * Whether messages from `from` reach `to` right now: both nodes are up and
   * neither a partition nor a `block` is in the way.
   */
  reachable(from: string, to: string): boolean {
    if (this.down.has(from) || this.down.has(to)) return false;
    if (from === to) return true;
    return !this.partitions.has(pairKey(from, to)) && !this.blocked.has(`${from}\u0000${to}`);
  }

  /**
   * Crash `name`: every connection it has is closed (its peers see the
   * connection end), its listeners are removed, and nothing reaches it or
   * leaves it until `restart`. The simulator doesn't stop the fibers that
   * run the node's code: the test does that, and starts them again after
   * `restart`, when they have to listen and connect afresh.
   */
  crash(name: string): void {
    this.down.add(name);
    for (const [where, listener] of this.listeners) {
      if (listener.node === name) this.listeners.delete(where);
    }
    for (const socket of [...(this.sockets.get(name) ?? [])]) socket.destroy();
  }

  /** Bring a crashed node back, with no listeners and no connections. */
  restart(name: string): void {
    this.down.delete(name);
  }

  /** Whether `name` has not crashed (or has restarted). */
  isUp(name: string): boolean {
    return !this.down.has(name);
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
      if (options.tls !== undefined) {
        return fail(
          new ConnectError({ address: where, message: "the simulated network doesn't do TLS" }),
        );
      }
      const host = "path" in address ? from : address.host;
      if (this.down.has(from)) {
        return fail(new ConnectError({ address: where, message: `${from} is down` }));
      }
      if (!this.reachable(from, host) || !this.reachable(host, from)) {
        return fail(
          new ConnectError({ address: where, message: `${host} is unreachable from ${from}` }),
        );
      }
      const listener = this.listeners.get(where);
      if (!listener) {
        return fail(
          new ConnectError({ address: where, message: `connection refused by ${where}` }),
        );
      }
      const [client, server] = this.socketPair(from, host);
      listener.accept(server, `${from}:${this.nextPort++}`);
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
          if (options.tls !== undefined) {
            return fail(
              new ListenError({
                address: formatAddress(address),
                message: "the simulated network doesn't do TLS",
              }),
            );
          }
          if (this.down.has(node)) {
            return fail(
              new ListenError({ address: formatAddress(address), message: `${node} is down` }),
            );
          }
          const bound: Address =
            "path" in address
              ? address
              : { host: node, port: address.port === 0 ? this.nextPort++ : address.port };
          const where = formatAddress(bound);
          if (this.listeners.has(where)) {
            return fail(new ListenError({ address: where, message: `${where} is already in use` }));
          }
          this.listeners.set(where, { node, accept });
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
    this.track(a, sideA);
    this.track(b, sideB);
    return [sideA, sideB];
  }

  /** Remember `socket` as belonging to `node` until it closes, so a crash can close it. */
  private track(node: string, socket: Duplex): void {
    let set = this.sockets.get(node);
    if (!set) this.sockets.set(node, (set = new Set()));
    set.add(socket);
    socket.once("close", () => set.delete(socket));
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
    const schedule = (run: () => void, wait: number) => {
      if (wait <= 0) queueMicrotask(run);
      else runFiber(this.clock.sleep(wait).map(run));
    };
    // Goes into the queue above, after everything sent before it.
    const enqueueOrdered = (chunk: Uint8Array | null, latency: number) => {
      const now = this.clock.now();
      const deliverAt = Math.max(now + latency, lastDelivery);
      lastDelivery = deliverAt;
      inFlight.push(chunk);
      const number = sent++;
      schedule(() => deliverThrough(number), deliverAt - now);
    };
    // Arrives on its own schedule, whatever was sent before or after. Closing
    // waits for the latest of these (see `lastDelivery`), so none arrives after it.
    const deliverAlone = (chunk: Uint8Array, latency: number) => {
      lastDelivery = Math.max(lastDelivery, this.clock.now() + latency);
      schedule(() => {
        const target = peer();
        if (!target.destroyed) target.push(chunk);
      }, latency);
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
      const unordered = chunk !== null && conditions.ordered === false;
      if (chunk !== null && unordered) deliverAlone(chunk, this.latency(conditions));
      else enqueueOrdered(chunk, this.latency(conditions));
      const duplicateRate = conditions.duplicateRate ?? 0;
      if (chunk !== null && duplicateRate > 0 && runSync(this.random.next()) < duplicateRate) {
        if (unordered) deliverAlone(chunk, this.latency(conditions));
        else enqueueOrdered(chunk, this.latency(conditions));
      }
    };
  }

  private latency(conditions: LinkConditions): number {
    const latency = conditions.latencyMs ?? 0;
    if (typeof latency === "number") return latency;
    return latency.min + runSync(this.random.next()) * (latency.max - latency.min);
  }
}
