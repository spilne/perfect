// Everything a connection or listener opens is closed when it stops.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import dgram from "node:dgram";
import * as net from "node:net";
import {
  eff,
  fromPromise,
  scoped,
  sleep,
  sync,
  timeoutOption,
  type Eff,
} from "@spilne/perfect-core";
import { BinaryCodec, SimulatedNetwork, Tcp, type Address } from "../src";
import { Mdns } from "../src/mdns";

const local: Address = { host: "127.0.0.1", port: 0 };
const codec = BinaryCodec.utf8;
const portOf = (address: Address) => ("port" in address ? address.port : 0);

const closedWithin = (socket: net.Socket, ms: number) =>
  new Promise<boolean>((resolve) => {
    if (socket.destroyed) return resolve(true);
    const timer = setTimeout(() => resolve(false), ms);
    socket.once("close", () => {
      clearTimeout(timer);
      resolve(true);
    });
  });

const connect = (port: number): Eff<net.Socket, never> =>
  fromPromise(
    () =>
      new Promise<net.Socket>((resolve) => {
        const socket = net.connect({ host: "127.0.0.1", port }, () => resolve(socket));
        socket.on("error", () => {});
      }),
    (error) => error,
  ).orDie();

const waitClosed = (socket: net.Socket, ms: number): Eff<boolean, never> =>
  fromPromise(
    () => closedWithin(socket, ms),
    (error) => error,
  ).orDie();

describe("maxConnections", () => {
  test("connections over the limit are closed on arrival, and room is made as others leave", async () => {
    const sockets: net.Socket[] = [];
    const outcome = await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, {
          codec,
          maxConnections: 2,
          handle: (connection) => connection.messages.forEach(() => sleep(0)),
        });
        const open = connect(portOf(listener.address)).flatMap((socket) =>
          sync(() => {
            sockets.push(socket);
            return socket;
          }),
        );
        const first = yield* open;
        const second = yield* open;
        yield* sleep(20);
        const third = yield* open;
        const refused = yield* waitClosed(third, 500);
        const firstStays = yield* waitClosed(first, 50);
        const secondStays = yield* waitClosed(second, 50);
        // One leaves; the next one is let in and stays.
        first.destroy();
        yield* sleep(50);
        const fourth = yield* open;
        const fourthStays = yield* waitClosed(fourth, 100);
        return { refused, firstStays, secondStays, fourthStays };
      }),
    )
      .orDie()
      .run();
    for (const socket of sockets) socket.destroy();
    expect(outcome).toEqual({
      refused: true,
      firstStays: false,
      secondStays: false,
      fourthStays: false,
    });
  });

  test("the limit also holds on the simulated network", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        let served = 0;
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec,
            maxConnections: 1,
            handle: (connection) =>
              sync(() => void served++).flatMap(() => connection.messages.forEach(() => sleep(0))),
          },
        );
        const client = network.node("client");
        yield* client.connect(listener.address, { codec });
        const second = yield* client.connect(listener.address, { codec });
        yield* sleep(20);
        yield* second.send("hello").either();
        return { served, secondEnds: yield* timeoutOption(second.messages.toArray(), 200) };
      }),
    )
      .orDie()
      .run();
    expect(result.served).toBe(1);
    expect(result.secondEnds).toEqual([]);
  });

  test("a nonsense limit is rejected", async () => {
    expect(() => Tcp.listen(local, { codec, maxConnections: 0, handle: () => sleep(0) })).toThrow(
      RangeError,
    );
  });
});

describe("TLS listener", () => {
  test("closing it also closes a client that never finished the handshake", async () => {
    const key = readFileSync(new URL("./fixtures/localhost-key.pem", import.meta.url));
    const cert = readFileSync(new URL("./fixtures/localhost-cert.pem", import.meta.url));
    let silent!: net.Socket;
    await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, {
          codec,
          tls: { key, cert },
          handle: () => sleep(0),
        });
        silent = yield* connect(portOf(listener.address));
        return yield* sleep(30);
      }),
    )
      .orDie()
      .run();
    expect(await closedWithin(silent, 1_000)).toBe(true);
  });
});

describe("closing a connection", () => {
  test("an interrupted close doesn't leave its fallback timer behind", async () => {
    const timers = new Set<unknown>();
    const realSet = globalThis.setTimeout;
    const realClear = globalThis.clearTimeout;
    globalThis.setTimeout = ((handler: () => void, ms?: number, ...rest: unknown[]) => {
      const timer = realSet(handler, ms, ...rest);
      // Only the connection's five-second wait for the other end.
      if (ms === 5_000) timers.add(timer);
      return timer;
    }) as typeof setTimeout;
    globalThis.clearTimeout = ((timer: Parameters<typeof clearTimeout>[0]) => {
      timers.delete(timer);
      realClear(timer);
    }) as typeof clearTimeout;
    // Closes its end a moment after the client closes theirs, so every close below ends.
    const server = net.createServer({ allowHalfOpen: true }, (socket) => {
      socket.resume();
      socket.once("end", () => void setTimeout(() => socket.end(), 150));
    });
    try {
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const port = (server.address() as net.AddressInfo).port;
      await scoped(
        eff(function* () {
          const connection = yield* Tcp.connect({ host: "127.0.0.1", port }, { codec });
          // Gives up before the server answers; the connection's own wait must stop with it.
          yield* timeoutOption(connection.close, 50);
        }),
      )
        .orDie()
        .run();
      expect(timers.size).toBe(0);
    } finally {
      globalThis.setTimeout = realSet;
      globalThis.clearTimeout = realClear;
      server.close();
    }
  });
});

describe("mDNS sockets", () => {
  const trackSockets = () => {
    const created: dgram.Socket[] = [];
    const open = new Set<dgram.Socket>();
    const original = dgram.createSocket;
    (dgram as { createSocket: unknown }).createSocket = (...args: Parameters<typeof original>) => {
      const socket = original(...args);
      created.push(socket);
      open.add(socket);
      socket.once("close", () => open.delete(socket));
      return socket;
    };
    return {
      created,
      open,
      restore: () => ((dgram as { createSocket: unknown }).createSocket = original),
    };
  };
  const options = { port: 40_000 + Math.floor(Math.random() * 10_000) };

  test("discover closes its socket when it is interrupted, announce when its scope ends", async () => {
    const sockets = trackSockets();
    try {
      await scoped(
        Mdns.announce(
          { service: "lifecycle", id: "one", address: { host: "10.0.0.1", port: 1 } },
          options,
        ).flatMap(() =>
          timeoutOption(Mdns.discover({ service: "lifecycle", waitMs: 5_000 }, options), 80),
        ),
      )
        .orDie()
        .run();
      await sleep(50).run();
      expect(sockets.created.length).toBe(2);
      expect([...sockets.open]).toEqual([]);
    } finally {
      sockets.restore();
    }
  });

  test("a scope that ends while the socket is still starting doesn't leak it", async () => {
    const sockets = trackSockets();
    try {
      for (let delay = 0; delay < 4; delay++) {
        await timeoutOption(
          scoped(
            Mdns.announce({ service: "race", id: "x", address: { host: "h", port: 1 } }, options),
          ),
          delay,
        )
          .orDie()
          .run();
      }
      await sleep(100).run();
      expect(sockets.created.length).toBeGreaterThan(0);
      expect([...sockets.open]).toEqual([]);
    } finally {
      sockets.restore();
    }
  });
});
