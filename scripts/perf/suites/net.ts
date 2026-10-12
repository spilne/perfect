// Networking suite — @spilne/perfect-net.
//
// Three kinds of case:
//   - CPU work every message pays for: framing, codecs, the hash ring.
//   - Round trips over the in-memory SimulatedNetwork, plain and through a
//     multiplexer channel: the cost of our connection layer without a kernel
//     socket in the way.
//   - Round trips over loopback TCP, next to a raw node:net socket doing the
//     same, so the difference is what the typed Connection costs. Like the
//     http suite, these are reported but never fail a build: loopback timings
//     swing too much between identical trees.
//
// Thresholds are a catastrophic floor (see core.ts), set far above what a
// dev machine measures.

import * as net from "node:net";
import { do_not_optimize } from "mitata";
import { WebSocketServer } from "ws";
import {
  fork,
  join,
  runDisposable,
  scoped,
  succeed,
  type DisposableResource,
  type Eff,
  type Throws,
} from "../../../packages/core/src";
import {
  BinaryCodec,
  HashRing,
  Multiplexer,
  SimulatedNetwork,
  Tcp,
  type Connection,
  type NetError,
} from "../../../packages/net/src";
import { encodeFrame, FrameReader } from "../../../packages/net/src/frames";
import { msgpackCodec } from "../../../packages/net/src/msgpack";
import type { BenchCase, Suite } from "./types";

const ROUND_TRIPS = 1_000;
const FRAMES = 1_000;
const LOOKUPS = 1_000;

interface Order {
  readonly id: number;
  readonly customer: string;
  readonly items: readonly { readonly sku: string; readonly quantity: number }[];
}
const order: Order = {
  id: 42,
  customer: "customer-7",
  items: [
    { sku: "apple", quantity: 3 },
    { sku: "bread", quantity: 1 },
  ],
};
const json = BinaryCodec.json<Order>();
const msgpack = msgpackCodec<Order>();

// 1 000 frames of 64 bytes, as they would arrive on a socket.
const framed = (() => {
  const payload = new Uint8Array(64).fill(7);
  const frames = Array.from({ length: FRAMES }, () => encodeFrame(payload));
  const wire = new Uint8Array(frames.reduce((sum, f) => sum + f.length, 0));
  let offset = 0;
  for (const frame of frames) {
    wire.set(frame, offset);
    offset += frame.length;
  }
  return wire;
})();

const nodes = Array.from({ length: 16 }, (_, i) => `node-${i}`);
const ring = HashRing.of(nodes);
const keys = Array.from({ length: LOOKUPS }, (_, i) => `customer:${i}`);

const echo = <M>(connection: Connection<M>) =>
  connection.messages.forEach((message) => connection.send(message));

/**
 * Send `count` messages and read the `count` echoes, reading while sending
 * as a real client would. (Sending everything first would stall a
 * multiplexer channel once its window fills: that is its flow control.)
 */
const roundTrips = (connection: Connection<Uint8Array>, count: number) => {
  const message = new Uint8Array(32);
  const sendFrom = (sent: number): Eff<void, Throws<NetError>> =>
    sent === count
      ? succeed(undefined)
      : connection.send(message).flatMap(() => sendFrom(sent + 1));
  return fork(connection.messages.take(count).toArray())
    .flatMap((reading) => sendFrom(0).flatMap(() => join(reading)))
    .orDie()
    .run();
};

// Connections kept open for the whole suite, closed in teardown.
let simulated: DisposableResource<Connection<Uint8Array>> | undefined;
let channel: DisposableResource<Connection<Uint8Array>> | undefined;
let tcp: DisposableResource<Connection<Uint8Array>> | undefined;
let raw: { server: net.Server; socket: net.Socket } | undefined;
let ws: DisposableResource<Connection<Uint8Array>> | undefined;
let wsServer: WebSocketServer | undefined;

async function openRaw(): Promise<{ server: net.Server; socket: net.Socket }> {
  const server = net.createServer((socket) => socket.pipe(socket));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as net.AddressInfo;
  const socket = net.createConnection({ host: "127.0.0.1", port });
  socket.setNoDelay(true);
  await new Promise<void>((resolve) => socket.once("connect", resolve));
  return { server, socket };
}

/** Write `count` 36-byte messages and wait until all their bytes come back. */
function rawRoundTrips(socket: net.Socket, count: number): Promise<void> {
  const message = Buffer.alloc(36);
  const expected = message.length * count;
  return new Promise((resolve) => {
    let received = 0;
    const onData = (chunk: Buffer) => {
      received += chunk.length;
      if (received >= expected) {
        socket.off("data", onData);
        resolve();
      }
    };
    socket.on("data", onData);
    for (let i = 0; i < count; i++) socket.write(message);
  });
}

export const netSuite: Suite = {
  name: "net",

  async setup(): Promise<void> {
    const bytes = BinaryCodec.bytes;
    simulated = await runDisposable(
      SimulatedNetwork.make()
        .flatMap((network) =>
          network
            .node("server")
            .listen({ host: "server", port: 1 }, { codec: bytes, handle: echo })
            .flatMap(() =>
              network.node("client").connect({ host: "server", port: 1 }, { codec: bytes }),
            ),
        )
        .orDie(),
    );
    channel = await runDisposable(
      SimulatedNetwork.make()
        .flatMap((network) =>
          network
            .node("server")
            .listen(
              { host: "server", port: 1 },
              {
                codec: bytes,
                handle: (connection) =>
                  scoped(
                    Multiplexer.make(connection, { side: "server", codec: bytes }).flatMap((mux) =>
                      mux.incoming.forEach((incoming) => echo(incoming)),
                    ),
                  ),
              },
            )
            .flatMap(() =>
              network.node("client").connect({ host: "server", port: 1 }, { codec: bytes }),
            )
            .flatMap((connection) => Multiplexer.make(connection, { side: "client", codec: bytes }))
            .flatMap((mux) => mux.open),
        )
        .orDie(),
    );
    tcp = await runDisposable(
      Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec: bytes, handle: echo })
        .flatMap((listener) => Tcp.connect(listener.address, { codec: bytes }))
        .orDie(),
    );
    raw = await openRaw();
    // Only where the tree being measured has the WebSocket client: the
    // baseline comparison runs these suite files against an older tree too.
    const netModule: Record<string, unknown> = await import("../../../packages/net/src");
    const webSocketClient = netModule.WebSocketClient as
      | typeof import("../../../packages/net/src").WebSocketClient
      | undefined;
    if (webSocketClient === undefined) return;
    wsServer = new WebSocketServer({ port: 0 });
    wsServer.on("connection", (socket) => socket.on("message", (data) => socket.send(data)));
    await new Promise<void>((resolve) => wsServer!.once("listening", () => resolve()));
    const { port } = wsServer.address() as net.AddressInfo;
    ws = await runDisposable(
      webSocketClient.connect(`ws://127.0.0.1:${port}`, { codec: BinaryCodec.bytes }).orDie(),
    );
  },

  async teardown(): Promise<void> {
    await simulated?.[Symbol.asyncDispose]();
    await channel?.[Symbol.asyncDispose]();
    await tcp?.[Symbol.asyncDispose]();
    raw?.socket.destroy();
    raw?.server.close();
    await ws?.[Symbol.asyncDispose]();
    await new Promise<void>((resolve) => (wsServer ? wsServer.close(() => resolve()) : resolve()));
    simulated = channel = tcp = raw = ws = wsServer = undefined;
  },

  cases(): readonly BenchCase[] {
    return [
      {
        name: "frames: read 1k x 64 B",
        unit: "ns/item",
        divisor: FRAMES,
        threshold: 2_000,
        run: () => do_not_optimize(new FrameReader().push(framed)),
      },
      {
        name: "json codec: encode + decode",
        unit: "ns/op",
        divisor: 1,
        threshold: 20_000,
        run: () => do_not_optimize(json.decode(json.encode(order))),
      },
      {
        name: "msgpack codec: encode + decode",
        unit: "ns/op",
        divisor: 1,
        threshold: 20_000,
        run: () => do_not_optimize(msgpack.decode(msgpack.encode(order))),
      },
      {
        name: "HashRing.owner, 16 nodes",
        unit: "ns/op",
        divisor: LOOKUPS,
        threshold: 10_000,
        run: () => {
          let last: string | undefined;
          for (const key of keys) last = ring.owner(key);
          return do_not_optimize(last);
        },
      },
      {
        name: "HashRing.of, 16 nodes x 128 points",
        unit: "ns/op",
        divisor: 1,
        threshold: 50_000_000,
        run: () => do_not_optimize(HashRing.of(nodes)),
      },
      {
        name: "simulated network: echo x1k",
        unit: "ns/item",
        divisor: ROUND_TRIPS,
        threshold: 200_000,
        run: () => roundTrips(simulated!.value, ROUND_TRIPS),
      },
      {
        name: "multiplexer channel: echo x1k",
        unit: "ns/item",
        divisor: ROUND_TRIPS,
        threshold: 400_000,
        run: () => roundTrips(channel!.value, ROUND_TRIPS),
      },
      {
        name: "TCP loopback raw node:net: echo x1k (baseline)",
        unit: "ns/item",
        divisor: ROUND_TRIPS,
        gating: false,
        run: () => rawRoundTrips(raw!.socket, ROUND_TRIPS),
      },
      // Measured only where setup found the WebSocket client.
      ...(ws === undefined
        ? []
        : [
            {
              name: "WebSocket loopback: echo x1k",
              unit: "ns/item" as const,
              divisor: ROUND_TRIPS,
              gating: false,
              run: () => roundTrips(ws!.value, ROUND_TRIPS),
            },
          ]),
      {
        name: "TCP loopback Connection: echo x1k",
        unit: "ns/item",
        divisor: ROUND_TRIPS,
        gating: false,
        run: () => roundTrips(tcp!.value, ROUND_TRIPS),
      },
    ];
  },
};
