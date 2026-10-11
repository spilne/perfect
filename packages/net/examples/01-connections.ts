// Networking basics — a TCP server and client, and the same code on the
// network simulator.
//
// Run: bun packages/net/examples/01-connections.ts

import { eff, provide, scoped } from "@spilne/perfect-core";
import { BinaryCodec, SimulatedNetwork, Tcp, Transport, type Connection } from "../src";
import { assertEq } from "./_assert";

// >>> example: tcp-echo
// Messages are typed; the codec turns them into bytes. A server's handler
// gets each connection on its own fiber; the listener and the connection are
// closed when the scope ends.
type Message =
  | { readonly type: "ping"; readonly n: number }
  | { readonly type: "pong"; readonly n: number };
const codec = BinaryCodec.json<Message>();

const pongs = (connection: Connection<Message>) =>
  connection.messages.forEach((message) =>
    message.type === "ping" ? connection.send({ type: "pong", n: message.n }) : connection.close,
  );

const replies = await scoped(
  eff(function* () {
    const listener = yield* Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec, handle: pongs });
    const connection = yield* Tcp.connect(listener.address, { codec });
    yield* connection.send({ type: "ping", n: 1 });
    yield* connection.send({ type: "ping", n: 2 });
    return yield* connection.messages.take(2).toArray();
  }),
)
  .orDie()
  .run();
assertEq(replies, [
  { type: "pong", n: 1 },
  { type: "pong", n: 2 },
]);
// <<< example

// >>> example: simulated-network
// Code that uses the Transport service runs on real sockets in production
// and on a SimulatedNetwork in tests, where the test decides about latency,
// lost messages and partitions.
const ping = (n: number) =>
  Transport.get.flatMap((transport) =>
    transport
      .connect({ host: "server", port: 7 }, { codec })
      .flatMap((connection) =>
        connection.send({ type: "ping", n }).flatMap(() => connection.messages.take(1).toArray()),
      ),
  );

const simulated = await scoped(
  eff(function* () {
    const network = yield* SimulatedNetwork.make();
    network.link("client", "server", { latencyMs: { min: 5, max: 20 } });
    yield* network.node("server").listen({ host: "server", port: 7 }, { codec, handle: pongs });

    const reply = yield* provide(ping(1), Transport, network.node("client"));
    network.partition("client", "server");
    const cutOff = yield* provide(ping(2), Transport, network.node("client")).either();
    return { reply, cutOff: cutOff._tag };
  }),
)
  .orDie()
  .run();
assertEq(simulated, { reply: [{ type: "pong", n: 1 }], cutOff: "Left" });
// <<< example
