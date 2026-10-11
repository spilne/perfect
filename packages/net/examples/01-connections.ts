// Networking basics — a TCP server and client, and the same code on the
// network simulator.
//
// Run: bun packages/net/examples/01-connections.ts

import { eff, provide } from "@spilne/perfect-core";
import { BinaryCodec, SimulatedNetwork, Tcp, Transport, type Connection } from "../src";
import { assertEq } from "./_assert";

// >>> example: tcp-messages
// Messages are typed; the codec turns them into bytes. A server's handler
// gets each connection on its own fiber, and the connection closes when the
// handler is done.
type Message =
  | { readonly type: "ping"; readonly n: number }
  | { readonly type: "pong"; readonly n: number };
const codec = BinaryCodec.json<Message>();

const pongs = (connection: Connection<Message>) =>
  connection.messages.forEach((message) =>
    message.type === "ping" ? connection.send({ type: "pong", n: message.n }) : connection.close,
  );
// <<< example

// >>> example: tcp-echo
// The listener and the connection close when the program ends.
const replies = await eff(function* () {
  const listener = yield* Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec, handle: pongs });
  const connection = yield* Tcp.connect(listener.address, { codec });
  yield* connection.send({ type: "ping", n: 1 });
  yield* connection.send({ type: "ping", n: 2 });
  return yield* connection.messages.take(2).toArray();
})
  .orDie()
  .run();
assertEq(replies, [
  { type: "pong", n: 1 },
  { type: "pong", n: 2 },
]);
// <<< example

// >>> example: tcp-echo-chained
// `.use` closes the connection as soon as the replies are in; the listener
// closes when the program ends.
const chained = await Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec, handle: pongs })
  .flatMap((listener) =>
    Tcp.connect(listener.address, { codec }).use((connection) =>
      connection
        .send({ type: "ping", n: 1 })
        .flatMap(() => connection.send({ type: "ping", n: 2 }))
        .flatMap(() => connection.messages.take(2).toArray()),
    ),
  )
  .orDie()
  .run();
assertEq(chained, [
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

const simulated = await eff(function* () {
  const network = yield* SimulatedNetwork.make();
  network.link("client", "server", { latencyMs: { min: 5, max: 20 } });
  yield* network.node("server").listen({ host: "server", port: 7 }, { codec, handle: pongs });

  const reply = yield* provide(ping(1), Transport, network.node("client"));
  network.partition("client", "server");
  const cutOff = yield* provide(ping(2), Transport, network.node("client")).either();
  return { reply, cutOff: cutOff._tag };
})
  .orDie()
  .run();
assertEq(simulated, { reply: [{ type: "pong", n: 1 }], cutOff: "Left" });
// <<< example
