// A WebSocket client: the same typed Connection as TCP.
//
// Run: bun packages/net/examples/05-websocket.ts

import { WebSocketServer } from "ws";
import { BinaryCodec, WebSocketClient } from "../src";
import { assertEq } from "./_assert";

// A tiny server for the example (in real life, the API you talk to).
const server = new WebSocketServer({ port: 0 });
server.on("connection", (socket) =>
  socket.on("message", (data) => {
    const { symbol } = JSON.parse(data.toString()) as { symbol: string };
    for (const price of [101, 102, 103]) socket.send(JSON.stringify({ symbol, price }));
  }),
);
await new Promise<void>((resolve) => server.once("listening", () => resolve()));
const { port } = server.address() as { port: number };
const url = `ws://127.0.0.1:${port}`;

// >>> example: websocket
// Subscribe, then read three ticks. JSON goes as text frames (`text: true`),
// as most WebSocket APIs expect; `.use` closes the socket afterwards.
type Request = { readonly symbol: string };
type Tick = { readonly symbol: string; readonly price: number };
const codec = BinaryCodec.json<Request | Tick>();

const ticks = await WebSocketClient.connect(url, { codec, text: true })
  .use((feed) => feed.send({ symbol: "ACME" }).flatMap(() => feed.messages.take(3).toArray()))
  .orDie()
  .run();
assertEq(ticks, [
  { symbol: "ACME", price: 101 },
  { symbol: "ACME", price: 102 },
  { symbol: "ACME", price: 103 },
]);
// <<< example

server.close();
