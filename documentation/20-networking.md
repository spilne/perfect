# Networking

`@spilne/perfect-net` sends typed messages over TCP, Unix domain sockets and
TLS, as effects. It runs on Node and Bun (it uses `node:net` and `node:tls`).

```bash
bun add @spilne/perfect-net
```

## Connections

A server's `handle` function gets every connection on its own fiber. A
connection has a typed `send`, the incoming `messages` as a `Stream`, and
`close`. Listeners and connections are scoped resources: they close when the
surrounding scope ends.

<!-- @embed packages/net/examples/01-connections.ts#tcp-echo -->

```ts
import { eff, scoped } from "@spilne/perfect-core";
import { BinaryCodec, Tcp, type Connection } from "@spilne/perfect-net";

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
```

<!-- @end -->

- **Addresses** are `{ host, port }` or `{ path }` for a Unix socket. Listen
  on port 0 to let the system pick one; `listener.address` tells which.
- **Messages** are framed (a 4-byte length, then the bytes), so one `send`
  is always one message on the other side. `maxMessageBytes` (default
  16 MiB) caps what is accepted.
- **Backpressure:** `send` waits when the socket's buffer is full, and when
  more than `highWaterMark` messages (default 1024) wait unread, reading from
  the socket pauses until the reader catches up.
- **Errors** are typed: `ConnectError` and `ListenError` when opening fails,
  `ConnectionError` when an open connection breaks, `MessageError` when a
  message can't be decoded or is too big.
- **A failing handler** only closes its own connection (the failure is
  logged); the listener keeps serving.
- **TLS:** `Tcp.listen(address, { ..., tls: { key, cert } })` and
  `Tcp.connect(address, { ..., tls: true })`, or `tls: { ca, cert, key, ... }`
  for a custom CA or mutual TLS.

## Codecs

A connection's codec turns its messages into bytes and back. Built in:
`BinaryCodec.bytes`, `BinaryCodec.utf8` and `BinaryCodec.json<T>()`. For
compact binary formats, install the library and use its adapter:

| Format | Import | Library |
| --- | --- | --- |
| MessagePack | `msgpackCodec<T>()` from `@spilne/perfect-net/msgpack` | `msgpackr` |
| Protocol Buffers | `protobufCodec(MessageSchema)` from `@spilne/perfect-net/protobuf` | `@bufbuild/protobuf` |

Any `{ encode(message): Uint8Array; decode(bytes): T }` works as a codec.

## Channels over one connection

`Multiplexer.make(connection, { side, codec })` runs many independent
channels over one connection opened with `BinaryCodec.bytes`. Each channel
is a `Connection` of its own. `mux.open` opens one, and `mux.incoming`
streams the ones the other side opens. `side` is `"client"` on the side
that connected and `"server"` on the side that accepted.

<!-- @embed packages/net/examples/02-channels.ts#multiplexer -->

```ts
import { eff, fork, scoped } from "@spilne/perfect-core";
import { BinaryCodec, Multiplexer, Tcp, type Connection } from "@spilne/perfect-net";

// Each channel is a Connection of its own. The shared connection carries
// raw bytes; the multiplexer's codec is for the channels' messages.
const codec = BinaryCodec.utf8;

const shout = (connection: Connection<Uint8Array>) =>
  scoped(
    eff(function* () {
      const mux = yield* Multiplexer.make(connection, { side: "server", codec });
      yield* mux.incoming.forEach((channel) =>
        fork(channel.messages.forEach((text) => channel.send(text.toUpperCase()))).map(
          () => undefined,
        ),
      );
    }),
  );

const shouted = await scoped(
  eff(function* () {
    const listener = yield* Tcp.listen(
      { host: "127.0.0.1", port: 0 },
      { codec: BinaryCodec.bytes, handle: shout },
    );
    const connection = yield* Tcp.connect(listener.address, { codec: BinaryCodec.bytes });
    const mux = yield* Multiplexer.make(connection, { side: "client", codec });

    const greetings = yield* mux.open;
    const farewells = yield* mux.open;
    yield* greetings.send("hello");
    yield* farewells.send("bye");
    return [
      ...(yield* greetings.messages.take(1).toArray()),
      ...(yield* farewells.messages.take(1).toArray()),
    ];
  }),
)
  .orDie()
  .run();
console.log(shouted); // → ["HELLO", "BYE"]
```

<!-- @end -->

Each channel has a window of messages (`window`, default 256) that the
sender may have unread on the other side. When the reader falls behind,
that channel's `send` waits, and the other channels keep going. When the
shared connection closes or breaks, every channel's messages end and `open`
fails.

## Reconnecting

`Resilient.connect(open, { retry, breaker })` wraps the effect that opens a
connection. When the connection breaks or the other side closes it, the
next `send` opens a new one and sends again, and `messages` carries on with
the new connection. Opening is retried with `retry` (by default from
100 ms, doubling up to 5 s, at most 10 times). An optional
`CircuitBreaker` makes callers fail fast while the server is down.

<!-- @embed packages/net/examples/02-channels.ts#resilient -->

```ts
import { RetryPolicy, eff, scoped, sleep } from "@spilne/perfect-core";
import { Resilient, Tcp, type Connection } from "@spilne/perfect-net";

// The server answers one message per connection and hangs up; the
// resilient connection opens a new one when it needs to.
const once = (connection: Connection<string>) =>
  connection.messages
    .take(1)
    .forEach((text) => connection.send(`got ${text}`))
    .flatMap(() => connection.close);

const answers = await scoped(
  eff(function* () {
    const listener = yield* Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec, handle: once });
    const connection = yield* Resilient.connect(Tcp.connect(listener.address, { codec }), {
      retry: RetryPolicy.exponential({ initial: 10 }).withMaxRetries(5),
    });
    const answers: string[] = [];
    for (const text of ["a", "b"]) {
      yield* connection.send(text);
      answers.push(...(yield* connection.messages.take(1).toArray()));
      yield* sleep(20); // the server hangs up
    }
    return answers;
  }),
)
  .orDie()
  .run();
console.log(answers); // → ["got a", "got b"]
```

<!-- @end -->

A message sent just as the connection breaks may be lost, or it may arrive
twice: once on the old connection and again on the new one. If a protocol
can't tolerate that, have the other side acknowledge messages, or make
messages safe to repeat.

## Testing with a simulated network

Code that gets its connections from the `Transport` service runs on real
sockets when you provide `Tcp`, and on a `SimulatedNetwork` in tests. The
simulator connects named nodes through links whose latency, message loss
and partitions the test controls. It uses the `Clock` and `Random`
services, so with `TestClock` and `TestRandom` a test is deterministic.

<!-- @embed packages/net/examples/01-connections.ts#simulated-network -->

```ts
import { eff, provide, scoped } from "@spilne/perfect-core";
import { SimulatedNetwork, Transport } from "@spilne/perfect-net";

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
console.log(simulated); // → { reply: [{ type: "pong", n: 1 }], cutOff: "Left" }
```

<!-- @end -->

| API | What it does |
| --- | --- |
| `SimulatedNetwork.make()` | a new network |
| `network.node(name)` | that node's `Transport`; it listens on `{ host: name, port }` |
| `network.link(a, b, { latencyMs, lossRate })` | link conditions, both directions |
| `network.partition(a, b)` / `network.heal(a, b)` | cut and restore the link |

Messages on a link keep their order, as on TCP. `lossRate` loses whole
messages, which TCP never does; use it to test code that must cope with lost
messages, such as heartbeats.

## Next

- [Package map](./19-packages.md)
