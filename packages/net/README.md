# @spilne/perfect-net

Networking for [`@spilne/perfect-core`](../core/README.md): typed messages
over TCP, Unix domain sockets, TLS and WebSockets, as effects, plus a network simulator
for tests. Runs on Node and Bun.

## Install

```bash
bun add @spilne/perfect-net
```

## Quickstart

```ts
import { eff, scoped } from "@spilne/perfect-core";
import { BinaryCodec, Tcp } from "@spilne/perfect-net";

const codec = BinaryCodec.json<{ n: number }>();

const program = scoped(
  eff(function* () {
    const listener = yield* Tcp.listen(
      { host: "127.0.0.1", port: 0 },
      {
        codec,
        handle: (connection) => connection.messages.forEach((m) => connection.send({ n: m.n * 2 })),
      },
    );
    const connection = yield* Tcp.connect(listener.address, { codec });
    yield* connection.send({ n: 21 });
    return yield* connection.messages.take(1).toArray(); // [{ n: 42 }]
  }),
);
```

- **Connections:** `Tcp.connect` / `Tcp.listen` with `{ host, port }` or
  `{ path }` (Unix socket), optional `tls`. Each `Connection` has a typed
  `send`, a `messages` stream and `close`, and closes with its scope.
- **Codecs:** `BinaryCodec.bytes`, `.utf8`, `.json<T>()`; MessagePack from
  `@spilne/perfect-net/msgpack` (with `msgpackr`) and Protobuf from
  `@spilne/perfect-net/protobuf` (with `@bufbuild/protobuf`).
- **Channels and reconnecting:** `Multiplexer` runs many channels over one
  connection, each with its own flow control; `Resilient.connect` reopens a
  broken connection with retries and an optional circuit breaker.
- **Clusters:** `Gossip.join` keeps a SWIM membership list, `HashRing`
  assigns keys to members, and `Discovery` (DNS, SRV, or mDNS from
  `@spilne/perfect-net/mdns` with `multicast-dns`) finds the first members.
- **Recording:** `Recorder.wrap` writes down a conversation and `Replay.of`
  plays it back for tests, with no server.
- **Testing:** depend on the `Transport` service and provide
  `SimulatedNetwork` nodes in tests, with latency, message loss and
  partitions under the test's control (deterministic with `TestClock` and
  `TestRandom`).

See the [networking guide](../../documentation/20-networking.md).
