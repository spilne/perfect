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
- **Clusters:** `Cluster.builder(...).service(...).join()` advertises service
  endpoints, protocol versions and supported command names. `select` finds a
  live peer; `call` adds a deadline and caller-approved retries. `drain` removes
  this node from selection before shutdown. Underneath, `Gossip.join` keeps
  SWIM membership, `HashRing` gives keyed affinity, and `Discovery` (DNS, SRV,
  or mDNS from `@spilne/perfect-net/mdns`) finds seed members.
- **Recording:** `Recorder.wrap` writes down a conversation and `Replay.of`
  plays it back for tests, with no server.
- **Testing:** depend on the `Transport` service and provide
  `SimulatedNetwork` nodes in tests, with latency, message loss, reordering,
  duplication, one-way partitions and node crashes under the test's control
  (deterministic with `TestClock` and `TestRandom`).

- **Hostile peers:** message sizes, receive queues, channels per connection and
  gossip tables are all bounded, and malformed input is rejected with typed
  errors and counters. Gossip has no authentication unless you pass an
  `authenticator` (`GossipAuth.hmacSha256(key)` for a shared cluster key, or
  `GossipAuth.ed25519(...)` for a key per member); it is not
  encryption and does not stop replay. Run it on a private network or over TLS.
  The guide lists every limit.

See the [networking guide](../../documentation/20-networking.md).

## Service cluster

```ts
const app = eff(function* () {
  const cluster = yield* Cluster.builder({ id, address: gossipAddress, seeds })
    .service({ name: "orders", version: "v1", address: ordersAddress, commands: ["get"] })
    .join();
  const peer = yield* cluster.select({ name: "orders", command: "get", version: "v1" });
  return peer.service.address; // Pass this to your HTTP, TCP, or RPC client.
});
```

Provide `Transport` with `Tcp` in production, and run the program inside
`scoped(...)`. The two-process [example](./examples/06-service-cluster.ts)
shows a command listener and an API process using the same cluster API. Give
each running instance a unique `id`; its service `name` stays stable across
restarts. A command list is routing metadata, not a schema or authorization
check: the receiving service must still validate and handle each command.

`select` and `call` exclude suspected and draining members. `call` never retries
unless `retryOn` approves the failure; use retries only for commands safe to
repeat. A hash-ring `key` provides affinity, not exclusive ownership. Use
fenced leases (for example, `PgPartitionedStateBackend`) for stateful work that
must have one owner during a network partition.
