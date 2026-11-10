# Networking

`@spilne/perfect-net` sends typed messages over TCP, Unix domain sockets and
TLS, as effects. It runs on Node and Bun (it uses `node:net` and `node:tls`).

```bash
bun add @spilne/perfect-net
```

## Connections

A server's `handle` function gets every connection on its own fiber. A
connection has a typed `send`, the incoming `messages` as a `Stream`, and
`close`. Listeners and connections are resources: they close when the
surrounding scope ends, which is the end of the program unless you end it
sooner with `.use(...)` or `scoped(...)`.

<!-- @embed packages/net/examples/01-connections.ts#tcp-messages -->

```ts
import { BinaryCodec, type Connection } from "@spilne/perfect-net";

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
```

<!-- @end -->

:::: syntax-tabs

::: syntax generator
<!-- @embed packages/net/examples/01-connections.ts#tcp-echo -->

```ts
import { eff } from "@spilne/perfect-core";
import { Tcp } from "@spilne/perfect-net";

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
```

<!-- @end -->
:::

::: syntax chainable
<!-- @embed packages/net/examples/01-connections.ts#tcp-echo-chained -->

```ts
import { Tcp } from "@spilne/perfect-net";

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
```

<!-- @end -->
:::

::::

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
  message can't be decoded or is too big, or when the connection ends in the
  middle of a message (a cut-off message is never mistaken for a clean end).
  Messages that arrived before a bad one are still delivered, then the
  stream fails and the connection is dropped.
- **Limits:** `maxMessageBytes` (default 16 MiB) bounds one message and
  must fit the 4-byte length header; `highWaterMark` (default 1024) pauses
  the socket while that many received messages wait unread, so a slow reader
  slows the sender instead of growing memory. `maxConnections` on `listen`
  closes connections beyond that many at once (default: no limit).
- **Closing** sends what is still buffered first. If the other side doesn't
  read it within 5 seconds, the socket is closed anyway, and a `send` still
  waiting on it fails with `ConnectionError`.
- **A failing handler** only closes its own connection (the failure is
  logged); the listener keeps serving.
- **TLS:** `Tcp.listen(address, { ..., tls: { key, cert } })` and
  `Tcp.connect(address, { ..., tls: true })`, or `tls: { ca, cert, key, ... }`
  for a custom CA or mutual TLS.

### Knowing who is on the other end

TLS with a CA tells you the server's name; it doesn't give every node a key of
its own. `AuthenticatedTransport.over(transport, { privateKey, trusted })`
wraps any `Transport` (`Tcp`, or a simulated node) so that, before a
connection reaches your code, both ends prove they hold the private key of a
public key in `trusted`. The handler then reads the verified key from
`connection.peer`:

```ts
const me = generateEd25519Identity(); // once per node; share publicKey
const secure = AuthenticatedTransport.over(Tcp, { privateKey: me.privateKey, trusted });

yield * secure.listen(address, { codec, handle: (c) => serve(c, c.peer!) });
yield * secure.connect(address, { codec, peer: expectedServerKey });
```

The handshake is three messages before any of yours: the client sends its key
and a fresh nonce; the server answers with its key, its own nonce and a
signature over both nonces and both keys; the client checks it and sends its
signature over the same data. Because each signature covers both nonces, both
keys and the signer's role, a recorded handshake can't be replayed, a
signature can't be reflected back, and knowing a node's public key is not
enough to pass as that node. A side that sees a key outside `trusted` stops
before signing anything. A handshake that doesn't finish within
`handshakeTimeoutMs` (default 5 000) is dropped, and a connection that fails it
never reaches `handle` (the failure is logged). Without `peer` on `connect`,
any trusted key is accepted as the server's; pass `peer` to pin the one you
mean. Code that only has the `Transport` service can't pass `peer`, but the
`trusted` check still applies.

What it doesn't do: it proves who is on the other end when the connection
opens. After that the messages are neither signed nor encrypted, so someone who
can alter the byte stream can inject or change them. Run it over TLS for that,
and sign anything that has to hold up on its own (a vote, a block) with the
node's key. `trusted` is fixed for the life of the transport; build a new one to
change it.

## WebSockets

`WebSocketClient.connect(url, { codec })` opens a WebSocket as the same
`Connection` as TCP, using the `WebSocket` built into Bun, Node 22+, Deno
and browsers. So reconnecting, channels and recording below work with it
too.

<!-- @embed packages/net/examples/05-websocket.ts#websocket -->

```ts
import { BinaryCodec, WebSocketClient } from "@spilne/perfect-net";

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
```

<!-- @end -->

- **Frames:** messages go as binary frames, or as text with `text: true`
  (the codec's bytes as UTF-8), which most JSON APIs expect. Received
  frames of either kind are decoded.
- **Closing:** a normal close from the server (1000, 1001) ends `messages`;
  any other code fails it with `ConnectionError` ("closed with code 4000:
  reason"). The client closes with 1000 when its scope ends.
- **Backpressure:** `send` waits while more than `maxBufferedBytes`
  (default 1 MiB) wait to go out. The WebSocket API can't pause the other
  side, so received messages are queued up to `maxQueuedMessages` (default
  10 000). Past that, `onOverflow` decides: `"fail"` (default) fails
  `messages` with a `MessageError` after the queued ones and closes the
  socket; `"drop-oldest"` and `"drop-newest"` throw one message away and
  carry on. A message that can't be decoded also closes the socket.
- **Reconnecting:** `Resilient.connect(WebSocketClient.connect(url, opts))`.

## Codecs

A connection's codec turns its messages into bytes and back. Built in:
`BinaryCodec.bytes`, `BinaryCodec.utf8` and `BinaryCodec.json<T>()`. For
compact binary formats, install the library and use its adapter:

| Format           | Import                                                             | Library              |
| ---------------- | ------------------------------------------------------------------ | -------------------- |
| MessagePack      | `msgpackCodec<T>()` from `@spilne/perfect-net/msgpack`             | `msgpackr`           |
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
import { eff, fork } from "@spilne/perfect-core";
import { BinaryCodec, Multiplexer, Tcp, type Connection } from "@spilne/perfect-net";

// Each channel is a Connection of its own. The shared connection carries
// raw bytes; the multiplexer's codec is for the channels' messages.
const codec = BinaryCodec.utf8;

const shout = (connection: Connection<Uint8Array>) =>
  eff(function* () {
    const mux = yield* Multiplexer.make(connection, { side: "server", codec });
    yield* mux.incoming.forEach((channel) =>
      fork(channel.messages.forEach((text) => channel.send(text.toUpperCase()))).map(
        () => undefined,
      ),
    );
  });

const shouted = await eff(function* () {
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
})
  .orDie()
  .run();
console.log(shouted); // → ["HELLO", "BYE"]
```

<!-- @end -->

Each channel has a window of messages (`window`, default 256) that the
sender may have unread on the other side. When the reader falls behind,
that channel's `send` waits, and the other channels keep going. Each side
tells the other its window when a channel opens, so the two sides may use
different windows. When the shared connection closes or breaks, every
channel's messages end, senders waiting for credit fail, and `open` fails.

The other side is not trusted, so what it controls is bounded:

| Option               | Default |                                                                                                                                          |
| -------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `window`             | 256     | unread messages per channel; a peer that sends more has that channel failed (a `MessageError` for the reader, a CLOSE to the peer)       |
| `maxChannels`        | 1024    | open channels, whoever opened them; `open` fails with `ChannelLimitExceeded`, channels the peer opens beyond it are refused with a CLOSE |
| `maxPendingIncoming` | 128     | channels the peer opened that nobody has taken from `incoming`; more are refused                                                         |

A peer that breaks the framing (a repeated, backward or wrong-parity
channel id, an unknown frame kind, a short frame) stops the whole
multiplexer with a `MessageError` and the connection is closed. Closed
channels leave the tables at once; late frames for them are ignored.
`mux.stats` shows the open and pending channels and how many were
refused. The wire format changed (OPEN carries the window, and the
accepting side answers with a CREDIT frame), so both ends must run the same
version.

## Reconnecting

`Resilient.connect(open, { retry, breaker })` wraps the effect that opens a
connection. When the connection breaks or the other side closes it, the
next `send` opens a new one and sends again, and `messages` carries on with
the new connection. Opening is retried with `retry` (by default from
100 ms, doubling up to 5 s, at most 10 times). An optional
`CircuitBreaker` makes callers fail fast while the server is down.

<!-- @embed packages/net/examples/02-channels.ts#resilient -->

```ts
import { RetryPolicy, eff, sleep } from "@spilne/perfect-core";
import { Resilient, Tcp, type Connection } from "@spilne/perfect-net";

// The server answers one message per connection and hangs up; the
// resilient connection opens a new one when it needs to.
const once = (connection: Connection<string>) =>
  connection.messages
    .take(1)
    .forEach((text) => connection.send(`got ${text}`))
    .flatMap(() => connection.close);

const answers = await eff(function* () {
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
})
  .orDie()
  .run();
console.log(answers); // → ["got a", "got b"]
```

<!-- @end -->

A message sent just as the connection breaks may be lost, or it may arrive
twice: once on the old connection and again on the new one. If a protocol
can't tolerate that, have the other side acknowledge messages, or make
messages safe to repeat.

## Clusters

### Services and commands

`Cluster.builder` adds a small service catalog over gossip membership. Each
node advertises service endpoints, protocol versions, and command names. A
client selects only alive, non-draining members that advertise the requested
command. The command list helps routing; the receiving service still validates
payloads and permissions.

```ts
const cluster =
  yield *
  Cluster.builder({ id, address: gossipAddress, seeds })
    .service({ name: "orders", version: "v1", address: ordersAddress, commands: ["get"] })
    .join();
const peer = yield * cluster.select({ name: "orders", version: "v1", command: "get" });
```

`cluster.call(query, invoke, { timeoutMs, maxAttempts?, retryOn? })` runs a
client effect with a deadline. It retries only when `retryOn` approves a typed
failure, so callers can keep non-idempotent commands single-attempt. Call
`cluster.drain` before stopping a service to withdraw its advertisement; the
surrounding scope then leaves the gossip group. Use a unique node `id` for
each process instance, even when the service name stays the same after a
restart. [A two-process example](../packages/net/examples/06-service-cluster.ts)
shows the full TCP path.

The hash-ring `key` gives a consistent preferred peer, but gossip membership
can temporarily differ during a network partition. For exclusive stateful
ownership, use a fenced lease-backed backend instead of the service catalog.

### Consistent hashing

A `HashRing` decides which node owns a key. When a node joins or leaves,
only the keys that node takes over or gives up change owner. A ring is an
immutable value: `with` and `without` return a new one, and `moved` lists
the keys that change owner between two rings.

<!-- @embed packages/net/examples/03-cluster.ts#hash-ring -->

```ts
import { HashRing } from "@spilne/perfect-net";

// Which worker handles which customer? The ring says, and adding a worker
// only moves the customers that the new worker takes over.
const ring = HashRing.of(["worker-1", "worker-2", "worker-3"]);
const customers = Array.from({ length: 1_000 }, (_, i) => `customer-${i}`);

const owner = ring.owner("customer-42"); // one of the workers
const [primary, backup] = ring.owners("customer-42", 2); // for a replica
const moves = ring.moved(ring.with("worker-4"), customers);
console.log(primary); // → owner
console.log(backup !== primary); // → true
assertEq(
  moves.every((move) => move.to === "worker-4"),
  true,
);
```

<!-- @end -->

Each node takes `vnodes` points on the ring (default 128), so keys spread
evenly. Every process that builds a ring from the same nodes gets the same
owners.

### Membership by gossip

`Gossip.join` makes this process a member of a cluster. It needs the
`Transport` service, so a cluster runs on `Tcp` in production and on a
`SimulatedNetwork` in tests. `members` is the current list. `changes`
streams `joined`, `updated` and `left` events, and the ring can follow
them.

<!-- @embed packages/net/examples/03-cluster.ts#gossip -->

```ts
import { eff, provide, sleep } from "@spilne/perfect-core";
import { Discovery, Gossip, HashRing, Tcp, Transport, type Address } from "@spilne/perfect-net";

// Three members on this machine. Each knows only the first one, and learns
// about the others by gossip. The ring follows the membership.
type Meta = { readonly role: string };

const join = (id: string, seeds: readonly Address[]) =>
  provide(
    Gossip.join<Meta>({
      id,
      address: { host: "127.0.0.1", port: 0 },
      seeds: Discovery.of(seeds),
      metadata: { role: "worker" },
      intervalMs: 50,
    }),
    Transport,
    Tcp,
  );

const members = await eff(function* () {
  const first = yield* join("worker-1", []);
  const seed = (yield* first.self).address;
  yield* join("worker-2", [seed]);
  yield* join("worker-3", [seed]);
  yield* sleep(500);

  const members = yield* first.members;
  const ring = HashRing.of(members.map((member) => member.id));
  return { ids: ring.nodes, owner: ring.owner("customer-42") };
})
  .orDie()
  .run();
console.log(members.ids); // → ["worker-1", "worker-2", "worker-3"]
console.log(members.owner); // → owner
```

<!-- @end -->

It follows SWIM. Every `intervalMs`, a member pings another member, taking
them in turn in a shuffled order. When the ping goes unanswered, it asks
`indirectChecks` other members to ping instead, so a bad link between two
members doesn't get either one thrown out. A member that nobody can reach
is suspected. If it doesn't show it is alive within `suspicionMs`, it is
declared dead. A suspected member that is in fact alive raises its
incarnation number, which outranks the suspicion. Every message carries
the sender's member list, so news reaches everyone in a few rounds.
Members leaving at the end of their scope say goodbye, so the others don't
have to wait for them to time out.

| Option                  | Default                                        |                                                                       |
| ----------------------- | ---------------------------------------------- | --------------------------------------------------------------------- |
| `intervalMs`            | 1000                                           | how often to check a member                                           |
| `pingTimeoutMs`         | 300                                            | how long to wait for an answer                                        |
| `indirectChecks`        | 3                                              | members asked to check one that didn't answer                         |
| `suspicionMs`           | 5000                                           | how long a suspect has to show it is alive                            |
| `deadRetentionMs`       | six times `suspicionMs`, at least 30 intervals | how long dead and departed members are remembered                     |
| `seedRefreshMs`         | five intervals, at least `suspicionMs`         | how often to retry seeds that aren't members while others are known   |
| `maxDigestMembers`      | 128                                            | members listed in one message                                         |
| `maxMembersPerMessage`  | 512                                            | members a received message may list (at least `maxDigestMembers + 1`) |
| `maxMessageBytes`       | 1 MiB                                          | the largest message accepted                                          |
| `maxMembers`            | 10 000                                         | members remembered, dead ones included                                |
| `maxInboundConnections` | 1024                                           | gossip connections open at once                                       |
| `authenticator`         | none                                           | signs and verifies messages (see below)                               |

Metadata is sent as JSON with every message, so keep it small.

**Scaling.** A message carries a sample of the member list, not all of it:
this member, the entries that changed most recently, and a window that
rotates over the rest, up to `maxDigestMembers` entries. Message size
therefore doesn't grow with the cluster, but news about a quiet member takes
about `members / maxDigestMembers` rounds to be passed on. A few thousand
members work; at tens of thousands, spreading takes minutes and you should
shard into several clusters. Members that died or left are forgotten after
`deadRetentionMs`; forgetting them earlier than the news takes to spread
lets stale gossip bring them back as new members for a while.

**Seeds.** With no other member known, the seeds are tried every round.
Otherwise the seeds that aren't live members are tried every
`seedRefreshMs`, so a seed that starts late is joined, and two halves of a
cluster that declared each other dead find each other once the network
heals. A seed that never answers is never a member, so it is never
suspected and never reported as having left. Seed discovery runs on the
member's own loop, so it never overlaps with itself and stops with the
member.

**Precedence.** News about a member is ordered by incarnation, then, at the
same incarnation, `dead` and `left` over `suspect` over `alive`. Only the
member itself can raise its incarnation: when it hears it is suspected or
dead at its own incarnation, or hears any higher incarnation than its own
(left over from an earlier run), it outbids it with the next one.

**Hostile messages.** Nothing in a received message is trusted. A message
that is over `maxMessageBytes`, lists more than `maxMembersPerMessage`
members, isn't valid UTF-8 and JSON, or has a member or address that is
malformed or out of range (an unknown state, an incarnation that is
negative, fractional or past 2^40, a port outside 1 to 65535, an id over
256 characters) is dropped as a whole, before anything in it is used, and
the connection is closed. `gossip.stats` counts them by reason
(`undecodable`, `too-large`, `malformed`, `too-many-members`,
`unauthenticated`), counts members ignored because the table was full
(`droppedMembers`), and reports `tableSize`. Connections that sit idle for
a few probe timeouts are dropped, so strangers can't hold every
connection open.

### Gossip security

Gossip has no authentication by default. Anyone who can reach a member's
port can add members, declare others suspect or dead (which others will
refute only if the member is alive and reachable), or make members connect
to arbitrary addresses with `ping-req`. Run gossip on a private network.

To keep strangers out, pass an `authenticator`. `GossipAuth.hmacSha256(key)`
signs every message with HMAC-SHA-256 under a key shared by the cluster
(at least 16 random bytes) and drops messages with a missing or wrong tag
before parsing them:

```ts
Gossip.join({ ..., authenticator: GossipAuth.hmacSha256(process.env.CLUSTER_KEY!) });
```

What this gives you, and what it doesn't: it stops anyone without the key
from taking part or forging news. It is not encryption, so membership and
metadata are visible on the wire. It does not stop replay of a recorded
message; incarnation numbers make most replays harmless, but not all. A
leaked key, or one compromised member, can forge anything.

For a key per member, use `GossipAuth.ed25519`. Each member signs with its own
Ed25519 key and sends its public key in the tag; a message is accepted only
when that key is in the `trusted` list. A leaked key then lets someone
impersonate one member, and you remove a member by leaving its key out of the
list:

```ts
const me = GossipAuth.generateEd25519Identity(); // once per member; share publicKey
Gossip.join({
  ...,
  authenticator: GossipAuth.ed25519({ privateKey: me.privateKey, trusted: allPublicKeys }),
});
```

The signature proves who sent a message, not that what the sender says about
other members is true, and `trusted` is fixed for the life of the
authenticator. For confidentiality, run gossip over TLS with client
certificates (`Transport` options) instead or as well. To plug in something
else (a different MAC, key rotation), implement `GossipAuthenticator`: a
`tagBytes` length, `sign(payload)` and `verify({ payload, tag })`.

### Finding members

`seeds` takes a list of addresses or a `Discovery`, an effect that finds
addresses. Gossip runs it every round while it knows no other member, and
every `seedRefreshMs` otherwise (see Seeds above).

| Discovery                                                    | Finds                                                                     |
| ------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `Discovery.of(addresses)`                                    | the given addresses                                                       |
| `Discovery.dns({ hostname, port })`                          | every address of a DNS name, such as a Kubernetes headless service        |
| `Discovery.srv(name)`                                        | the targets and ports of SRV records                                      |
| `Discovery.all([...])`                                       | everything the given discoveries find                                     |
| `Mdns.discover({ service })` from `@spilne/perfect-net/mdns` | members that `Mdns.announce` on the local network (needs `multicast-dns`) |

A failed lookup logs a warning and finds no addresses. It doesn't fail.

## Testing with a simulated network

Code that gets its connections from the `Transport` service runs on real
sockets when you provide `Tcp`, and on a `SimulatedNetwork` in tests. The
simulator connects named nodes through links whose latency, message loss,
reordering, duplication and partitions the test controls, and it can crash
and restart nodes. It uses the `Clock` and `Random`
services, so with `TestClock` and `TestRandom` a test is deterministic.

<!-- @embed packages/net/examples/01-connections.ts#simulated-network -->

```ts
import { eff, provide } from "@spilne/perfect-core";
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
console.log(simulated); // → { reply: [{ type: "pong", n: 1 }], cutOff: "Left" }
```

<!-- @end -->

| API                                                                   | What it does                                                  |
| --------------------------------------------------------------------- | ------------------------------------------------------------- |
| `SimulatedNetwork.make()`                                             | a new network                                                 |
| `network.node(name)`                                                  | that node's `Transport`; it listens on `{ host: name, port }` |
| `network.link(a, b, { latencyMs, lossRate, ordered, duplicateRate })` | link conditions, both directions                              |
| `network.partition(a, b)` / `network.heal(a, b)`                      | cut and restore the link                                      |
| `network.block(from, to)` / `network.unblock(from, to)`               | cut and restore one direction only                            |
| `network.crash(name)` / `network.restart(name)`                       | close a node's connections and listeners; bring it back empty |

Messages on a link keep their order in each direction, as on TCP, even when
random latency would draw a shorter delay for a later message. `lossRate`
loses whole messages, which TCP never does; use it to test code that must
cope with lost messages, such as heartbeats. Conditions that mean nothing (a
`lossRate` outside 0 to 1, a negative latency, `min` above `max`) throw a
`RangeError` from `link`, and `tls` on `connect` or `listen` fails with a
`ConnectError` or `ListenError`: the simulator does no TLS and won't pretend to.

To test code that has to survive an unreliable network, such as a consensus
protocol, set `ordered: false` on a link so that a message with a short random
delay can overtake an earlier one, and `duplicateRate` to deliver some
messages twice. Each `send` is one whole message, so these act on messages.
The multiplexer and anything else that relies on TCP's ordering breaks on
such a link, which is the point of opting in. `block` cuts one direction: the
other node still reaches this one, and new connections fail because a
connection needs both ways. `crash` closes every connection of a node (its
peers see them end), removes its listeners and refuses everything to and from
it until `restart`. The simulator can't stop the fibers that run the node's
code. The test does that, then starts them again after `restart`, when they
listen and connect afresh. Messages already on their way to a crashed node
are lost.

## Recording and replaying

`Recorder.wrap(connection)` returns a connection that behaves exactly like
the original and writes down every message sent and received. Record a
conversation with the real server once, and save the transcript. From then
on, `Replay.of(transcript)` plays the server's side for the code under test,
with no server.

<!-- @embed packages/net/examples/04-replay.ts#record-replay -->

```ts
import { eff } from "@spilne/perfect-core";
import { BinaryCodec, Recorder, Replay, Tcp, type Connection } from "@spilne/perfect-net";

type Message = { readonly ask: string } | { readonly answer: number };
const codec = BinaryCodec.json<Message>();

// The code under test.
const askLength = (connection: Connection<Message>, word: string) =>
  connection.send({ ask: word }).flatMap(() => connection.messages.take(1).toArray());

const server = (connection: Connection<Message>) =>
  connection.messages.forEach((m) =>
    "ask" in m ? connection.send({ answer: m.ask.length }) : connection.close,
  );

// Once, against the real server: record.
const transcript = await eff(function* () {
  const listener = yield* Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec, handle: server });
  const recording = yield* Recorder.wrap(yield* Tcp.connect(listener.address, { codec }));
  yield* askLength(recording.connection, "hello");
  return yield* recording.transcript;
})
  .orDie()
  .run();
const saved = JSON.stringify(transcript); // e.g. a file next to the test

// From then on: replay. The client gets the recorded answer, and `verify`
// fails if it doesn't send exactly what was recorded.
const replayed = await Replay.of<Message>(JSON.parse(saved))
  .flatMap((replay) =>
    askLength(replay.connection, "hello").flatMap((answer) => replay.verify.map(() => answer)),
  )
  .orDie()
  .run();
console.log(replayed); // → [{ answer: 5 }]
```

<!-- @end -->

The replay keeps the recorded order: a recorded answer is handed over only
after the code under test has sent the message before it. A send that
differs from the recording fails, and so does `verify`, which names the
message and its position. `verify` also fails when the code stops before
the end of the recording. Sends are compared with deep equality; pass
`equals` to compare them differently, for example to ignore timestamps.

## Performance

Messages sent in the same tick leave in one socket write instead of a
packet each. Received messages are read in batches, one stream step per
batch rather than per message. The `net` suite of the performance harness
measures framing, the codecs, the hash ring, and round trips over the
simulator, a multiplexer channel and loopback TCP. Its raw `node:net`
socket shows what the typed connection costs on top:

```bash
bun scripts/perf/collect.ts --suite net
```

## Next

- [Package map](./19-packages.md)
