// Channels over one connection, and a connection that reconnects by itself.
//
// Run: bun packages/net/examples/02-channels.ts

import { RetryPolicy, eff, fork, sleep } from "@spilne/perfect-core";
import { BinaryCodec, Multiplexer, Resilient, Tcp, type Connection } from "../src";
import { assertEq } from "./_assert";

// >>> example: multiplexer
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
assertEq(shouted, ["HELLO", "BYE"]);
// <<< example

// >>> example: resilient
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
assertEq(answers, ["got a", "got b"]);
// <<< example
