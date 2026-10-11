// Record a conversation with a real server once, then test the client
// against the recording, with no server.
//
// Run: bun packages/net/examples/04-replay.ts

import { eff } from "@spilne/perfect-core";
import { BinaryCodec, Recorder, Replay, Tcp, type Connection } from "../src";
import { assertEq } from "./_assert";

// >>> example: record-replay
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
assertEq(replayed, [{ answer: 5 }]);
// <<< example
