import { describe, expect, test } from "bun:test";
import { eff, fork, join, scoped, type Eff, type Throws } from "@spilne/perfect-core";
import {
  BinaryCodec,
  Recorder,
  Replay,
  SimulatedNetwork,
  type Connection,
  type NetError,
  type Transcript,
} from "../src";

type Message =
  | { readonly type: "price?"; readonly item: string }
  | { readonly type: "price"; readonly item: string; readonly cents: number };
const codec = BinaryCodec.json<Message>();

const prices: Record<string, number> = { apple: 50, bread: 230 };
const shop = (connection: Connection<Message>) =>
  connection.messages.forEach((m) =>
    m.type === "price?"
      ? connection.send({ type: "price", item: m.item, cents: prices[m.item] ?? 0 })
      : connection.close,
  );

// The code under test: asks for each price and adds them up.
const total = (connection: Connection<Message>, items: string[]): Eff<number, Throws<NetError>> =>
  eff(function* () {
    let sum = 0;
    for (const item of items) {
      yield* connection.send({ type: "price?", item });
      const [answer] = yield* connection.messages.take(1).toArray();
      if (answer?.type === "price") sum += answer.cents;
    }
    return sum;
  });

/** Run `total` against the shop on a simulated network, and record it. */
const record = (items: string[]) =>
  scoped(
    eff(function* () {
      const network = yield* SimulatedNetwork.make();
      yield* network.node("shop").listen({ host: "shop", port: 1 }, { codec, handle: shop });
      const connection = yield* network.node("me").connect({ host: "shop", port: 1 }, { codec });
      const recording = yield* Recorder.wrap(connection);
      const sum = yield* total(recording.connection, items);
      return { sum, transcript: yield* recording.transcript };
    }),
  )
    .orDie()
    .run();

describe("Recorder", () => {
  test("writes down the conversation, and the connection works as before", async () => {
    const { sum, transcript } = await record(["apple", "bread"]);
    expect(sum).toBe(280);
    expect(transcript.map((r) => [r.direction, r.message.type])).toEqual([
      ["sent", "price?"],
      ["received", "price"],
      ["sent", "price?"],
      ["received", "price"],
    ]);
  });
});

describe("Replay", () => {
  const replay = (transcript: Transcript<Message>, items: string[]) =>
    Replay.of(transcript)
      .flatMap((r) =>
        total(r.connection, items)
          .either()
          .flatMap((sum) => r.verify.either().map((verified) => ({ sum, verified }))),
      )
      .run();

  test("the same code gets the recorded answers, with no server", async () => {
    const { transcript } = await record(["apple", "bread"]);
    // A transcript survives being saved as JSON.
    const saved: Transcript<Message> = JSON.parse(JSON.stringify(transcript));
    const result = await replay(saved, ["apple", "bread"]);
    expect(result.sum).toEqual({ _tag: "Right", right: 280 });
    expect(result.verified._tag).toBe("Right");
  });

  test("a send that differs fails, and verify says what and where", async () => {
    const { transcript } = await record(["apple", "bread"]);
    const result = await replay(transcript, ["apple", "cheese"]);
    expect(result.sum._tag).toBe("Left");
    expect(result.verified._tag === "Left" && result.verified.left).toMatchObject({
      at: 2,
      message: expect.stringContaining('"cheese"'),
    });
  });

  test("stopping early fails verify", async () => {
    const { transcript } = await record(["apple", "bread"]);
    const result = await replay(transcript, ["apple"]);
    expect(result.sum).toEqual({ _tag: "Right", right: 50 });
    expect(result.verified._tag === "Left" && result.verified.left.message).toContain(
      "stopped early",
    );
  });

  test("an answer isn't handed over before the question was sent", async () => {
    const { transcript } = await record(["apple"]);
    const result = await Replay.of(transcript)
      .flatMap((r) =>
        // Read first: the answer only comes once the question is sent.
        r.connection.messages
          .take(1)
          .toArray()
          .race(r.connection.send({ type: "price?", item: "apple" }).map(() => "sent" as const)),
      )
      .orDie()
      .run();
    expect(result).toBe("sent");
  });

  test("after the recording, the messages end like a closed connection", async () => {
    const { transcript } = await record(["apple"]);
    const messages = await Replay.of(transcript)
      .flatMap((r) =>
        r.connection
          .send({ type: "price?", item: "apple" })
          .flatMap(() => r.connection.messages.toArray()),
      )
      .orDie()
      .run();
    expect(messages).toEqual([{ type: "price", item: "apple", cents: 50 }]);
  });

  test("sending when the other side should speak first is a mismatch", async () => {
    const transcript: Transcript<Message> = [
      { direction: "received", atMs: 0, message: { type: "price", item: "apple", cents: 50 } },
    ];
    const result = await replay(transcript, ["apple"]);
    expect(result.verified._tag === "Left" && result.verified.left.message).toContain(
      "expects to receive",
    );
  });

  test("closing ends a reader that is waiting for its turn", async () => {
    const { transcript } = await record(["apple"]);
    const result = await Replay.of(transcript)
      .flatMap((r) =>
        eff(function* () {
          const reading = yield* fork(r.connection.messages.toArray());
          yield* r.connection.close;
          const messages = yield* join(reading);
          const sent = yield* r.connection.send({ type: "price?", item: "apple" }).either();
          return { messages, sent: sent._tag };
        }),
      )
      .orDie()
      .run();
    expect(result).toEqual({ messages: [], sent: "Left" });
  });
});
