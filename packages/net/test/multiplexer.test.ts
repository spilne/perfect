import { describe, expect, test } from "bun:test";
import { eff, fork, join, scoped, sleep, type Eff, type Throws } from "@spilne/perfect-core";
import { BinaryCodec, Multiplexer, SimulatedNetwork, type Connection, type NetError } from "../src";

const codec = BinaryCodec.json<string>();

// The server side: every channel the client opens answers each message with
// the channel's own prefix, so we can tell channels apart.
const echoServer = (connection: Connection<Uint8Array>): Eff<void, Throws<NetError>> =>
  scoped(
    eff(function* () {
      const mux = yield* Multiplexer.make(connection, { side: "server", codec });
      yield* mux.incoming.forEach((channel) =>
        fork(channel.messages.forEach((text) => channel.send(`${channel.remote}:${text}`))).map(
          () => undefined,
        ),
      );
    }),
  );

const withClient = <A>(
  body: (mux: Multiplexer<string>) => Eff<A, Throws<NetError>>,
  options: { window?: number } = {},
): Promise<A> =>
  scoped(
    SimulatedNetwork.make().flatMap((network) =>
      network
        .node("server")
        .listen({ host: "server", port: 0 }, { codec: BinaryCodec.bytes, handle: echoServer })
        .flatMap((listener) =>
          network.node("client").connect(listener.address, { codec: BinaryCodec.bytes }),
        )
        .flatMap((connection) =>
          Multiplexer.make(connection, { side: "client", codec, ...options }),
        )
        .flatMap(body),
    ),
  )
    .orDie()
    .run();

describe("Multiplexer", () => {
  test("channels carry their own messages over one connection", async () => {
    const answers = await withClient((mux) =>
      eff(function* () {
        const a = yield* mux.open;
        const b = yield* mux.open;
        yield* a.send("x");
        yield* b.send("y");
        yield* a.send("z");
        return {
          a: yield* a.messages.take(2).toArray(),
          b: yield* b.messages.take(1).toArray(),
        };
      }),
    );
    // The client numbers its channels 1, 3, 5...
    const suffixes = (texts: string[]) => texts.map((text) => text.slice(text.indexOf("#")));
    expect(suffixes(answers.a)).toEqual(["#1:x", "#1:z"]);
    expect(suffixes(answers.b)).toEqual(["#3:y"]);
  });

  test("both sides can open channels", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec: BinaryCodec.bytes,
            handle: (connection) =>
              scoped(
                eff(function* () {
                  const mux = yield* Multiplexer.make(connection, { side: "server", codec });
                  const channel = yield* mux.open;
                  yield* channel.send("hello from the server");
                  yield* sleep(50);
                }),
              ),
          },
        );
        const connection = yield* network
          .node("client")
          .connect(listener.address, { codec: BinaryCodec.bytes });
        const mux = yield* Multiplexer.make(connection, { side: "client", codec });
        const [channel] = yield* mux.incoming.take(1).toArray();
        const [text] = yield* channel!.messages.take(1).toArray();
        return { id: channel!.remote.split("#")[1], text };
      }),
    )
      .orDie()
      .run();
    // The accepting side numbers its channels 2, 4, 6...
    expect(result).toEqual({ id: "2", text: "hello from the server" });
  });

  test("a channel nobody reads doesn't hold up the others", async () => {
    const answers = await withClient(
      (mux) =>
        eff(function* () {
          const ignored = yield* mux.open;
          const busy = yield* mux.open;
          // More than the window: the sender waits for credit that never
          // comes, since nobody reads `ignored`'s answers... on the server
          // side the echo blocks too, but only on that channel.
          const stuck = yield* fork(
            eff(function* () {
              for (let i = 0; i < 20; i++) yield* ignored.send(`${i}`);
            }),
          );
          for (let i = 0; i < 10; i++) yield* busy.send(`${i}`);
          const replies = yield* busy.messages.take(10).toArray();
          yield* ignored.messages.take(20).toArray();
          yield* join(stuck);
          return replies.length;
        }),
      { window: 4 },
    );
    expect(answers).toBe(10);
  });

  test("closing a channel ends its messages on the other side", async () => {
    const received = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec: BinaryCodec.bytes,
            handle: (connection) =>
              scoped(
                eff(function* () {
                  const mux = yield* Multiplexer.make(connection, { side: "server", codec });
                  const channel = yield* mux.open;
                  yield* channel.send("one");
                  yield* channel.send("two");
                  yield* channel.close;
                  yield* sleep(50);
                }),
              ),
          },
        );
        const connection = yield* network
          .node("client")
          .connect(listener.address, { codec: BinaryCodec.bytes });
        const mux = yield* Multiplexer.make(connection, { side: "client", codec });
        const [channel] = yield* mux.incoming.take(1).toArray();
        return yield* channel!.messages.toArray();
      }),
    )
      .orDie()
      .run();
    expect(received).toEqual(["one", "two"]);
  });

  test("when the connection goes, every channel ends and opening fails", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec: BinaryCodec.bytes,
            // Accept, then hang up after a moment.
            handle: (connection) => sleep(10).flatMap(() => connection.close),
          },
        );
        const connection = yield* network
          .node("client")
          .connect(listener.address, { codec: BinaryCodec.bytes });
        const mux = yield* Multiplexer.make(connection, { side: "client", codec });
        const channel = yield* mux.open;
        const messages = yield* channel.messages.toArray();
        const reopen = yield* mux.open.either();
        return { messages, reopen: reopen._tag };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ messages: [], reopen: "Left" });
  });
});
