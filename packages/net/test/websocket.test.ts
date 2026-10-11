import { afterEach, describe, expect, test } from "bun:test";
import { RetryPolicy, eff, fork, join, sleep, tryPromise } from "@spilne/perfect-core";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";
import { BinaryCodec, Resilient, WebSocketClient } from "../src";

type Message = { readonly n: number };
const codec = BinaryCodec.json<Message>();

/** A server on a free port that hands each connection to `onConnection`. */
async function serve(onConnection: (socket: ServerSocket) => void) {
  const server = new WebSocketServer({ port: 0 });
  server.on("connection", onConnection);
  await new Promise<void>((resolve) => server.once("listening", () => resolve()));
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  servers.push(server);
  return { url: `ws://127.0.0.1:${port}`, server };
}
const servers: WebSocketServer[] = [];
afterEach(async () => {
  for (const server of servers.splice(0)) {
    for (const client of server.clients) client.terminate();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

/** Echoes every message back with n doubled, keeping the frame kind. */
const doubler = (socket: ServerSocket) =>
  socket.on("message", (data, isBinary) => {
    const message = JSON.parse(data.toString()) as Message;
    const reply = JSON.stringify({ n: message.n * 2 });
    socket.send(isBinary ? Buffer.from(reply) : reply);
  });

describe("WebSocketClient", () => {
  test("sends and receives typed messages, as binary frames by default", async () => {
    const kinds: boolean[] = [];
    const { url } = await serve((socket) => {
      socket.on("message", (_data, isBinary) => kinds.push(isBinary));
      doubler(socket);
    });
    const replies = await WebSocketClient.connect(url, { codec })
      .use((connection) =>
        connection
          .send({ n: 1 })
          .flatMap(() => connection.send({ n: 2 }))
          .flatMap(() => connection.messages.take(2).toArray()),
      )
      .orDie()
      .run();
    expect(replies).toEqual([{ n: 2 }, { n: 4 }]);
    expect(kinds).toEqual([true, true]);
  });

  test("text: true sends text frames, which most JSON APIs expect", async () => {
    const kinds: boolean[] = [];
    const { url } = await serve((socket) => {
      socket.on("message", (_data, isBinary) => kinds.push(isBinary));
      doubler(socket);
    });
    const replies = await WebSocketClient.connect(url, { codec, text: true })
      .use((connection) =>
        connection.send({ n: 5 }).flatMap(() => connection.messages.take(1).toArray()),
      )
      .orDie()
      .run();
    expect(replies).toEqual([{ n: 10 }]);
    expect(kinds).toEqual([false]);
  });

  test("the messages end when the server closes normally", async () => {
    const { url } = await serve((socket) => {
      socket.send(JSON.stringify({ n: 1 }));
      socket.close(1000, "bye");
    });
    const messages = await WebSocketClient.connect(url, { codec })
      .use((connection) => connection.messages.toArray())
      .orDie()
      .run();
    expect(messages).toEqual([{ n: 1 }]);
  });

  test("an abnormal close fails the messages with ConnectionError", async () => {
    const { url } = await serve((socket) => socket.close(4000, "go away"));
    const result = await WebSocketClient.connect(url, { codec })
      .use((connection) => connection.messages.toArray())
      .either()
      .run();
    expect(result._tag === "Left" && result.left).toMatchObject({
      _tag: "ConnectionError",
      message: "closed with code 4000: go away",
    });
  });

  test("a message that can't be decoded fails with MessageError", async () => {
    const { url } = await serve((socket) => socket.send("not json"));
    const result = await WebSocketClient.connect(url, { codec })
      .use((connection) => connection.messages.take(1).toArray())
      .either()
      .run();
    expect(result._tag === "Left" && result.left._tag).toBe("MessageError");
  });

  test("nothing listening fails with ConnectError", async () => {
    const { url, server } = await serve(() => {});
    await new Promise<void>((resolve) => server.close(() => resolve()));
    servers.splice(servers.indexOf(server), 1);
    const result = await WebSocketClient.connect(url, { codec, timeoutMs: 2_000 }).either().run();
    expect(result._tag === "Left" && result.left._tag).toBe("ConnectError");
  });

  test("it closes with the scope, and the server sees a normal close", async () => {
    let closeCode = 0;
    const closed = new Promise<void>((resolve) => {
      void serve((socket) =>
        socket.on("close", (code) => {
          closeCode = code;
          resolve();
        }),
      ).then(({ url }) =>
        WebSocketClient.connect(url, { codec })
          .use(() => sleep(10))
          .orDie()
          .run(),
      );
    });
    await closed;
    expect(closeCode).toBe(1000);
  });

  test("with Resilient.connect it reconnects after the server drops it", async () => {
    let connections = 0;
    const { url } = await serve((socket) => {
      connections++;
      socket.on("message", (data) => {
        const message = JSON.parse(data.toString()) as Message;
        socket.send(JSON.stringify({ n: message.n * 2 }));
        // Answer one message per connection, then drop it.
        socket.close(1001, "going away");
      });
    });
    const replies = await eff(function* () {
      const connection = yield* Resilient.connect(WebSocketClient.connect(url, { codec }), {
        retry: RetryPolicy.exponential({ initial: 10 }).withMaxRetries(5),
      });
      const reading = yield* fork(connection.messages.take(2).toArray());
      yield* connection.send({ n: 1 });
      yield* tryPromise(
        () => new Promise((resolve) => setTimeout(resolve, 50)),
        () => undefined,
      ).orDie();
      yield* connection.send({ n: 2 });
      return yield* join(reading);
    })
      .orDie()
      .run();
    expect(replies).toEqual([{ n: 2 }, { n: 4 }]);
    expect(connections).toBe(2);
  });
});
