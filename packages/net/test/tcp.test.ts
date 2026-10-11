import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as net from "node:net";
import {
  acquireRelease,
  eff,
  fail,
  fromPromise,
  scoped,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import {
  BinaryCodec,
  Tcp,
  type Address,
  type Connection,
  type ConnectionOptions,
  type NetError,
} from "../src";
import { encodeFrame } from "../src/frames";

interface Ping {
  readonly n: number;
}
const codec = BinaryCodec.json<Ping>();
const local: Address = { host: "127.0.0.1", port: 0 };

// A server that answers every message with n * 2.
const doubler = (connection: Connection<Ping>) =>
  connection.messages.forEach((message) => connection.send({ n: message.n * 2 }));

const roundTrip = (
  address: Address,
  values: number[],
  options: Partial<ConnectionOptions<Ping>> = {},
) =>
  eff(function* () {
    const connection = yield* Tcp.connect(address, { codec, ...options });
    for (const n of values) yield* connection.send({ n });
    return yield* connection.messages.take(values.length).toArray();
  });

describe("Tcp", () => {
  test("a client and a server exchange messages", async () => {
    const answers = await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, { codec, handle: doubler });
        expect("port" in listener.address && listener.address.port > 0).toBe(true);
        return yield* roundTrip(listener.address, [1, 2, 3]);
      }),
    )
      .orDie()
      .run();
    expect(answers).toEqual([{ n: 2 }, { n: 4 }, { n: 6 }]);
  });

  test("works over a Unix socket", async () => {
    const path = join(tmpdir(), `perfect-net-${process.pid}-${Date.now()}.sock`);
    const answers = await scoped(
      Tcp.listen({ path }, { codec, handle: doubler }).flatMap(() => roundTrip({ path }, [5])),
    )
      .orDie()
      .run();
    expect(answers).toEqual([{ n: 10 }]);
  });

  test("many messages, in order", async () => {
    const values = Array.from({ length: 5_000 }, (_, i) => i);
    const answers = await scoped(
      Tcp.listen(local, { codec, handle: doubler }).flatMap((listener) =>
        roundTrip(listener.address, values, { highWaterMark: 16 }),
      ),
    )
      .orDie()
      .run();
    expect(answers.map((answer) => answer.n)).toEqual(values.map((n) => n * 2));
  });

  test("connecting where nothing listens is a ConnectError", async () => {
    const port = await freePort();
    const result = await scoped(Tcp.connect({ host: "127.0.0.1", port }, { codec }))
      .either()
      .run();
    expect(result._tag === "Left" && result.left._tag).toBe("ConnectError");
  });

  test("a failing handler only closes its own connection", async () => {
    let calls = 0;
    const answers = await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, {
          codec,
          handle: (connection): Eff<void, Throws<NetError | string>> =>
            ++calls === 1 ? fail("first one fails") : doubler(connection),
        });
        // The first connection is closed by the server...
        const first = yield* Tcp.connect(listener.address, { codec });
        const received = yield* first.messages.toArray();
        expect(received).toEqual([]);
        // ...and the listener still serves the next one.
        return yield* roundTrip(listener.address, [21]);
      }),
    )
      .orDie()
      .run();
    expect(answers).toEqual([{ n: 42 }]);
  });

  test("the listener closes with its scope", async () => {
    const address = await scoped(
      Tcp.listen(local, { codec, handle: doubler }).map((listener) => listener.address),
    )
      .orDie()
      .run();
    const result = await scoped(Tcp.connect(address, { codec })).either().run();
    expect(result._tag).toBe("Left");
  });

  test("a message that can't be decoded fails the stream with a MessageError", async () => {
    const result = await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, {
          codec: BinaryCodec.utf8,
          handle: (connection) => connection.send("not json"),
        });
        const connection = yield* Tcp.connect(listener.address, { codec });
        return yield* connection.messages.toArray();
      }),
    )
      .either()
      .run();
    expect(result._tag === "Left" && result.left._tag).toBe("MessageError");
  });

  test("a message over maxMessageBytes is refused", async () => {
    const result = await scoped(
      eff(function* () {
        const listener = yield* rawServer(encodeFrame(new Uint8Array(100)));
        const connection = yield* Tcp.connect(listener, { codec, maxMessageBytes: 10 });
        return yield* connection.messages.toArray();
      }),
    )
      .either()
      .run();
    expect(result._tag === "Left" && result.left.message).toContain("over the limit");
  });

  test("TLS", async () => {
    const key = readFileSync(new URL("./fixtures/localhost-key.pem", import.meta.url));
    const cert = readFileSync(new URL("./fixtures/localhost-cert.pem", import.meta.url));
    const answers = await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, { codec, handle: doubler, tls: { key, cert } });
        const port = "port" in listener.address ? listener.address.port : 0;
        const connection = yield* Tcp.connect(
          { host: "localhost", port },
          { codec, tls: { ca: cert } },
        );
        yield* connection.send({ n: 7 });
        return yield* connection.messages.take(1).toArray();
      }),
    )
      .orDie()
      .run();
    expect(answers).toEqual([{ n: 14 }]);
  });
});

function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

// A plain server that writes `bytes` to whoever connects; closed with the scope.
function rawServer(bytes: Uint8Array): Eff<Address, never> {
  return acquireRelease(
    fromPromise(
      () =>
        new Promise<net.Server>((resolve) => {
          const server = net.createServer((socket) => socket.end(bytes));
          server.listen(0, "127.0.0.1", () => resolve(server));
        }),
      (error) => error,
    ).orDie(),
    (server) => sync(() => server.close()),
  ).map((server) => ({ host: "127.0.0.1", port: (server.address() as net.AddressInfo).port }));
}
