// What happens when things go wrong: full buffers, servers that don't answer,
// messages that can't be encoded or decoded, servers that stay down.

import { describe, expect, test } from "bun:test";
import * as net from "node:net";
import {
  CircuitBreaker,
  Deferred,
  RetryPolicy,
  TestRuntime,
  acquireRelease,
  eff,
  fail,
  fork,
  join,
  scoped,
  sleep,
  sync,
  timeoutOption,
  tryPromise,
  type Eff,
} from "@spilne/perfect-core";
import { BinaryCodec, Resilient, SimulatedNetwork, Tcp, type Address } from "../src";

const local: Address = { host: "127.0.0.1", port: 0 };

/**
 * A server that accepts connections and never says anything. `drop` cuts
 * every connection at once, like a server that dies.
 */
function silentServer(): Eff<{ address: Address; closed: () => number; drop: () => void }, never> {
  let closed = 0;
  const sockets = new Set<net.Socket>();
  return acquireRelease(
    tryPromise(
      () =>
        new Promise<net.Server>((resolve) => {
          const server = net.createServer((socket) => {
            sockets.add(socket);
            // Read (and ignore) what arrives, or the socket never notices
            // that the other side went away.
            socket.resume();
            socket.on("close", () => closed++);
          });
          server.listen(0, "127.0.0.1", () => resolve(server));
        }),
      (error) => error,
    ).orDie(),
    (server) => sync(() => void server.close()),
  ).map((server) => ({
    address: { host: "127.0.0.1", port: (server.address() as net.AddressInfo).port },
    closed: () => closed,
    drop: () => {
      for (const socket of sockets) socket.destroy();
    },
  }));
}

/** A server that accepts connections but never reads them, until dropped. */
function stuckServer(): Eff<{ address: Address; drop: () => void }, never> {
  const sockets = new Set<net.Socket>();
  return acquireRelease(
    tryPromise(
      () =>
        new Promise<net.Server>((resolve) => {
          const server = net.createServer((socket) => {
            sockets.add(socket);
            socket.pause();
          });
          server.listen(0, "127.0.0.1", () => resolve(server));
        }),
      (error) => error,
    ).orDie(),
    (server) => sync(() => void server.close()),
  ).map((server) => ({
    address: { host: "127.0.0.1", port: (server.address() as net.AddressInfo).port },
    drop: () => {
      for (const socket of sockets) socket.destroy();
    },
  }));
}

describe("a full socket buffer", () => {
  // Big messages and a reader that doesn't read: soon the socket's buffer is
  // full and `send` has to wait.
  const chunk = new Uint8Array(1024 * 1024);
  const count = 64;

  test("send waits until the other side reads, then goes on", async () => {
    const result = await scoped(
      eff(function* () {
        const go = yield* Deferred.make<void>();
        let received = 0;
        const listener = yield* Tcp.listen(local, {
          codec: BinaryCodec.bytes,
          highWaterMark: 1,
          handle: (connection) =>
            go.await.flatMap(() =>
              connection.messages.forEach(() =>
                sync(() => {
                  received++;
                }),
              ),
            ),
        });
        const connection = yield* Tcp.connect(listener.address, { codec: BinaryCodec.bytes });
        let sent = 0;
        const sending = yield* fork(
          eff(function* () {
            for (let i = 0; i < count; i++) {
              yield* connection.send(chunk);
              sent++;
            }
          }),
        );
        yield* sleep(100);
        const sentBeforeReading = sent;
        yield* go.succeed(undefined);
        yield* join(sending);
        while (received < count) yield* sleep(5);
        return { sentBeforeReading, received };
      }),
    )
      .orDie()
      .run();
    expect(result.sentBeforeReading).toBeLessThan(count);
    expect(result.received).toBe(count);
  });

  test("a send waiting on a full buffer fails when the connection breaks", async () => {
    const result = await scoped(
      eff(function* () {
        const server = yield* stuckServer();
        const connection = yield* Tcp.connect(server.address, { codec: BinaryCodec.bytes });
        const sending = yield* fork(
          eff(function* () {
            for (let i = 0; i < count; i++) yield* connection.send(chunk);
          }).either(),
        );
        yield* sleep(100);
        server.drop();
        return yield* join(sending);
      }),
    )
      .orDie()
      .run();
    expect(result._tag).toBe("Left");
    if (result._tag === "Left") expect(result.left._tag).toBe("ConnectionError");
  });
});

describe("connecting", () => {
  test("gives up after timeoutMs when the server doesn't answer", async () => {
    // The TLS handshake never completes against a server that stays quiet.
    const result = await scoped(
      silentServer().flatMap((server) =>
        Tcp.connect(server.address, {
          codec: BinaryCodec.bytes,
          tls: true,
          timeoutMs: 50,
        }).either(),
      ),
    ).run();
    expect(result._tag === "Left" && result.left.message).toContain("timed out");
  });

  test("an interrupted connect closes its socket", async () => {
    const closed = await scoped(
      silentServer().flatMap((server) =>
        timeoutOption(
          Tcp.connect(server.address, { codec: BinaryCodec.bytes, tls: true }).either(),
          30,
        ).flatMap((outcome) => sleep(50).map(() => ({ outcome, closed: server.closed() }))),
      ),
    ).run();
    expect(closed).toEqual({ outcome: undefined, closed: 1 });
  });

  test("a listener closed by hand can still end with its scope", async () => {
    const result = await scoped(
      eff(function* () {
        const listener = yield* Tcp.listen(local, {
          codec: BinaryCodec.bytes,
          handle: () => sleep(0),
        });
        yield* listener.close;
        return yield* Tcp.connect(listener.address, { codec: BinaryCodec.bytes }).either();
      }),
    )
      .orDie()
      .run();
    expect(result._tag).toBe("Left");
  });
});

describe("messages that don't fit the codec", () => {
  test("a message the codec can't encode fails the send, not the connection", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const codec = BinaryCodec.json<{ n: number | bigint }>();
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec,
            handle: (connection) => connection.messages.forEach((m) => connection.send(m)),
          },
        );
        const connection = yield* network.node("client").connect(listener.address, { codec });
        const bad = yield* connection.send({ n: 1n }).either();
        yield* connection.send({ n: 2 });
        const [echo] = yield* connection.messages.take(1).toArray();
        return { bad: bad._tag === "Left" && bad.left._tag, echo };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ bad: "MessageError", echo: { n: 2 } });
  });
});

describe("Resilient.connect when things go wrong", () => {
  test("a message it can't decode fails the stream instead of reconnecting", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        let accepted = 0;
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec: BinaryCodec.utf8,
            handle: (connection) => {
              accepted++;
              return connection.send("this is not JSON");
            },
          },
        );
        const connection = yield* Resilient.connect(
          network
            .node("client")
            .connect({ host: "server", port: 1 }, { codec: BinaryCodec.json<number>() }),
        );
        const messages = yield* connection.messages.toArray().either();
        return { failed: messages._tag === "Left" && messages.left._tag, accepted };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ failed: "MessageError", accepted: 1 });
  });

  test("with a circuit breaker, callers fail fast while the server is down", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const breaker = CircuitBreaker.make<unknown>({
          failureThreshold: 1,
          resetTimeoutMs: 60_000,
        });
        const open = network
          .node("client")
          .connect({ host: "nobody", port: 1 }, { codec: BinaryCodec.bytes });
        const options = {
          retry: RetryPolicy.exponential({ initial: 1 }).withMaxRetries(0),
          breaker,
        };
        const first = yield* Resilient.connect(open, options).either();
        const second = yield* Resilient.connect(open, options).either();
        return [first, second].map((r) => (r._tag === "Left" ? r.left.message : "connected"));
      }),
    )
      .orDie()
      .run();
    expect(result[0]).not.toContain("circuit breaker");
    expect(result[1]).toContain("circuit breaker is open");
  });
});

describe("handler failures in the log", () => {
  test("a failing handler is logged with its cause; a listener closing logs nothing", async () => {
    const t = TestRuntime.make();
    await t.runAdvancing(
      scoped(
        eff(function* () {
          const network = yield* SimulatedNetwork.make();
          const codec = BinaryCodec.utf8;
          yield* network
            .node("server")
            .listen(
              { host: "server", port: 1 },
              { codec, handle: () => fail("the handler broke") },
            );
          yield* network.node("server").listen(
            { host: "server", port: 2 },
            // Still running when the listener closes at the end of the scope.
            { codec, handle: () => sleep(60_000) },
          );
          yield* network.node("client").connect({ host: "server", port: 1 }, { codec });
          yield* network.node("client").connect({ host: "server", port: 2 }, { codec });
          yield* sleep(0);
        }),
      ).orDie(),
    );
    const warnings = t.logger.atLevel("warn");
    expect(warnings.map((w) => w.message)).toEqual(["connection handler failed"]);
    expect(warnings[0]?.annotations.cause).toBe("Fail(the handler broke)");
  });
});
