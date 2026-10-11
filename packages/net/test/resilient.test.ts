import { describe, expect, test } from "bun:test";
import { RetryPolicy, eff, fork, scoped, sleep } from "@spilne/perfect-core";
import { BinaryCodec, Resilient, SimulatedNetwork, Tcp, type Connection } from "../src";

const codec = BinaryCodec.json<number>();
const quickRetry = RetryPolicy.exponential({ initial: 1 }).withMaxRetries(3);

describe("Resilient.connect", () => {
  test("opens a new connection after the server hangs up", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        let accepted = 0;
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec,
            // Answer one message, then hang up.
            handle: (connection: Connection<number>) => {
              accepted += 1;
              return connection.messages
                .take(1)
                .forEach((n) => connection.send(n * 10))
                .flatMap(() => connection.close);
            },
          },
        );
        const connection = yield* Resilient.connect(
          network.node("client").connect({ host: "server", port: 1 }, { codec }),
          { retry: quickRetry },
        );
        yield* connection.send(1);
        const [first] = yield* connection.messages.take(1).toArray();
        yield* sleep(5); // the hang-up arrives
        yield* connection.send(2);
        const [second] = yield* connection.messages.take(1).toArray();
        return { first, second, accepted };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ first: 10, second: 20, accepted: 2 });
  });

  test("the message stream carries on across reconnects", async () => {
    const received = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        let next = 0;
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec,
            // Each connection gets two numbers, then the server hangs up.
            handle: (connection: Connection<number>) =>
              connection
                .send(next++)
                .flatMap(() => connection.send(next++))
                .flatMap(() => connection.close),
          },
        );
        const connection = yield* Resilient.connect(
          network.node("client").connect({ host: "server", port: 1 }, { codec }),
          { retry: quickRetry },
        );
        return yield* connection.messages.take(6).toArray();
      }),
    )
      .orDie()
      .run();
    expect(received).toEqual([0, 1, 2, 3, 4, 5]);
  });

  test("fails with ConnectError once the retries run out", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        return yield* Resilient.connect(
          network.node("client").connect({ host: "nobody", port: 1 }, { codec }),
          { retry: quickRetry },
        ).either();
      }),
    )
      .orDie()
      .run();
    expect(result._tag).toBe("Left");
    if (result._tag === "Left") expect(result.left._tag).toBe("ConnectError");
  });

  test("after close it doesn't reconnect", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        let accepted = 0;
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec,
            handle: (connection: Connection<number>) => {
              accepted += 1;
              return connection.messages.forEach(() => sleep(0));
            },
          },
        );
        const connection = yield* Resilient.connect(
          network.node("client").connect({ host: "server", port: 1 }, { codec }),
          { retry: quickRetry },
        );
        yield* connection.close;
        const send = yield* connection.send(1).either();
        const messages = yield* connection.messages.toArray();
        yield* sleep(5);
        return { send: send._tag, messages, accepted };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ send: "Left", messages: [], accepted: 1 });
  });

  test("over TCP, it finds the server again after a restart", async () => {
    const answer = await scoped(
      eff(function* () {
        const echo = (connection: Connection<number>) =>
          connection.messages.forEach((n) => connection.send(n + 1));
        // Find a free port, then stop listening on it.
        const address = yield* scoped(
          Tcp.listen({ host: "127.0.0.1", port: 0 }, { codec, handle: echo }).map(
            (listener) => listener.address,
          ),
        );
        // The server comes back a little later, on the same port.
        yield* fork(
          sleep(30)
            .flatMap(() => Tcp.listen(address, { codec, handle: echo }))
            .flatMap(() => sleep(1_000)),
        );
        const resilient = yield* Resilient.connect(Tcp.connect(address, { codec }), {
          retry: RetryPolicy.exponential({ initial: 5 }).withMaxRetries(20),
        });
        yield* resilient.send(41);
        const [reply] = yield* resilient.messages.take(1).toArray();
        return reply;
      }),
    )
      .orDie()
      .run();
    expect(answer).toBe(42);
  });
});
