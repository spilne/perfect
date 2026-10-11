import { describe, expect, test } from "bun:test";
import {
  Clock,
  Random,
  TestClock,
  TestRandom,
  eff,
  fork,
  join,
  provide,
  runFiber,
  scoped,
  sleep,
  type Eff,
} from "@spilne/perfect-core";
import { BinaryCodec, SimulatedNetwork, Transport, type Connection } from "../src";

const codec = BinaryCodec.json<number>();
const doubler = (connection: Connection<number>) =>
  connection.messages.forEach((n) => connection.send(n * 2));

// Yield to the scheduler a few times so pending work can run.
const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

describe("SimulatedNetwork", () => {
  test("nodes talk through the network like through real sockets", async () => {
    const answers = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec,
            handle: doubler,
          },
        );
        const connection = yield* network.node("client").connect(listener.address, { codec });
        yield* connection.send(1);
        yield* connection.send(2);
        return yield* connection.messages.take(2).toArray();
      }),
    )
      .orDie()
      .run();
    expect(answers).toEqual([2, 4]);
  });

  test("code written against the Transport service runs on it unchanged", async () => {
    const client = Transport.get.flatMap((transport) =>
      transport
        .connect({ host: "server", port: 7 }, { codec })
        .flatMap((connection) =>
          connection.send(21).flatMap(() => connection.messages.take(1).toArray()),
        ),
    );
    const answer = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        yield* network
          .node("server")
          .listen({ host: "server", port: 7 }, { codec, handle: doubler });
        return yield* provide(client, Transport, network.node("client"));
      }),
    )
      .orDie()
      .run();
    expect(answer).toEqual([42]);
  });

  test("latency follows the Clock, so a TestClock controls it", async () => {
    const clock = new TestClock();
    const received: number[] = [];
    const program = scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        network.link("client", "server", { latencyMs: 100 });
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec,
            handle: (connection) =>
              connection.messages.forEach((n) =>
                eff(function* () {
                  received.push(n);
                }),
              ),
          },
        );
        const connection = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec });
        yield* connection.send(1);
        yield* sleep(1_000);
      }),
    );
    // Run it on the side, and move the test clock from here.
    const running = runFiber(provide(program, Clock, clock).orDie());
    await settle();
    expect(received).toEqual([]); // still on its way
    clock.advance(100);
    await settle();
    expect(received).toEqual([1]);
    // Let the program finish (its sleep, and closing over the slow link).
    for (let i = 0; i < 5; i++) {
      clock.advance(1_000);
      await settle();
    }
    expect((await running.await())._tag).toBe("Success");
  });

  test("messages keep their order even with random latency", async () => {
    const values = Array.from({ length: 50 }, (_, i) => i);
    const program = scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        network.link("client", "server", { latencyMs: { min: 1, max: 20 } });
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec,
            handle: doubler,
          },
        );
        const connection = yield* network.node("client").connect(listener.address, { codec });
        for (const n of values) yield* connection.send(n);
        return yield* connection.messages.take(values.length).toArray();
      }),
    );
    const answers = await provide(program, Random, new TestRandom(7)).orDie().run();
    expect(answers).toEqual(values.map((n) => n * 2));
  });

  test("a lossy link drops messages, the same ones for the same seed", async () => {
    const delivered = async (seed: number) => {
      const received: number[] = [];
      await provide(
        scoped(
          eff(function* () {
            const network = yield* SimulatedNetwork.make();
            network.link("client", "server", { lossRate: 0.5 });
            yield* network.node("server").listen(
              { host: "server", port: 1 },
              {
                codec,
                handle: (connection) =>
                  connection.messages.forEach((n) =>
                    eff(function* () {
                      received.push(n);
                    }),
                  ),
              },
            );
            const connection = yield* network
              .node("client")
              .connect({ host: "server", port: 1 }, { codec });
            for (let n = 0; n < 40; n++) yield* connection.send(n);
            yield* sleep(20);
          }),
        ),
        Random,
        new TestRandom(seed),
      )
        .orDie()
        .run();
      return received;
    };
    const first = await delivered(1);
    expect(first.length).toBeGreaterThan(5);
    expect(first.length).toBeLessThan(35);
    expect(await delivered(1)).toEqual(first);
  });

  test("a partition blocks new connections and loses messages until healed", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const received: number[] = [];
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec,
            handle: (connection) =>
              connection.messages.forEach((n) =>
                eff(function* () {
                  received.push(n);
                }),
              ),
          },
        );
        const connection = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec });

        network.partition("client", "server");
        const blocked = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec })
          .either();
        yield* connection.send(1); // lost
        yield* sleep(5);

        network.heal("client", "server");
        yield* connection.send(2);
        yield* sleep(5);
        return { blocked: blocked._tag, received };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ blocked: "Left", received: [2] });
  });

  test("closing a connection ends the other side's messages", async () => {
    const received = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec,
            handle: (connection) => connection.send(1).flatMap(() => connection.close),
          },
        );
        const connection = yield* network.node("client").connect(listener.address, { codec });
        return yield* connection.messages.toArray();
      }),
    )
      .orDie()
      .run();
    expect(received).toEqual([1]);
  });

  test("two listeners can't share an address, and nobody listening refuses", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const server = network.node("server");
        yield* server.listen({ host: "server", port: 1 }, { codec, handle: doubler });
        const second = yield* server
          .listen({ host: "server", port: 1 }, { codec, handle: doubler })
          .either();
        const refused = yield* network
          .node("client")
          .connect({ host: "server", port: 2 }, { codec })
          .either();
        return [second._tag, refused._tag];
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual(["Left", "Left"]);
  });

  test("a client fiber and a server fiber run side by side", async () => {
    const answer = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const server = yield* fork(
          network
            .node("server")
            .listen({ host: "server", port: 3 }, { codec, handle: doubler })
            .flatMap(() => sleep(50) as Eff<void, never>),
        );
        yield* sleep(1);
        const connection = yield* network
          .node("client")
          .connect({ host: "server", port: 3 }, { codec });
        yield* connection.send(5);
        const [reply] = yield* connection.messages.take(1).toArray();
        yield* join(server);
        return reply;
      }),
    )
      .orDie()
      .run();
    expect(answer).toBe(10);
  });
});
