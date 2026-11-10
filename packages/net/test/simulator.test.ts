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
  type Throws,
} from "@spilne/perfect-core";
import { BinaryCodec, SimulatedNetwork, Transport, type Connection, type NetError } from "../src";

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

describe("SimulatedNetwork fidelity", () => {
  test("random latency spreads arrivals out but never reorders a direction, in either direction", async () => {
    const clock = new TestClock();
    const atServer: [number, number][] = [];
    const atClient: [number, number][] = [];
    const program = scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        network.link("client", "server", { latencyMs: { min: 1, max: 100 } });
        const listener = yield* network.node("server").listen(
          { host: "server", port: 0 },
          {
            codec,
            handle: (connection) =>
              eff(function* () {
                for (let n = 0; n < 30; n++) yield* connection.send(n);
                yield* connection.messages.forEach((n) =>
                  eff(function* () {
                    atServer.push([n, clock.now()]);
                  }),
                );
              }),
          },
        );
        const connection = yield* network.node("client").connect(listener.address, { codec });
        for (let n = 0; n < 30; n++) yield* connection.send(n);
        yield* connection.messages.forEach((n) =>
          eff(function* () {
            atClient.push([n, clock.now()]);
          }),
        );
      }),
    );
    const fiber = runFiber(
      provide(provide(program, Clock, clock), Random, new TestRandom(11)).orDie(),
    );
    for (let t = 0; t < 400; t++) {
      clock.advance(1);
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    fiber.interrupt();
    const order = Array.from({ length: 30 }, (_, n) => n);
    for (const arrivals of [atServer, atClient]) {
      expect(arrivals.map(([n]) => n)).toEqual(order);
      const times = arrivals.map(([, at]) => at);
      expect(times).toEqual([...times].sort((a, b) => a - b));
      // Control: the latency really varied, so the order above was kept on purpose.
      expect(new Set(times).size).toBeGreaterThan(2);
    }
  });

  test("conditions that mean nothing are refused when the link is set", () => {
    const run = (conditions: Parameters<SimulatedNetwork["link"]>[2]) =>
      provide(
        SimulatedNetwork.make().flatMap((network) =>
          eff(function* () {
            network.link("a", "b", conditions);
          }),
        ),
        Random,
        new TestRandom(1),
      )
        .orDie()
        .run();
    expect(run({ lossRate: 1.5 })).rejects.toThrow(RangeError);
    expect(run({ lossRate: -0.1 })).rejects.toThrow(RangeError);
    expect(run({ lossRate: Number.NaN })).rejects.toThrow(RangeError);
    expect(run({ latencyMs: -5 })).rejects.toThrow(RangeError);
    expect(run({ latencyMs: { min: 50, max: 10 } })).rejects.toThrow(RangeError);
    expect(run({ latencyMs: Number.NaN })).rejects.toThrow(RangeError);
    expect(run({ latencyMs: { min: 1, max: 5 }, lossRate: 0.2 })).resolves.toBeUndefined();
  });

  test("TLS is refused instead of silently ignored", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const secureListen = yield* network
          .node("server")
          .listen({ host: "server", port: 1 }, { codec, handle: doubler, tls: {} })
          .either();
        const listener = yield* network
          .node("server")
          .listen({ host: "server", port: 1 }, { codec, handle: doubler });
        const secureConnect = yield* network
          .node("client")
          .connect(listener.address, { codec, tls: true })
          .either();
        return { listen: secureListen._tag, connect: secureConnect._tag };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({ listen: "Left", connect: "Left" });
  });
});

describe("SimulatedNetwork message-level faults", () => {
  // `server` records what it receives; `client` connects to it. Runs `body`
  // on a TestClock and returns what the server saw.
  const run = async (params: {
    readonly seed?: number;
    readonly setup: (network: SimulatedNetwork) => void;
    readonly body: (
      connection: Connection<number>,
      network: SimulatedNetwork,
    ) => Eff<void, Throws<NetError>>;
  }) => {
    const received: number[] = [];
    const clock = new TestClock();
    const program = scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        params.setup(network);
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
        yield* params.body(connection, network);
        yield* sleep(60);
      }),
    );
    // Virtual time: how long the machine takes to run this can't change the order of arrivals.
    const fiber = runFiber(
      provide(provide(program, Clock, clock), Random, new TestRandom(params.seed ?? 5)).orDie(),
    );
    for (let t = 0; t < 80; t++) {
      clock.advance(1);
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    fiber.interrupt();
    return received;
  };
  const sendAll = (values: number[]) => (connection: Connection<number>) =>
    eff(function* () {
      for (const n of values) yield* connection.send(n);
    });
  const values = Array.from({ length: 40 }, (_, i) => i);

  test("an unordered link lets messages overtake each other, the same way for the same seed", async () => {
    const setup = (network: SimulatedNetwork) =>
      network.link("client", "server", { latencyMs: { min: 1, max: 30 }, ordered: false });
    const first = await run({ setup, body: sendAll(values) });
    expect([...first].sort((a, b) => a - b)).toEqual(values);
    expect(first).not.toEqual(values);
    expect(await run({ setup, body: sendAll(values) })).toEqual(first);
  });

  test("an ordered link with random latency keeps order (the default is unchanged)", async () => {
    const received = await run({
      setup: (network) => network.link("client", "server", { latencyMs: { min: 1, max: 30 } }),
      body: sendAll(values),
    });
    expect(received).toEqual(values);
  });

  test("duplicateRate delivers messages twice, in order on an ordered link", async () => {
    const received = await run({
      setup: (network) => network.link("client", "server", { duplicateRate: 1 }),
      body: sendAll([1, 2, 3]),
    });
    expect(received).toEqual([1, 1, 2, 2, 3, 3]);
  });

  test("duplicates on an unordered link are independent copies", async () => {
    const received = await run({
      setup: (network) =>
        network.link("client", "server", {
          latencyMs: { min: 1, max: 30 },
          ordered: false,
          duplicateRate: 1,
        }),
      body: sendAll(values),
    });
    expect([...received].sort((a, b) => a - b)).toEqual(values.flatMap((n) => [n, n]));
  });

  test("closing waits for messages still on the way on an unordered link", async () => {
    const received = await run({
      setup: (network) =>
        network.link("client", "server", {
          latencyMs: { min: 1, max: 30 },
          ordered: false,
          duplicateRate: 0.5,
        }),
      body: (connection) =>
        eff(function* () {
          for (const n of values) yield* connection.send(n);
          yield* connection.close;
        }),
    });
    expect(new Set(received)).toEqual(new Set(values));
  });

  test("block cuts one direction only and stops new connections", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const atServer: number[] = [];
        const atClient: number[] = [];
        yield* network.node("server").listen(
          { host: "server", port: 1 },
          {
            codec,
            handle: (connection) =>
              connection.messages.forEach((n) =>
                eff(function* () {
                  atServer.push(n);
                  yield* connection.send(n + 100);
                }),
              ),
          },
        );
        const connection = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec });
        yield* fork(
          connection.messages.forEach((n) =>
            eff(function* () {
              atClient.push(n);
            }),
          ),
        );

        network.block("server", "client"); // the client still reaches the server
        yield* connection.send(1);
        yield* sleep(10);
        const fresh = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec })
          .either();

        network.unblock("server", "client");
        yield* connection.send(2);
        yield* sleep(10);
        return { atServer, atClient, fresh: fresh._tag };
      }),
    )
      .orDie()
      .run();
    // 1 reached the server but its reply was lost; after unblocking, 2 round-trips.
    expect(result).toEqual({ atServer: [1, 2], atClient: [102], fresh: "Left" });
  });

  test("crash closes the node's connections and listeners until it restarts", async () => {
    const result = await scoped(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const serve = () =>
          network.node("server").listen({ host: "server", port: 1 }, { codec, handle: doubler });
        yield* serve();
        const connection = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec });
        yield* connection.send(1);
        const before = yield* connection.messages.take(1).toArray();

        network.crash("server");
        const afterCrash = yield* connection.messages.toArray(); // the stream ends
        const refused = yield* network
          .node("client")
          .connect({ host: "server", port: 1 }, { codec })
          .either();
        const listenWhileDown = yield* serve().either();
        const fromDead = yield* network
          .node("server")
          .connect({ host: "client", port: 2 }, { codec })
          .either();

        network.restart("server");
        yield* serve();
        const again = yield* network.node("client").connect({ host: "server", port: 1 }, { codec });
        yield* again.send(5);
        const after = yield* again.messages.take(1).toArray();
        return {
          before,
          afterCrash,
          refused: refused._tag,
          listenWhileDown: listenWhileDown._tag,
          fromDead: fromDead._tag,
          up: network.isUp("server"),
          after,
        };
      }),
    )
      .orDie()
      .run();
    expect(result).toEqual({
      before: [2],
      afterCrash: [],
      refused: "Left",
      listenWhileDown: "Left",
      fromDead: "Left",
      up: true,
      after: [10],
    });
  });

  test("a duplicate rate outside 0 to 1 is refused", async () => {
    const network = await SimulatedNetwork.make().run();
    expect(() => network.link("a", "b", { duplicateRate: 1.5 })).toThrow(RangeError);
  });
});
