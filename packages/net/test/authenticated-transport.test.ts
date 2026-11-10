import { describe, expect, test } from "bun:test";
import { eff, scoped, type Eff, type Throws } from "@spilne/perfect-core";
import {
  AuthenticatedTransport,
  BinaryCodec,
  SimulatedNetwork,
  Tcp,
  generateEd25519Identity,
  type Connection,
} from "../src";
import { concatBytes, ed25519Signer, toHex } from "../src/ed25519";

const codec = BinaryCodec.json<number>();
const [alice, bob, mallory] = [
  generateEd25519Identity(),
  generateEd25519Identity(),
  generateEd25519Identity(),
];
const trusted = [alice.publicKey, bob.publicKey];
const address = { host: "server", port: 1 };

const run = <A>(program: Eff<A, Throws<unknown>>) => scoped(program).orDie().run() as Promise<A>;

const doubler = (connection: Connection<number>) =>
  connection.messages.forEach((n) => connection.send(n * 2));

/** Bob serves on a simulated network; `handled` lists the key of each connection his handler saw. */
const serveBob = (network: SimulatedNetwork, handled: string[]) =>
  AuthenticatedTransport.over(network.node("server"), {
    privateKey: bob.privateKey,
    trusted,
  }).listen(address, {
    codec,
    handle: (connection) => {
      handled.push(toHex(connection.peer ?? new Uint8Array()));
      return doubler(connection);
    },
  });

const clientOf = (network: SimulatedNetwork, identity = alice, keys = trusted, timeout = 2_000) =>
  AuthenticatedTransport.over(network.node("client"), {
    privateKey: identity.privateKey,
    trusted: keys,
    handshakeTimeoutMs: timeout,
  });

describe("AuthenticatedTransport", () => {
  test("both ends learn who the other is, and messages sent right behind the handshake arrive", async () => {
    const result = await run(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const handled: string[] = [];
        yield* serveBob(network, handled);
        const connection = yield* clientOf(network).connect(address, {
          codec,
          peer: bob.publicKey,
        });
        // No wait between the handshake and the messages: they can reach the
        // server in the same read as the client's last handshake message.
        for (const n of [1, 2, 3]) yield* connection.send(n);
        const replies = yield* connection.messages.take(3).toArray();
        return { replies, peer: toHex(connection.peer!), handled };
      }),
    );
    expect(result).toEqual({
      replies: [2, 4, 6],
      peer: toHex(bob.publicKey),
      handled: [toHex(alice.publicKey)],
    });
  });

  test("a client whose key the server doesn't trust is refused and never reaches the handler", async () => {
    const result = await run(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const handled: string[] = [];
        yield* serveBob(network, handled);
        const outcome = yield* clientOf(network, mallory, [...trusted, mallory.publicKey])
          .connect(address, { codec })
          .either();
        return { outcome: outcome._tag, handled };
      }),
    );
    expect(result).toEqual({ outcome: "Left", handled: [] });
  });

  test("a server whose key the client doesn't trust is refused", async () => {
    const result = await run(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        const handled: string[] = [];
        yield* serveBob(network, handled);
        const outcome = yield* clientOf(network, alice, [alice.publicKey])
          .connect(address, { codec })
          .either();
        return { outcome: outcome._tag, handled };
      }),
    );
    expect(result).toEqual({ outcome: "Left", handled: [] });
  });

  test("pinning the server's key refuses another trusted node answering for it", async () => {
    const result = await run(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        yield* serveBob(network, []);
        const client = clientOf(network);
        const wrong = yield* client.connect(address, { codec, peer: alice.publicKey }).either();
        const right = yield* client.connect(address, { codec, peer: bob.publicKey }).either();
        return {
          wrong: wrong._tag === "Left" ? wrong.left.message : "connected",
          right: right._tag,
        };
      }),
    );
    expect(result.wrong).toContain("not the expected peer");
    expect(result.right).toBe("Right");
  });

  test("a server that never answers is given up on after the handshake timeout", async () => {
    const result = await run(
      eff(function* () {
        const network = yield* SimulatedNetwork.make();
        yield* network.node("server").listen(address, {
          codec: BinaryCodec.bytes,
          handle: (connection) => connection.messages.forEach(() => eff(function* () {})),
        });
        const outcome = yield* clientOf(network, alice, trusted, 40)
          .connect(address, { codec })
          .either();
        return outcome._tag === "Left" ? outcome.left.message : "connected";
      }),
    );
    expect(result).toContain("timed out");
  });

  describe("against a hand-written client", () => {
    const hello = (publicKey: Uint8Array, nonce: Uint8Array) =>
      concatBytes(Uint8Array.of(1), publicKey, nonce);
    const context = new TextEncoder().encode("perfect-net/handshake/v1");
    const transcript = (params: {
      readonly signer: 1 | 2;
      readonly initiatorNonce: Uint8Array;
      readonly responderNonce: Uint8Array;
    }) =>
      concatBytes(
        context,
        Uint8Array.of(params.signer),
        params.initiatorNonce,
        params.responderNonce,
        alice.publicKey,
        bob.publicKey,
      );
    const nonce = (fill: number) => new Uint8Array(32).fill(fill);

    // Alice's side of a handshake, by hand. Returns the connection to the
    // server and the finish message, which `finish` can replace.
    const handshake = (params: {
      readonly network: SimulatedNetwork;
      readonly initiatorNonce: Uint8Array;
      readonly finish?: (genuine: Uint8Array) => Uint8Array;
    }) =>
      eff(function* () {
        const raw = yield* params.network
          .node("raw")
          .connect(address, { codec: BinaryCodec.bytes });
        yield* raw.send(hello(alice.publicKey, params.initiatorNonce));
        const [reply] = yield* raw.messages.take(1).toArray();
        const responderNonce = reply!.slice(33, 65);
        const genuine = concatBytes(
          Uint8Array.of(3),
          ed25519Signer(alice.privateKey).sign(
            transcript({
              signer: 1,
              initiatorNonce: params.initiatorNonce,
              responderNonce,
            }),
          ),
        );
        yield* raw.send(params.finish ? params.finish(genuine) : genuine);
        return { raw, genuine, reply: reply! };
      });
    const settle = () => new Promise((resolve) => setTimeout(resolve, 20));

    test("a hand-written handshake with the right signatures is accepted", async () => {
      const handled = await run(
        eff(function* () {
          const network = yield* SimulatedNetwork.make();
          const seen: string[] = [];
          yield* serveBob(network, seen);
          const { raw } = yield* handshake({ network, initiatorNonce: nonce(1) });
          yield* raw.send(new TextEncoder().encode("2"));
          const [answer] = yield* raw.messages.take(1).toArray();
          return { seen, answer: new TextDecoder().decode(answer) };
        }),
      );
      expect(handled).toEqual({ seen: [toHex(alice.publicKey)], answer: "4" });
    });

    test("knowing a trusted public key is not enough: a made-up signature is refused", async () => {
      const seen: string[] = [];
      const after = await run(
        eff(function* () {
          const network = yield* SimulatedNetwork.make();
          yield* serveBob(network, seen);
          const { raw } = yield* handshake({
            network,
            initiatorNonce: nonce(2),
            finish: () => concatBytes(Uint8Array.of(3), new Uint8Array(64)),
          });
          return yield* raw.messages.toArray(); // the server closes the connection
        }),
      );
      expect(after).toEqual([]);
      expect(seen).toEqual([]);
    });

    test("a recorded handshake can't be replayed in a new session", async () => {
      const seen: string[] = [];
      await run(
        eff(function* () {
          const network = yield* SimulatedNetwork.make();
          yield* serveBob(network, seen);
          const first = yield* handshake({ network, initiatorNonce: nonce(3) });
          yield* first.raw.close;
          // Same client nonce and the same finish message, new server nonce.
          const second = yield* handshake({
            network,
            initiatorNonce: nonce(3),
            finish: () => first.genuine,
          });
          yield* second.raw.messages.toArray();
        }),
      );
      await settle();
      expect(seen).toEqual([toHex(alice.publicKey)]);
    });

    test("garbage and a stranger's key are closed without reaching the handler", async () => {
      const seen: string[] = [];
      const closed = await run(
        eff(function* () {
          const network = yield* SimulatedNetwork.make();
          yield* serveBob(network, seen);
          const garbage = yield* network.node("raw").connect(address, { codec: BinaryCodec.bytes });
          yield* garbage.send(new Uint8Array([9, 9, 9]));
          const stranger = yield* network
            .node("raw")
            .connect(address, { codec: BinaryCodec.bytes });
          yield* stranger.send(hello(mallory.publicKey, nonce(4)));
          return [yield* garbage.messages.toArray(), yield* stranger.messages.toArray()];
        }),
      );
      expect(closed).toEqual([[], []]);
      expect(seen).toEqual([]);
    });
  });

  test("works over real sockets", async () => {
    const result = await run(
      eff(function* () {
        const handled: string[] = [];
        const listener = yield* AuthenticatedTransport.over(Tcp, {
          privateKey: bob.privateKey,
          trusted,
        }).listen(
          { host: "127.0.0.1", port: 0 },
          {
            codec,
            handle: (connection) => {
              handled.push(toHex(connection.peer!));
              return doubler(connection);
            },
          },
        );
        const connection = yield* AuthenticatedTransport.over(Tcp, {
          privateKey: alice.privateKey,
          trusted,
        }).connect(listener.address, { codec, peer: bob.publicKey });
        yield* connection.send(21);
        const [answer] = yield* connection.messages.take(1).toArray();
        return { answer, peer: toHex(connection.peer!), handled };
      }),
    );
    expect(result).toEqual({
      answer: 42,
      peer: toHex(bob.publicKey),
      handled: [toHex(alice.publicKey)],
    });
  });

  test("bad keys and timeouts are refused when the transport is built", async () => {
    const network = await SimulatedNetwork.make().run();
    const node = network.node("a");
    expect(() =>
      AuthenticatedTransport.over(node, { privateKey: new Uint8Array(5), trusted }),
    ).toThrow(RangeError);
    expect(() =>
      AuthenticatedTransport.over(node, {
        privateKey: alice.privateKey,
        trusted: [new Uint8Array(3)],
      }),
    ).toThrow(RangeError);
    expect(() =>
      AuthenticatedTransport.over(node, {
        privateKey: alice.privateKey,
        trusted,
        handshakeTimeoutMs: 0,
      }),
    ).toThrow(RangeError);
  });
});
