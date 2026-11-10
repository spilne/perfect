// A Transport that knows who it is talking to. `AuthenticatedTransport.over`
// wraps another Transport (TCP, TLS or the simulator): before a connection
// reaches your code, both ends prove they hold the private key of a public key
// in the `trusted` list, and the connection carries that key as `peer`.
//
//   const secure = AuthenticatedTransport.over(Tcp, { privateKey, trusted });
//   yield* secure.listen(address, { codec, handle: (c) => ... c.peer ... });
//   yield* secure.connect(address, { codec, peer: expectedPublicKey });
//
// The handshake (three messages, before any of yours):
//   1. the client sends its public key and a fresh random nonce;
//   2. the server answers with its own key, its own fresh nonce, and a
//      signature over both nonces and both keys;
//   3. the client checks that, and sends its signature over the same data.
// Each signature covers both nonces, both keys and the signer's role, so a
// recorded handshake can't be replayed to someone else, a signature can't be
// reflected back at its maker, and knowing a member's public key is not enough
// to pass as that member. A side that sees a key outside `trusted` stops before
// signing anything.
//
// What it does not do:
// - It proves who is on the other end when the connection opens. After that
//   the messages are not signed or encrypted, so someone who can alter the
//   byte stream (a man in the middle) can inject or change them. Run it over
//   TLS for that, and sign anything that must hold up on its own (a vote, a
//   block) with the member's key.
// - `trusted` is fixed for the life of the transport. To change it, build a
//   new one.

import { randomBytes } from "node:crypto";
import {
  Cause,
  Deferred,
  async,
  fail,
  forkDaemon,
  succeed,
  suspend,
  sync,
  timeoutFail,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import { BinaryCodec } from "./codec.js";
import type { Connection, ConnectionOptions } from "./connection.js";
import {
  ChannelLimitExceeded,
  ConnectError,
  ConnectionError,
  ListenError,
  MessageError,
  type NetError,
} from "./errors.js";
import {
  ED25519_KEY_BYTES,
  ED25519_SIGNATURE_BYTES,
  concatBytes,
  ed25519Keyring,
  ed25519Signer,
} from "./ed25519.js";
import { Inbox } from "./inbox.js";
import type { Address, ConnectOptions, Listener, ListenOptions, Transport } from "./transport.js";

const HELLO = 1;
const REPLY = 2;
const FINISH = 3;
const NONCE_BYTES = 32;
const INITIATOR = 1;
const RESPONDER = 2;
const CONTEXT = new TextEncoder().encode("perfect-net/handshake/v1");
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 5_000;

const HELLO_BYTES = 1 + ED25519_KEY_BYTES + NONCE_BYTES;
const REPLY_BYTES = 1 + ED25519_KEY_BYTES + NONCE_BYTES + ED25519_SIGNATURE_BYTES;
const FINISH_BYTES = 1 + ED25519_SIGNATURE_BYTES;

/** What the wrapper needs to know about this node and the cluster. */
export interface AuthenticatedTransportOptions {
  /** This node's 32-byte Ed25519 private seed (see `generateEd25519Identity`). */
  readonly privateKey: Uint8Array;
  /** The 32-byte public keys of every node allowed to connect or be connected to. */
  readonly trusted: ReadonlyArray<Uint8Array>;
  /** Give up on a handshake that takes longer than this many ms. Default: 5 000. */
  readonly handshakeTimeoutMs?: number;
}

/** Options for `connect` on an `AuthenticatedTransport`. */
export interface AuthenticatedConnectOptions<M> extends ConnectOptions<M> {
  /**
   * The public key the server must prove it holds. Without it any trusted key
   * is accepted, so a trusted node could answer for another one.
   */
  readonly peer?: Uint8Array;
}

/** A Transport whose connections carry the verified key of the other end as `peer`. */
export interface AuthenticatedTransport extends Transport {
  /** This node's public key. */
  readonly publicKey: Uint8Array;
  connect<M>(
    address: Address,
    options: AuthenticatedConnectOptions<M>,
  ): Eff<Connection<M>, Throws<ConnectError>>;
}

/** What the state machine decided about a handshake frame. */
type Step =
  | { readonly kind: "continue"; readonly send?: Uint8Array }
  | { readonly kind: "done"; readonly peer: Uint8Array; readonly send?: Uint8Array }
  | { readonly kind: "rejected"; readonly reason: string };

const rejected = (reason: string): Step => ({ kind: "rejected", reason });

function secure<M>(params: {
  readonly inner: Connection<Uint8Array>;
  readonly role: "initiator" | "responder";
  readonly privateKey: Uint8Array;
  readonly trusted: ReadonlyArray<Uint8Array>;
  readonly codec: ConnectionOptions<M>["codec"];
  readonly highWaterMark: number;
  readonly expectPeer: Uint8Array | undefined;
  readonly handshakeTimeoutMs: number;
}): Eff<Connection<M>, Throws<ConnectError>> {
  const { inner, role, codec, expectPeer } = params;
  const address = inner.remote;
  const signer = ed25519Signer(params.privateKey);
  const keyring = ed25519Keyring(params.trusted);
  const myNonce = new Uint8Array(randomBytes(NONCE_BYTES));

  const transcript = (parts: {
    readonly signer: typeof INITIATOR | typeof RESPONDER;
    readonly initiatorNonce: Uint8Array;
    readonly responderNonce: Uint8Array;
    readonly initiatorKey: Uint8Array;
    readonly responderKey: Uint8Array;
  }) =>
    concatBytes(
      CONTEXT,
      Uint8Array.of(parts.signer),
      parts.initiatorNonce,
      parts.responderNonce,
      parts.initiatorKey,
      parts.responderKey,
    );
  const acceptable = (key: Uint8Array): string | undefined =>
    keyring.has(key) ? undefined : "the other end's key is not trusted";

  type Phase = "awaitReply" | "awaitHello" | "awaitFinish" | "open";
  let phase: Phase = role === "initiator" ? "awaitReply" : "awaitHello";
  let theirKey: Uint8Array = new Uint8Array(0);
  let theirNonce: Uint8Array = new Uint8Array(0);

  const advance = (frame: Uint8Array): Step => {
    if (phase === "awaitHello") {
      if (frame[0] !== HELLO || frame.length !== HELLO_BYTES) return rejected("expected a hello");
      theirKey = frame.slice(1, 1 + ED25519_KEY_BYTES);
      theirNonce = frame.slice(1 + ED25519_KEY_BYTES);
      const problem = acceptable(theirKey);
      if (problem) return rejected(problem);
      const signature = signer.sign(
        transcript({
          signer: RESPONDER,
          initiatorNonce: theirNonce,
          responderNonce: myNonce,
          initiatorKey: theirKey,
          responderKey: signer.publicKey,
        }),
      );
      phase = "awaitFinish";
      return {
        kind: "continue",
        send: concatBytes(Uint8Array.of(REPLY), signer.publicKey, myNonce, signature),
      };
    }
    if (phase === "awaitReply") {
      if (frame[0] !== REPLY || frame.length !== REPLY_BYTES) return rejected("expected a reply");
      theirKey = frame.slice(1, 1 + ED25519_KEY_BYTES);
      theirNonce = frame.slice(1 + ED25519_KEY_BYTES, 1 + ED25519_KEY_BYTES + NONCE_BYTES);
      const signature = frame.subarray(1 + ED25519_KEY_BYTES + NONCE_BYTES);
      const problem = acceptable(theirKey);
      if (problem) return rejected(problem);
      if (expectPeer && !sameBytes(expectPeer, theirKey)) {
        return rejected("the server is not the expected peer");
      }
      const proof = keyring.verify({
        publicKey: theirKey,
        message: transcript({
          signer: RESPONDER,
          initiatorNonce: myNonce,
          responderNonce: theirNonce,
          initiatorKey: signer.publicKey,
          responderKey: theirKey,
        }),
        signature,
      });
      if (!proof) return rejected("the server's signature is wrong");
      phase = "open";
      const mine = signer.sign(
        transcript({
          signer: INITIATOR,
          initiatorNonce: myNonce,
          responderNonce: theirNonce,
          initiatorKey: signer.publicKey,
          responderKey: theirKey,
        }),
      );
      return { kind: "done", peer: theirKey, send: concatBytes(Uint8Array.of(FINISH), mine) };
    }
    if (phase === "awaitFinish") {
      if (frame[0] !== FINISH || frame.length !== FINISH_BYTES)
        return rejected("expected a finish");
      const proof = keyring.verify({
        publicKey: theirKey,
        message: transcript({
          signer: INITIATOR,
          initiatorNonce: theirNonce,
          responderNonce: myNonce,
          initiatorKey: theirKey,
          responderKey: signer.publicKey,
        }),
        signature: frame.subarray(1),
      });
      if (!proof) return rejected("the client's signature is wrong");
      phase = "open";
      return { kind: "done", peer: theirKey };
    }
    return rejected("unexpected handshake frame");
  };

  return Deferred.make<Uint8Array, string>().flatMap((settled) => {
    // Messages that arrive after the handshake wait here for the reader. When
    // too many wait, the pump stops taking frames, which fills the inner
    // connection's own inbox and pauses the socket.
    let paused = false;
    let wake: (() => void) | undefined;
    const inbox = new Inbox<M, NetError>({
      highWaterMark: params.highWaterMark,
      pause: () => {
        paused = true;
      },
      resume: () => {
        paused = false;
        wake?.();
      },
    });
    const unpaused = async<void, never>((resume) => {
      if (!paused) return resume(succeed(undefined));
      wake = () => resume(succeed(undefined));
      return () => {
        wake = undefined;
      };
    });
    const unreadable = (message: string, cause?: unknown) =>
      new MessageError({ address, message, cause });

    const onFrame = (frame: Uint8Array): Eff<void, Throws<NetError | ConnectError>> =>
      suspend<void, Throws<NetError | ConnectError>>(() => {
        if (phase !== "open") {
          const step = advance(frame);
          if (step.kind === "rejected") {
            return settled
              .fail(step.reason)
              .flatMap(() => fail(new ConnectError({ address, message: step.reason })));
          }
          const send = step.send ? inner.send(step.send) : succeed(undefined);
          return step.kind === "done"
            ? send.flatMap(() => settled.succeed(step.peer)).map(() => undefined)
            : send;
        }
        let message: M;
        try {
          message = codec.decode(frame);
        } catch (error) {
          const problem = unreadable(`a message couldn't be decoded: ${String(error)}`, error);
          inbox.fail(problem);
          return fail(problem);
        }
        inbox.push(message);
        return paused ? unpaused : succeed(undefined);
      });

    const pump = inner.messages
      .forEach(onFrame)
      .exit()
      .flatMap((exit) =>
        sync(() => {
          if (exit._tag === "Failure") {
            const broken = Cause.failures(exit.cause).find(
              (e) =>
                e instanceof ConnectionError ||
                e instanceof MessageError ||
                e instanceof ChannelLimitExceeded,
            );
            if (broken) inbox.fail(broken);
          }
          inbox.end();
        })
          .flatMap(() => settled.fail("the connection closed during the handshake"))
          .flatMap(() => inner.close),
      );

    return forkDaemon(pump).flatMap((pumpFiber) => {
      const hello =
        role === "initiator"
          ? inner.send(concatBytes(Uint8Array.of(HELLO), signer.publicKey, myNonce))
          : succeed(undefined);
      const abandon = (reason: string) =>
        sync(() => pumpFiber.interrupt())
          .flatMap(() => inner.close)
          .flatMap(() =>
            fail(new ConnectError({ address, message: `peer authentication failed: ${reason}` })),
          );
      return hello.either().flatMap((sent) =>
        sent._tag === "Left"
          ? abandon(sent.left.message)
          : timeoutFail(settled.await, params.handshakeTimeoutMs, () => "the handshake timed out")
              .either()
              .flatMap((outcome) =>
                outcome._tag === "Left" ? abandon(outcome.left) : succeed(view(outcome.right)),
              ),
      );
    });

    function view(peer: Uint8Array): Connection<M> {
      return {
        remote: inner.remote,
        peer,
        send: (message) =>
          suspend(() => {
            let payload: Uint8Array;
            try {
              payload = codec.encode(message);
            } catch (error) {
              return fail(unreadable(`couldn't encode: ${String(error)}`, error));
            }
            return inner.send(payload);
          }),
        messages: Stream.unfoldChunkEffect(undefined, () =>
          inbox.take.map((batch) => (batch === undefined ? null : ([batch, undefined] as const))),
        ),
        close: inner.close,
      };
    }
  });
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, i) => byte === b[i]);
}

export const AuthenticatedTransport = {
  /** Wrap `transport` so every connection is authenticated. */
  over(transport: Transport, options: AuthenticatedTransportOptions): AuthenticatedTransport {
    const handshakeTimeoutMs = options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS;
    if (!(handshakeTimeoutMs > 0)) {
      throw new RangeError(`handshakeTimeoutMs must be positive, got ${handshakeTimeoutMs}`);
    }
    // Fails here, not on the first connection, for a bad key.
    const publicKey = ed25519Signer(options.privateKey).publicKey;
    ed25519Keyring(options.trusted);
    const shared = {
      privateKey: options.privateKey,
      trusted: options.trusted,
      handshakeTimeoutMs,
    };
    return {
      publicKey,
      connect: <M>(address: Address, connectOptions: AuthenticatedConnectOptions<M>) => {
        const { peer, codec, ...rest } = connectOptions;
        return transport.connect(address, { ...rest, codec: BinaryCodec.bytes }).flatMap((inner) =>
          secure({
            ...shared,
            inner,
            role: "initiator",
            codec,
            highWaterMark: connectOptions.highWaterMark ?? 1024,
            expectPeer: peer,
          }),
        );
      },
      listen: <M, S>(
        address: Address,
        listenOptions: ListenOptions<M, S>,
      ): Eff<Listener, Throws<ListenError> | Exclude<S, Throws<unknown>>> =>
        transport.listen(address, {
          ...listenOptions,
          codec: BinaryCodec.bytes,
          handle: (inner) =>
            secure({
              ...shared,
              inner,
              role: "responder",
              codec: listenOptions.codec,
              highWaterMark: listenOptions.highWaterMark ?? 1024,
              expectPeer: undefined,
            }).flatMap((connection) => listenOptions.handle(connection)),
        }) as Eff<Listener, Throws<ListenError> | Exclude<S, Throws<unknown>>>,
    };
  },
} as const;
