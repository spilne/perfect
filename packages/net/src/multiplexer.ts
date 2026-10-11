// Many logical channels over one connection. Each channel is itself a
// Connection (send, messages, close), so it works anywhere a connection does,
// and channels don't hold each other up.
//
//   const mux = yield* Multiplexer.make(connection, { side: "client", codec });
//   const orders = yield* mux.open;        // a new channel
//   mux.incoming                           // channels the other side opens
//
// On the wire every frame is one message on the shared connection:
// [kind: 1 byte][channel id: 4 bytes][payload]. The side that connected
// numbers its channels 1, 3, 5... and the other side 2, 4, 6..., so ids never
// collide. Each channel has a window: a sender may have at most `window`
// messages the reader hasn't taken yet, and the reader grants more as it
// takes them. A slow reader therefore only slows its own channel.

import {
  acquireRelease,
  async,
  awaitFiber,
  fail,
  forkDaemon,
  succeed,
  suspend,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import type { BinaryCodec } from "./codec.js";
import type { Connection } from "./connection.js";
import { ConnectionError, MessageError, type NetError } from "./errors.js";
import { Inbox } from "./inbox.js";

const OPEN = 1;
const DATA = 2;
const CREDIT = 3;
const CLOSE = 4;

const HEADER = 5;

function wakeSenders(channel: { creditWaiters: Set<() => void> }): void {
  const waiters = [...channel.creditWaiters];
  channel.creditWaiters.clear();
  for (const wake of waiters) wake();
}

function frame(kind: number, channel: number, payload?: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(HEADER + (payload?.length ?? 0));
  const view = new DataView(bytes.buffer);
  view.setUint8(0, kind);
  view.setUint32(1, channel);
  if (payload) bytes.set(payload, HEADER);
  return bytes;
}

function creditFrame(channel: number, count: number): Uint8Array {
  const payload = new Uint8Array(4);
  new DataView(payload.buffer).setUint32(0, count);
  return frame(CREDIT, channel, payload);
}

export interface MultiplexerOptions<M> {
  /** "client" if this side made the connection, "server" if it accepted it. */
  readonly side: "client" | "server";
  /** How messages on every channel become bytes. */
  readonly codec: BinaryCodec<M>;
  /** Messages a sender may have in flight per channel. Default: 256. */
  readonly window?: number;
}

export interface Multiplexer<M> {
  /** Open a new channel. The other side sees it in `incoming`. */
  readonly open: Eff<Connection<M>, Throws<NetError>>;
  /** Channels the other side opens, as they arrive. Read it from one place. */
  readonly incoming: Stream<Connection<M>, Throws<NetError>>;
}

/** One channel's state on this side. */
class Channel<M> {
  readonly inbox: Inbox<M, NetError>;
  /** Messages we may still send before the reader grants more. */
  credit: number;
  /** Senders waiting for credit; woken together, each checks again. */
  creditWaiters = new Set<() => void>();
  closedHere = false;
  closedThere = false;

  constructor(
    readonly id: number,
    window: number,
  ) {
    this.credit = window;
    this.inbox = new Inbox<M, NetError>(
      Infinity,
      () => {},
      () => {},
    );
  }
}

export const Multiplexer = {
  /**
   * Start multiplexing over `connection`, which must carry raw bytes
   * (opened with `BinaryCodec.bytes`). It stops, and every channel ends,
   * when the surrounding scope ends or the connection closes.
   */
  make<M>(
    connection: Connection<Uint8Array>,
    options: MultiplexerOptions<M>,
  ): Eff<Multiplexer<M>, never> {
    const { codec } = options;
    const window = options.window ?? 256;
    const channels = new Map<number, Channel<M>>();
    const incoming = new Inbox<Connection<M>, NetError>(
      Infinity,
      () => {},
      () => {},
    );
    let nextId = options.side === "client" ? 1 : 2;
    let stopped: NetError | undefined;

    const remote = connection.remote;
    const broken = (message: string) => new ConnectionError({ address: remote, message });

    const asConnection = (channel: Channel<M>): Connection<M> => {
      const send = (message: M): Eff<void, Throws<NetError>> =>
        suspend(() => {
          if (stopped) return fail(stopped);
          if (channel.closedHere || channel.closedThere) {
            return fail(broken(`channel ${channel.id} is closed`));
          }
          const sendNow = () => {
            channel.credit -= 1;
            let payload: Uint8Array;
            try {
              payload = codec.encode(message);
            } catch (error) {
              return fail(
                new MessageError({ address: remote, message: String(error), cause: error }),
              );
            }
            return connection.send(frame(DATA, channel.id, payload));
          };
          if (channel.credit > 0) return sendNow();
          // The reader is behind: wait until it grants more.
          return async<void, never>((resume) => {
            const wake = () => resume(succeed(undefined));
            channel.creditWaiters.add(wake);
            return () => channel.creditWaiters.delete(wake);
          }).flatMap(() => send(message));
        });

      // Taking messages grants the sender as many again.
      const messages = Stream.unfoldEffect(undefined, () =>
        channel.inbox.take.flatMap((batch) => {
          if (batch === undefined) return succeed(null);
          const grant =
            channel.closedThere || stopped
              ? succeed(undefined)
              : connection
                  .send(creditFrame(channel.id, batch.length))
                  .catch(() => succeed(undefined));
          return grant.map(() => [batch, undefined] as const);
        }),
      ).flatMap((batch) => Stream.fromArray(batch as M[]));

      const close: Eff<void, never> = suspend(() => {
        if (channel.closedHere) return succeed(undefined);
        channel.closedHere = true;
        if (channel.closedThere) channels.delete(channel.id);
        return connection.send(frame(CLOSE, channel.id)).catch(() => succeed(undefined));
      });

      return {
        remote: `${remote}#${channel.id}`,
        send,
        messages: messages as Stream<M, Throws<NetError>>,
        close,
      };
    };

    const onFrame = (bytes: Uint8Array): void => {
      if (bytes.length < HEADER) return;
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const kind = view.getUint8(0);
      const id = view.getUint32(1);
      const payload = bytes.subarray(HEADER);
      if (kind === OPEN) {
        const channel = new Channel<M>(id, window);
        channels.set(id, channel);
        incoming.push(asConnection(channel));
        return;
      }
      const channel = channels.get(id);
      if (!channel) return;
      if (kind === DATA) {
        let message: M;
        try {
          message = codec.decode(payload);
        } catch (error) {
          channel.inbox.fail(
            new MessageError({
              address: remote,
              message: `couldn't decode: ${String(error)}`,
              cause: error,
            }),
          );
          return;
        }
        channel.inbox.push(message);
      } else if (kind === CREDIT) {
        channel.credit += new DataView(payload.buffer, payload.byteOffset, 4).getUint32(0);
        wakeSenders(channel);
      } else if (kind === CLOSE) {
        channel.closedThere = true;
        channel.inbox.end();
        // A sender waiting for credit won't get any now.
        wakeSenders(channel);
        if (channel.closedHere) channels.delete(id);
      }
    };

    const stop = (error: NetError) => {
      if (stopped) return;
      stopped = error;
      incoming.end();
      for (const channel of channels.values()) {
        channel.inbox.end();
        wakeSenders(channel);
      }
    };

    // Read the shared connection and hand each frame to its channel.
    const reader = connection.messages
      .forEach((bytes) => sync(() => onFrame(bytes)))
      .exit()
      .flatMap((exit) =>
        sync(() =>
          stop(
            exit._tag === "Failure"
              ? broken("the connection broke")
              : broken("the connection closed"),
          ),
        ),
      );

    const open: Eff<Connection<M>, Throws<NetError>> = suspend(() => {
      if (stopped) return fail(stopped);
      const channel = new Channel<M>(nextId, window);
      nextId += 2;
      channels.set(channel.id, channel);
      return connection.send(frame(OPEN, channel.id)).map(() => asConnection(channel));
    });

    const incomingStream = Stream.unfoldEffect(undefined, () =>
      incoming.take.map((batch) => (batch === undefined ? null : ([batch, undefined] as const))),
    ).flatMap((batch) => Stream.fromArray(batch));

    return acquireRelease(
      forkDaemon(reader).map((fiber) => ({ fiber })),
      ({ fiber }) =>
        sync(() => {
          stop(broken("the multiplexer stopped"));
          fiber.interrupt();
        }).flatMap(() => awaitFiber(fiber)),
    ).map(() => ({
      open,
      incoming: incomingStream as Stream<Connection<M>, Throws<NetError>>,
    }));
  },
} as const;
