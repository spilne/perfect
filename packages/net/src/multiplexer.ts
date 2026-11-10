// Many logical channels over one connection. Each channel is itself a
// Connection (send, messages, close), so it works anywhere a connection does,
// and channels don't hold each other up.
//
//   const mux = yield* Multiplexer.make(connection, { side: "client", codec });
//   const orders = yield* mux.open;        // a new channel
//   mux.incoming                           // channels the other side opens
//
// On the wire every frame is one message on the shared connection:
// [kind: 1 byte][channel id: 4 bytes][payload]. OPEN carries the opener's
// window, and the side that accepts a channel answers with a CREDIT frame
// for its own window, so each sender knows how much the reader will queue
// even when the two sides are configured differently. The side that connected
// numbers its channels 1, 3, 5... and the other side 2, 4, 6..., so ids never
// collide. Each channel has a window: a sender may have at most `window`
// messages the reader hasn't taken yet, and the reader grants more as it
// takes them. A slow reader therefore only slows its own channel.
//
// The other side is not trusted. What it controls is bounded: at most
// `maxChannels` open channels, at most `maxPendingIncoming` channels nobody
// has picked up from `incoming`, and at most `window` unread messages per
// channel. A channel past a limit is refused or failed on its own; a peer
// that breaks the framing (a wrong or repeated channel id, an unknown
// frame kind, a short frame) stops the whole multiplexer with a MessageError.

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
import { ChannelLimitExceeded, ConnectionError, MessageError, type NetError } from "./errors.js";
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

const MAX_CHANNEL_ID = 0xffff_ffff;

function creditFrame(channel: number, count: number): Uint8Array {
  const payload = new Uint8Array(4);
  new DataView(payload.buffer).setUint32(0, count);
  return frame(CREDIT, channel, payload);
}

/** Options for `Multiplexer.make`. */
export interface MultiplexerOptions<M> {
  /** "client" if this side made the connection, "server" if it accepted it. */
  readonly side: "client" | "server";
  /** How messages on every channel become bytes. */
  readonly codec: BinaryCodec<M>;
  /** Messages a sender may have in flight per channel. Default: 256. */
  readonly window?: number;
  /** Channels that may be open at once, whoever opened them. Default: 1024. */
  readonly maxChannels?: number;
  /**
   * Channels the other side opened that nobody has taken from `incoming`
   * yet. More are refused. Default: 128.
   */
  readonly maxPendingIncoming?: number;
}

/** What a multiplexer is holding, for monitoring and tests. */
export interface MultiplexerStats {
  /** Channels neither side has closed. */
  readonly openChannels: number;
  /** Channels waiting in `incoming`. */
  readonly pendingIncoming: number;
  /** Channels the other side opened that were refused because of a limit. */
  readonly rejectedChannels: number;
}

/** Channels over one connection, from `Multiplexer.make`. */
export interface Multiplexer<M> {
  /**
   * Open a new channel. The other side sees it in `incoming`. Fails with
   * ChannelLimitExceeded at `maxChannels`.
   */
  readonly open: Eff<Connection<M>, Throws<NetError>>;
  /** Channels the other side opens, as they arrive. Read it from one place. */
  readonly incoming: Stream<Connection<M>, Throws<NetError>>;
  readonly stats: Eff<MultiplexerStats, never>;
}

/** One channel's state on this side. */
class Channel<M> {
  readonly id: number;
  readonly inbox: Inbox<M, NetError>;
  /** Messages we may still send before the reader grants more. */
  credit: number;
  /** Senders waiting for credit; woken together, each checks again. */
  creditWaiters = new Set<() => void>();
  closedHere = false;
  closedThere = false;

  constructor(params: { id: number; credit: number }) {
    this.id = params.id;
    this.credit = params.credit;
    this.inbox = new Inbox<M, NetError>();
  }
}

/** Many independent channels over one connection, each with its own flow control. */
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
    const maxChannels = options.maxChannels ?? 1024;
    const maxPendingIncoming = options.maxPendingIncoming ?? 128;
    for (const [name, value] of Object.entries({ window, maxChannels, maxPendingIncoming })) {
      if (!Number.isInteger(value) || value < 1) {
        throw new RangeError(`${name} must be a positive integer, got ${value}`);
      }
    }
    const channels = new Map<number, Channel<M>>();
    const incoming = new Inbox<Connection<M>, NetError>();
    let nextId = options.side === "client" ? 1 : 2;
    // The other side numbers its channels with the other parity, upwards.
    const peerParity = options.side === "client" ? 0 : 1;
    let lastPeerId = 0;
    let rejectedChannels = 0;
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
      const messages = Stream.unfoldChunkEffect(undefined, () =>
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
      );

      const close: Eff<void, never> = suspend(() => {
        if (channel.closedHere) return succeed(undefined);
        channel.closedHere = true;
        // Frames still on their way for it are dropped as for any unknown id.
        channels.delete(channel.id);
        return connection.send(frame(CLOSE, channel.id)).catch(() => succeed(undefined));
      });

      return {
        remote: `${remote}#${channel.id}`,
        send,
        messages: messages,
        close,
      };
    };

    const ignore = succeed(undefined);
    const windowBytes = new Uint8Array(4);
    new DataView(windowBytes.buffer).setUint32(0, window);

    /** The other side broke the protocol: stop everything and hang up. */
    const violation = (message: string): Eff<void, never> => {
      stop(new MessageError({ address: remote, message: `protocol violation: ${message}` }));
      return connection.close;
    };

    /** End one channel with an error, and tell the other side. */
    const failChannel = (channel: Channel<M>, error: MessageError): Eff<void, never> => {
      channel.inbox.fail(error);
      channel.closedThere = true;
      channel.closedHere = true;
      channels.delete(channel.id);
      wakeSenders(channel);
      return connection.send(frame(CLOSE, channel.id)).catch(() => ignore);
    };

    const onFrame = (bytes: Uint8Array): Eff<void, never> => {
      if (stopped) return ignore;
      if (bytes.length < HEADER) return violation(`a frame of ${bytes.length} bytes is too short`);
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const kind = view.getUint8(0);
      const id = view.getUint32(1);
      const payload = bytes.subarray(HEADER);
      if (kind === OPEN) {
        if (id % 2 !== peerParity || id <= lastPeerId) {
          return violation(`channel ${id} can't be opened by the other side now`);
        }
        lastPeerId = id;
        if (payload.length !== 4) return violation(`an OPEN frame of ${payload.length} bytes`);
        const peerWindow = new DataView(payload.buffer, payload.byteOffset, 4).getUint32(0);
        if (channels.size >= maxChannels || incoming.size >= maxPendingIncoming) {
          rejectedChannels += 1;
          return connection.send(frame(CLOSE, id)).catch(() => ignore);
        }
        const channel = new Channel<M>({ id, credit: peerWindow });
        channels.set(id, channel);
        incoming.push(asConnection(channel));
        return connection.send(creditFrame(id, window)).catch(() => ignore);
      }
      if (kind !== DATA && kind !== CREDIT && kind !== CLOSE) {
        return violation(`unknown frame kind ${kind}`);
      }
      if (kind === CREDIT && payload.length !== 4) {
        return violation(`a credit frame of ${payload.length} bytes instead of 4`);
      }
      const channel = channels.get(id);
      if (!channel) return ignore;
      if (kind === DATA) {
        if (channel.inbox.size >= window) {
          return failChannel(
            channel,
            new MessageError({
              address: remote,
              message: `channel ${id} got more than its window of ${window} unread messages`,
            }),
          );
        }
        let message: M;
        try {
          message = codec.decode(payload);
        } catch (error) {
          return failChannel(
            channel,
            new MessageError({
              address: remote,
              message: `couldn't decode: ${String(error)}`,
              cause: error,
            }),
          );
        }
        channel.inbox.push(message);
      } else if (kind === CREDIT) {
        channel.credit += new DataView(payload.buffer, payload.byteOffset, 4).getUint32(0);
        wakeSenders(channel);
      } else {
        channel.closedThere = true;
        channel.inbox.end();
        // A sender waiting for credit won't get any now.
        wakeSenders(channel);
        channels.delete(id);
      }
      return ignore;
    };

    const stop = (error: NetError) => {
      if (stopped) return;
      stopped = error;
      incoming.end();
      for (const channel of channels.values()) {
        channel.inbox.end();
        wakeSenders(channel);
      }
      channels.clear();
    };

    // Read the shared connection and hand each frame to its channel.
    const reader = connection.messages
      .forEach(onFrame)
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
      if (channels.size >= maxChannels) {
        return fail(
          new ChannelLimitExceeded({
            address: remote,
            message: `already ${channels.size} open channels (the limit is ${maxChannels})`,
          }),
        );
      }
      if (nextId > MAX_CHANNEL_ID) {
        return fail(
          new ChannelLimitExceeded({
            address: remote,
            message: "this connection has used up its channel ids",
          }),
        );
      }
      // It may send nothing until the other side answers with its window.
      const channel = new Channel<M>({ id: nextId, credit: 0 });
      nextId += 2;
      channels.set(channel.id, channel);
      return connection
        .send(frame(OPEN, channel.id, windowBytes))
        .catch((error) => {
          channels.delete(channel.id);
          return fail(error);
        })
        .map(() => asConnection(channel));
    });

    const incomingStream = Stream.unfoldChunkEffect(undefined, () =>
      incoming.take.map((batch) => (batch === undefined ? null : ([batch, undefined] as const))),
    );

    return acquireRelease(
      forkDaemon(reader).map((fiber) => ({ fiber })),
      ({ fiber }) =>
        sync(() => {
          stop(broken("the multiplexer stopped"));
          fiber.interrupt();
        }).flatMap(() => awaitFiber(fiber)),
    ).map(() => ({
      open,
      incoming: incomingStream,
      stats: sync(() => ({
        openChannels: channels.size,
        pendingIncoming: incoming.size,
        rejectedChannels,
      })),
    }));
  },
} as const;
