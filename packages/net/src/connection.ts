// A connection carrying typed messages over a byte stream: a TCP or Unix
// socket, a TLS socket, or a simulated link. All of them look like a Node
// Duplex stream, so they share this one implementation.

import type { Duplex } from "node:stream";
import { Stream } from "@spilne/perfect-core/stream";
import { async, fail, succeed, suspend, type Eff, type Throws } from "@spilne/perfect-core";
import type { BinaryCodec } from "./codec.js";
import { ConnectionError, MessageError, type NetError } from "./errors.js";
import { encodeFrame, FrameReader, FrameTooBig, DEFAULT_MAX_FRAME_BYTES } from "./frames.js";
import { Inbox } from "./inbox.js";

/** An open connection that sends and receives messages of type `M`. */
export interface Connection<M> {
  /** The other end, like "10.0.0.5:4000" or "/tmp/app.sock". */
  readonly remote: string;
  /**
   * Send one message. Waits when the socket's buffer is full, so a fast
   * sender can't run far ahead of the network.
   */
  send(message: M): Eff<void, Throws<NetError>>;
  /**
   * The messages the other end sends, until it closes the connection. It
   * fails if the connection breaks or a message can't be decoded. Read it
   * from one place: messages taken by one reader are gone for the others.
   */
  readonly messages: Stream<M, Throws<NetError>>;
  /** Close the connection after what was sent has gone out. */
  readonly close: Eff<void, never>;
}

/** Options shared by everything that opens connections. */
/** How a connection reads and writes messages. */
export interface ConnectionOptions<M> {
  /** How messages become bytes and back. */
  readonly codec: BinaryCodec<M>;
  /** The largest message accepted, in bytes. Default: 16 MiB. */
  readonly maxMessageBytes?: number;
  /** Stop reading from the socket once this many messages wait unread. Default: 1024. */
  readonly highWaterMark?: number;
}

/** Wrap an open byte stream (a connected socket) as a Connection. */
export function connectionOver<M>(
  socket: Duplex,
  remote: string,
  options: ConnectionOptions<M>,
): Connection<M> {
  const { codec } = options;
  const reader = new FrameReader(options.maxMessageBytes ?? DEFAULT_MAX_FRAME_BYTES);
  const inbox = new Inbox<M, NetError>(
    options.highWaterMark ?? 1024,
    () => socket.pause(),
    () => socket.resume(),
  );
  let closed = false;

  const broken = (message: string, cause?: unknown) =>
    new ConnectionError({ address: remote, message, cause });
  const unreadable = (message: string, cause?: unknown) => {
    inbox.fail(new MessageError({ address: remote, message, cause }));
    socket.destroy();
  };

  socket.on("data", (chunk: Uint8Array) => {
    let frames: Uint8Array[];
    try {
      frames = reader.push(chunk);
    } catch (error) {
      if (error instanceof FrameTooBig) return unreadable(error.message, error);
      throw error;
    }
    for (const frame of frames) {
      let message: M;
      try {
        message = codec.decode(frame);
      } catch (error) {
        return unreadable(`a message couldn't be decoded: ${String(error)}`, error);
      }
      inbox.push(message);
    }
  });
  socket.on("end", () => inbox.end());
  socket.on("error", (error) => inbox.fail(broken(error.message, error)));
  socket.on("close", () => {
    closed = true;
    inbox.end();
  });

  let corked = false;
  const uncork = () => {
    corked = false;
    socket.uncork();
  };

  const send = (message: M): Eff<void, Throws<NetError>> =>
    suspend(() => {
      if (closed || socket.destroyed || socket.writableEnded) {
        return fail(broken("the connection is closed"));
      }
      let payload: Uint8Array;
      try {
        payload = codec.encode(message);
      } catch (error) {
        return fail(
          new MessageError({
            address: remote,
            message: `couldn't encode: ${String(error)}`,
            cause: error,
          }),
        );
      }
      // Messages sent in the same tick go out in one write instead of a
      // packet each: cork the socket now, uncork it on the next tick.
      if (!corked) {
        corked = true;
        socket.cork();
        process.nextTick(uncork);
      }
      if (socket.write(encodeFrame(payload))) return succeed(undefined);
      // The socket's buffer is full: wait until it drains (or the
      // connection goes away).
      return async<void, NetError>((resume) => {
        const drained = () => {
          socket.off("close", gone);
          resume(succeed(undefined));
        };
        const gone = () => {
          socket.off("drain", drained);
          resume(fail(broken("the connection closed while sending")));
        };
        socket.once("drain", drained);
        socket.once("close", gone);
        return () => {
          socket.off("drain", drained);
          socket.off("close", gone);
        };
      });
    });

  // One stream step per batch taken from the inbox, not one per message.
  const messages = Stream.unfoldChunkEffect(undefined, () =>
    inbox.take.map((batch) => (batch === undefined ? null : ([batch, undefined] as const))),
  );

  const close: Eff<void, never> = async<void, never>((resume) => {
    if (closed || socket.destroyed) return resume(succeed(undefined));
    // Give the other end a moment to finish, then make sure it's gone.
    const timer = setTimeout(() => socket.destroy(), 5_000);
    socket.once("close", () => {
      clearTimeout(timer);
      resume(succeed(undefined));
    });
    socket.end();
  });

  return {
    remote,
    send,
    messages: messages,
    close,
  };
}
