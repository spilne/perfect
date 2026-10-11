// WebSocket client connections, as the same typed Connection as TCP: a typed
// `send`, the incoming `messages` as a Stream, and `close`.
//
//   const feed = yield* WebSocketClient.connect("wss://example.com/feed", {
//     codec: BinaryCodec.json<Tick>(),
//     text: true,                       // JSON as text frames
//   });
//   yield* feed.messages.take(10).forEach(handle);
//
// It uses the WebSocket built into Bun, Node (22+), Deno and browsers. Being
// a Connection, it works with Resilient.connect (reconnecting), the
// Multiplexer and the Recorder like any other.
//
// The WebSocket API can't pause the other side, so received messages are
// buffered however fast they come; read them promptly. Sending waits while
// more than `maxBufferedBytes` are queued in the socket.

import {
  acquireRelease,
  async,
  fail,
  succeed,
  suspend,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import type { Connection, ConnectionOptions } from "./connection.js";
import { ConnectError, ConnectionError, MessageError, type NetError } from "./errors.js";
import { DEFAULT_MAX_FRAME_BYTES } from "./frames.js";
import { Inbox } from "./inbox.js";

export interface WebSocketOptions<M> extends ConnectionOptions<M> {
  /** Subprotocols to offer the server. */
  readonly protocols?: string | readonly string[];
  /**
   * Send text frames (the codec's bytes as UTF-8) instead of binary ones.
   * Most JSON APIs expect text. Received frames of either kind are decoded.
   */
  readonly text?: boolean;
  /** Give up connecting after this many ms. Default: 10 000. */
  readonly timeoutMs?: number;
  /** `send` waits while more than this many bytes wait to go out. Default: 1 MiB. */
  readonly maxBufferedBytes?: number;
}

/** Close codes that mean the connection ended normally. */
const NORMAL_CLOSE = new Set([1000, 1001, 1005]);

const utf8 = new TextEncoder();
const fromUtf8 = new TextDecoder();

export const WebSocketClient = {
  /**
   * Connect to a WebSocket server. The connection is closed when the
   * surrounding scope ends (or with `.use`).
   */
  connect<M>(
    url: string | URL,
    options: WebSocketOptions<M>,
  ): Eff<Connection<M>, Throws<ConnectError>> {
    return acquireRelease(open(url, options), (connection) => connection.close);
  },
} as const;

function open<M>(
  url: string | URL,
  options: WebSocketOptions<M>,
): Eff<Connection<M>, Throws<ConnectError>> {
  const where = String(url);
  return async<Connection<M>, ConnectError>((resume) => {
    let socket: WebSocket;
    try {
      socket = new WebSocket(url, options.protocols as string | string[] | undefined);
    } catch (error) {
      resume(fail(new ConnectError({ address: where, message: String(error), cause: error })));
      return;
    }
    socket.binaryType = "arraybuffer";

    const timer = setTimeout(() => {
      socket.close();
      failed(`timed out connecting to ${where}`);
    }, options.timeoutMs ?? 10_000);
    const failed = (message: string, cause?: unknown) => {
      clearTimeout(timer);
      socket.removeEventListener("open", opened);
      resume(fail(new ConnectError({ address: where, message, cause })));
    };
    const onError = (event: Event) => failed(`could not connect to ${where}`, event);
    const onClose = (event: CloseEvent) =>
      failed(`${where} closed the connection (${event.code} ${event.reason})`.trim(), event);
    const opened = () => {
      clearTimeout(timer);
      socket.removeEventListener("error", onError);
      socket.removeEventListener("close", onClose);
      resume(succeed(connectionOver(socket, where, options)));
    };
    socket.addEventListener("open", opened, { once: true });
    socket.addEventListener("error", onError, { once: true });
    socket.addEventListener("close", onClose, { once: true });
    // Interrupted while connecting.
    return () => {
      clearTimeout(timer);
      socket.removeEventListener("open", opened);
      socket.close();
    };
  });
}

function connectionOver<M>(
  socket: WebSocket,
  remote: string,
  options: WebSocketOptions<M>,
): Connection<M> {
  const { codec } = options;
  const maxMessageBytes = options.maxMessageBytes ?? DEFAULT_MAX_FRAME_BYTES;
  const maxBufferedBytes = options.maxBufferedBytes ?? 1024 * 1024;
  // The socket can't be paused, so the inbox never asks it to.
  const inbox = new Inbox<M, NetError>(
    Infinity,
    () => {},
    () => {},
  );
  const broken = (message: string, cause?: unknown) =>
    new ConnectionError({ address: remote, message, cause });

  socket.addEventListener("message", (event: MessageEvent) => {
    const data: unknown = event.data;
    const bytes =
      typeof data === "string"
        ? utf8.encode(data)
        : data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : undefined;
    if (bytes === undefined) return;
    if (bytes.length > maxMessageBytes) {
      inbox.fail(
        new MessageError({
          address: remote,
          message: `a message of ${bytes.length} bytes is over the limit of ${maxMessageBytes}`,
        }),
      );
      socket.close(1009, "message too big");
      return;
    }
    try {
      inbox.push(codec.decode(bytes));
    } catch (error) {
      inbox.fail(
        new MessageError({
          address: remote,
          message: `couldn't decode: ${String(error)}`,
          cause: error,
        }),
      );
    }
  });
  socket.addEventListener("close", (event: CloseEvent) => {
    if (NORMAL_CLOSE.has(event.code)) inbox.end();
    else
      inbox.fail(
        broken(`closed with code ${event.code}${event.reason ? `: ${event.reason}` : ""}`),
      );
  });

  // Wait until the socket's outgoing buffer has room, or it closes.
  const room: Eff<void, Throws<NetError>> = async<void, NetError>((resume) => {
    const check = setInterval(() => {
      if (socket.readyState !== WebSocket.OPEN) {
        clearInterval(check);
        resume(fail(broken("the connection closed while sending")));
      } else if (socket.bufferedAmount <= maxBufferedBytes) {
        clearInterval(check);
        resume(succeed(undefined));
      }
    }, 5);
    return () => clearInterval(check);
  });

  const send = (message: M): Eff<void, Throws<NetError>> =>
    suspend(() => {
      if (socket.readyState !== WebSocket.OPEN) {
        return fail(broken("the connection is closed"));
      }
      let bytes: Uint8Array;
      try {
        bytes = codec.encode(message);
      } catch (error) {
        return fail(
          new MessageError({
            address: remote,
            message: `couldn't encode: ${String(error)}`,
            cause: error,
          }),
        );
      }
      socket.send(options.text ? fromUtf8.decode(bytes) : sendable(bytes));
      return socket.bufferedAmount > maxBufferedBytes ? room : succeed(undefined);
    });

  const messages = Stream.unfoldChunkEffect(undefined, () =>
    inbox.take.map((batch) => (batch === undefined ? null : ([batch, undefined] as const))),
  );

  // Close normally and wait for the server to agree, at most 5 s.
  const close: Eff<void, never> = async<void, never>((resume) => {
    if (socket.readyState === WebSocket.CLOSED) {
      resume(succeed(undefined));
      return;
    }
    const done = () => {
      clearTimeout(timer);
      resume(succeed(undefined));
    };
    const timer = setTimeout(done, 5_000);
    socket.addEventListener("close", done, { once: true });
    if (socket.readyState !== WebSocket.CLOSING) socket.close(1000);
    return () => {
      clearTimeout(timer);
      socket.removeEventListener("close", done);
    };
  });

  return { remote, send, messages, close };
}

/** WebSocket sends bytes over a plain ArrayBuffer; copy only the rare others. */
function sendable(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  return isOverArrayBuffer(bytes) ? bytes : new Uint8Array(bytes);
}

function isOverArrayBuffer(bytes: Uint8Array): bytes is Uint8Array<ArrayBuffer> {
  return bytes.buffer instanceof ArrayBuffer;
}
