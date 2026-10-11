// How messages become bytes and back. Every connection has one codec for
// the messages it sends and receives (a union type works well, e.g. one
// tagged with `type`).
//
// The built-in codecs cover plain bytes, text and JSON. For compact binary
// formats, see `@spilne/perfect-net/msgpack` and `@spilne/perfect-net/protobuf`.

/** Turns a message into bytes and back. `decode` may throw for bad input. */
export interface BinaryCodec<T> {
  /** The bytes of one message. */
  encode(message: T): Uint8Array;
  /** The message in `bytes`; throws if they aren't one. */
  decode(bytes: Uint8Array): T;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export const BinaryCodec = {
  /** The bytes as they are. */
  bytes: {
    encode: (message: Uint8Array) => message,
    decode: (bytes: Uint8Array) => bytes,
  } satisfies BinaryCodec<Uint8Array>,

  /** UTF-8 text. Bytes that aren't valid UTF-8 are rejected. */
  utf8: {
    encode: (message: string) => encoder.encode(message),
    decode: (bytes: Uint8Array) => decoder.decode(bytes),
  } satisfies BinaryCodec<string>,

  /**
   * JSON as UTF-8. The type parameter is not checked when decoding; check
   * the shape yourself (or use a schema) if the peer isn't trusted.
   */
  json<T>(): BinaryCodec<T> {
    return {
      encode: (message) => encoder.encode(JSON.stringify(message)),
      decode: (bytes) => JSON.parse(decoder.decode(bytes)) as T,
    };
  },
} as const;
