// Framing: on the wire every message is a 4-byte big-endian length followed
// by that many bytes. A socket delivers bytes in arbitrary pieces, so the
// reader collects them until a whole frame is there.

/** The largest frame accepted by default: 16 MiB. */
export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

const HEADER_BYTES = 4;
const RETAINED_BUFFER_BYTES = 64 * 1024;

/** The largest length the 4-byte header can express. */
const MAX_HEADER_VALUE = 0xffff_ffff;

/** A message's bytes with the length header in front. */
export function encodeFrame(payload: Uint8Array): Uint8Array {
  if (payload.length > MAX_HEADER_VALUE) throw new FrameTooBig(payload.length, MAX_HEADER_VALUE);
  const frame = new Uint8Array(HEADER_BYTES + payload.length);
  new DataView(frame.buffer).setUint32(0, payload.length);
  frame.set(payload, HEADER_BYTES);
  return frame;
}

/** Why the bytes on a connection stopped being frames. */
export class FrameError extends Error {}

/** Thrown when a header announces a frame over the limit. */
export class FrameTooBig extends FrameError {
  constructor(
    readonly frameBytes: number,
    readonly maxFrameBytes: number,
  ) {
    super(`a message of ${frameBytes} bytes is over the limit of ${maxFrameBytes}`);
  }
}

/** The stream ended in the middle of a frame. */
export class FrameTruncated extends FrameError {
  constructor(readonly pendingBytes: number) {
    super(`the connection ended in the middle of a message (${pendingBytes} bytes of it arrived)`);
  }
}

/**
 * Collects incoming bytes and hands out whole frames. Bytes of a frame that
 * is still incomplete are copied once into a buffer that doubles when full,
 * so a frame arriving in many small pieces costs linear time. Whole frames
 * inside one chunk are returned as views of that chunk without copying.
 *
 * Once a header is refused the reader is poisoned: it never resynchronises,
 * because the bytes after a bad header can't be trusted to be frames. The
 * frames completed before the bad header are still returned by the `push`
 * that met it, and `failure` says what went wrong; every later `push`
 * throws it.
 */
export class FrameReader {
  private buffer = new Uint8Array(0);
  private start = 0;
  private end = 0;
  private poisoned: FrameError | undefined;

  constructor(private readonly maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {
    if (!Number.isInteger(maxFrameBytes) || maxFrameBytes < 0 || maxFrameBytes > MAX_HEADER_VALUE) {
      throw new RangeError(
        `the frame size limit must be an integer from 0 to ${MAX_HEADER_VALUE}, got ${maxFrameBytes}`,
      );
    }
  }

  /** Set once a header was refused. */
  get failure(): FrameError | undefined {
    return this.poisoned;
  }

  /** Bytes received for a frame that isn't complete yet. */
  get pendingBytes(): number {
    return this.end - this.start;
  }

  /**
   * Add bytes; returns every frame they complete. Throws FrameTooBig when a
   * header is over the limit and no complete frame came before it (see
   * `failure` for the other case), and on every call after that.
   */
  push(bytes: Uint8Array): Uint8Array[] {
    if (this.poisoned) throw this.poisoned;
    if (this.pendingBytes === 0) return this.parse({ chunk: bytes, buffered: false });
    this.append(bytes);
    return this.parse({ chunk: this.buffer.subarray(this.start, this.end), buffered: true });
  }

  /**
   * Take the frames out of `chunk`. A `buffered` chunk is our own buffer, so
   * its frames are copied (the buffer is reused) and what is left over stays
   * where it is; otherwise the frames are views and the rest is buffered.
   */
  private parse(params: { chunk: Uint8Array; buffered: boolean }): Uint8Array[] {
    const { chunk, buffered } = params;
    const frames: Uint8Array[] = [];
    let offset = 0;
    while (chunk.length - offset >= HEADER_BYTES) {
      const length = new DataView(chunk.buffer, chunk.byteOffset + offset, HEADER_BYTES).getUint32(
        0,
      );
      if (length > this.maxFrameBytes) {
        this.poisoned = new FrameTooBig(length, this.maxFrameBytes);
        this.start = this.end = 0;
        if (frames.length === 0) throw this.poisoned;
        return frames;
      }
      if (chunk.length - offset - HEADER_BYTES < length) break;
      const from = offset + HEADER_BYTES;
      const frame = chunk.subarray(from, from + length);
      frames.push(buffered ? frame.slice() : frame);
      offset = from + length;
    }
    if (buffered) {
      this.start += offset;
      if (this.start === this.end) {
        this.start = this.end = 0;
        // Don't keep a big frame's buffer for the rest of the connection.
        if (this.buffer.length > RETAINED_BUFFER_BYTES) this.buffer = new Uint8Array(0);
      }
    } else if (offset < chunk.length) {
      this.append(chunk.subarray(offset));
    }
    return frames;
  }

  private append(bytes: Uint8Array): void {
    const needed = this.end + bytes.length;
    if (needed > this.buffer.length) {
      const live = this.end - this.start;
      const grown = new Uint8Array(Math.max(needed - this.start, this.buffer.length * 2, 64));
      grown.set(this.buffer.subarray(this.start, this.end));
      this.buffer = grown;
      this.start = 0;
      this.end = live;
    }
    this.buffer.set(bytes, this.end);
    this.end += bytes.length;
  }
}
