// Framing: on the wire every message is a 4-byte big-endian length followed
// by that many bytes. A socket delivers bytes in arbitrary pieces, so the
// reader collects them until a whole frame is there.

/** The largest frame accepted by default: 16 MiB. */
export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

const HEADER_BYTES = 4;

/** A message's bytes with the length header in front. */
export function encodeFrame(payload: Uint8Array): Uint8Array {
  const frame = new Uint8Array(HEADER_BYTES + payload.length);
  new DataView(frame.buffer).setUint32(0, payload.length);
  frame.set(payload, HEADER_BYTES);
  return frame;
}

/** Thrown when a header announces a frame over the limit. */
export class FrameTooBig extends Error {
  constructor(
    readonly frameBytes: number,
    readonly maxFrameBytes: number,
  ) {
    super(`a message of ${frameBytes} bytes is over the limit of ${maxFrameBytes}`);
  }
}

/**
 * Collects incoming bytes and hands out whole frames. Pieces are kept in a
 * list and joined only when a frame is complete, so a big frame arriving in
 * many pieces is copied once.
 */
export class FrameReader {
  private pieces: Uint8Array[] = [];
  private buffered = 0;
  // Length of the frame being collected, once its header has been read.
  private expected: number | undefined;

  constructor(private readonly maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {}

  /** Add bytes; returns every frame they complete. Throws FrameTooBig. */
  push(bytes: Uint8Array): Uint8Array[] {
    if (bytes.length > 0) {
      this.pieces.push(bytes);
      this.buffered += bytes.length;
    }
    const frames: Uint8Array[] = [];
    while (true) {
      if (this.expected === undefined) {
        if (this.buffered < HEADER_BYTES) break;
        const header = this.take(HEADER_BYTES);
        this.expected = new DataView(header.buffer, header.byteOffset, HEADER_BYTES).getUint32(0);
        if (this.expected > this.maxFrameBytes) {
          throw new FrameTooBig(this.expected, this.maxFrameBytes);
        }
      }
      if (this.buffered < this.expected) break;
      frames.push(this.take(this.expected));
      this.expected = undefined;
    }
    return frames;
  }

  /** Remove and return the first `count` buffered bytes. */
  private take(count: number): Uint8Array {
    if (count === 0) return new Uint8Array(0);
    const first = this.pieces[0]!;
    let result: Uint8Array;
    if (first.length >= count) {
      // Common case: no copy needed.
      result = first.subarray(0, count);
      if (first.length === count) this.pieces.shift();
      else this.pieces[0] = first.subarray(count);
    } else {
      result = new Uint8Array(count);
      let filled = 0;
      while (filled < count) {
        const piece = this.pieces[0]!;
        const used = Math.min(piece.length, count - filled);
        result.set(piece.subarray(0, used), filled);
        filled += used;
        if (used === piece.length) this.pieces.shift();
        else this.pieces[0] = piece.subarray(used);
      }
    }
    this.buffered -= count;
    return result;
  }
}
