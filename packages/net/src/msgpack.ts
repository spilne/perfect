// MessagePack messages, through msgpackr (install it alongside this package).
// Smaller and faster than JSON, and it keeps types JSON can't: Dates,
// Uint8Arrays, Maps, Sets and bigints.

import { Packr } from "msgpackr";
import type { BinaryCodec } from "./codec.js";

/** A MessagePack codec. Pass msgpackr options to change how values are encoded. */
export function msgpackCodec<T>(options?: ConstructorParameters<typeof Packr>[0]): BinaryCodec<T> {
  const packr = new Packr({ useRecords: false, ...options });
  return {
    encode: (message) => packr.pack(message),
    decode: (bytes) => packr.unpack(bytes) as T,
  };
}
