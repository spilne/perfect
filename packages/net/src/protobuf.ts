// Protocol Buffers messages, through @bufbuild/protobuf (Protobuf-ES; install
// it alongside this package). Give it the schema generated for your message
// type by protoc-gen-es.

import { fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import type { BinaryCodec } from "./codec.js";

/** A codec for one Protobuf message type, from its generated schema. */
export function protobufCodec<Desc extends DescMessage>(
  schema: Desc,
): BinaryCodec<MessageShape<Desc>> {
  return {
    encode: (message) => toBinary(schema, message),
    decode: (bytes) => fromBinary(schema, bytes),
  };
}
