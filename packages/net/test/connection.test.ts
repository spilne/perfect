import { describe, expect, test } from "bun:test";
import { Duplex } from "node:stream";
import { sync } from "@spilne/perfect-core";
import { BinaryCodec, connectionOver } from "../src";
import { encodeFrame } from "../src/frames";

const codec = BinaryCodec.utf8;
const text = (value: string) => encodeFrame(new TextEncoder().encode(value));
const join = (...parts: Uint8Array[]) => new Uint8Array(parts.flatMap((part) => [...part]));

/** A socket the test feeds by hand; what the connection writes is thrown away. */
const fakeSocket = () => new Duplex({ read() {}, write: (_chunk, _encoding, done) => done() });

describe("connectionOver", () => {
  test("messages before a refused header are delivered, then the stream fails", async () => {
    const socket = fakeSocket();
    const connection = connectionOver(socket, "peer", { codec, maxMessageBytes: 8 });
    socket.push(join(text("one"), text("two"), new Uint8Array([0, 0, 1, 0])));
    const seen: string[] = [];
    const result = await connection.messages
      .forEach((message) => sync(() => void seen.push(message)))
      .either()
      .run();
    expect(seen).toEqual(["one", "two"]);
    expect(result._tag === "Left" && result.left._tag).toBe("MessageError");
    expect(socket.destroyed).toBe(true);
  });

  test("a connection that ends in the middle of a message fails instead of ending cleanly", async () => {
    const socket = fakeSocket();
    const connection = connectionOver(socket, "peer", { codec });
    socket.push(join(text("whole"), text("cut off").subarray(0, 6)));
    socket.push(null);
    const seen: string[] = [];
    const result = await connection.messages
      .forEach((message) => sync(() => void seen.push(message)))
      .either()
      .run();
    expect(seen).toEqual(["whole"]);
    expect(result._tag === "Left" && result.left._tag).toBe("MessageError");
  });

  test("a connection that ends between messages ends cleanly", async () => {
    const socket = fakeSocket();
    const connection = connectionOver(socket, "peer", { codec });
    socket.push(join(text("a"), text("b")));
    socket.push(null);
    expect(await connection.messages.toArray().orDie().run()).toEqual(["a", "b"]);
  });

  test("a reader that falls behind pauses the socket, and reading resumes it", async () => {
    const socket = fakeSocket();
    const connection = connectionOver(socket, "peer", { codec, highWaterMark: 2 });
    socket.push(join(text("1"), text("2"), text("3")));
    await new Promise((resolve) => setImmediate(resolve));
    expect(socket.isPaused()).toBe(true);
    expect(await connection.messages.take(3).toArray().orDie().run()).toEqual(["1", "2", "3"]);
    expect(socket.isPaused()).toBe(false);
  });

  test("nonsense limits are rejected when the connection is made", () => {
    expect(() => connectionOver(fakeSocket(), "peer", { codec, maxMessageBytes: -1 })).toThrow(
      RangeError,
    );
    expect(() => connectionOver(fakeSocket(), "peer", { codec, highWaterMark: 0 })).toThrow(
      RangeError,
    );
    expect(() =>
      connectionOver(fakeSocket(), "peer", { codec, highWaterMark: Number.NaN }),
    ).toThrow(RangeError);
  });
});
