// A peer that doesn't follow the protocol must not be able to grow the
// multiplexer's tables or queues without limit.

import { describe, expect, test } from "bun:test";
import {
  Deferred,
  eff,
  fork,
  join,
  scoped,
  sleep,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { BinaryCodec, Multiplexer, SimulatedNetwork, type Connection, type NetError } from "../src";

const codec = BinaryCodec.json<string>();

const OPEN = 1;
const DATA = 2;
const CREDIT = 3;
const CLOSE = 4;

const windowOf = (window: number) => {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, window);
  return bytes;
};
const openFrame = (id: number, window = 256) => frame(OPEN, id, windowOf(window));
const frame = (kind: number, id: number, payload: Uint8Array = new Uint8Array(0)) => {
  const bytes = new Uint8Array(5 + payload.length);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, kind);
  view.setUint32(1, id);
  bytes.set(payload, 5);
  return bytes;
};
const data = (id: number, text: string) =>
  frame(DATA, id, new TextEncoder().encode(JSON.stringify(text)));
const kindsAndIds = (frames: Uint8Array[]) =>
  frames.map((bytes) => [bytes[0], new DataView(bytes.buffer, bytes.byteOffset).getUint32(1)]);

interface Harness {
  readonly mux: Multiplexer<string>;
  /** The hostile peer: raw frames in and out. */
  readonly peer: Connection<Uint8Array>;
  readonly received: Uint8Array[];
}

/**
 * A multiplexer on the "server" side (it numbers its channels 2, 4...),
 * talking to a test that writes raw frames, which is what a peer ignoring
 * the protocol would do.
 */
const withHostilePeer = <A>(
  options: { window?: number; maxChannels?: number; maxPendingIncoming?: number },
  body: (harness: Harness) => Eff<A, Throws<NetError>>,
): Promise<A> => {
  const program: Eff<A, Throws<NetError>> = eff(function* () {
    const network = yield* SimulatedNetwork.make();
    const accepted = yield* Deferred.make<Connection<Uint8Array>>();
    const release = yield* Deferred.make<void>();
    const listener = yield* network.node("server").listen(
      { host: "server", port: 0 },
      {
        codec: BinaryCodec.bytes,
        handle: (connection) => accepted.succeed(connection).flatMap(() => release.await),
      },
    );
    const peer = yield* network
      .node("client")
      .connect(listener.address, { codec: BinaryCodec.bytes });
    const serverEnd = yield* accepted.await;
    const mux = yield* Multiplexer.make(serverEnd, { side: "server", codec, ...options });
    const received: Uint8Array[] = [];
    yield* fork(peer.messages.forEach((bytes) => sync(() => void received.push(bytes))));
    const result = yield* body({ mux, peer, received });
    yield* release.succeed(undefined);
    return result;
  }) as Eff<A, Throws<NetError>>;
  return scoped(program).orDie().run();
};

const settle = sleep(20);

describe("Multiplexer limits", () => {
  test("channels opened over maxChannels are refused with a CLOSE, and the table stays small", async () => {
    const { open, rejected, closes } = await withHostilePeer(
      { maxChannels: 4 },
      ({ mux, peer, received }) =>
        eff(function* () {
          for (let id = 1; id <= 41; id += 2) yield* peer.send(openFrame(id));
          yield* settle;
          const stats = yield* mux.stats;
          return {
            open: stats.openChannels,
            rejected: stats.rejectedChannels,
            closes: kindsAndIds(received).filter(([kind]) => kind === CLOSE).length,
          };
        }),
    );
    expect(open).toBe(4);
    expect(rejected).toBe(17);
    expect(closes).toBe(17);
  });

  test("opening a channel here past maxChannels fails with a typed error", async () => {
    const result = await withHostilePeer({ maxChannels: 2 }, ({ mux }) =>
      eff(function* () {
        yield* mux.open;
        yield* mux.open;
        const third = yield* mux.open.either();
        return third._tag === "Left" ? third.left._tag : "opened";
      }),
    );
    expect(result).toBe("ChannelLimitExceeded");
  });

  test("channels the application doesn't pick up from `incoming` are capped", async () => {
    const pending = await withHostilePeer({ maxPendingIncoming: 3 }, ({ mux, peer }) =>
      eff(function* () {
        for (let id = 1; id <= 21; id += 2) yield* peer.send(openFrame(id));
        yield* settle;
        return (yield* mux.stats).pendingIncoming;
      }),
    );
    expect(pending).toBe(3);
  });

  test("picking channels up makes room for more", async () => {
    const accepted = await withHostilePeer({ maxPendingIncoming: 2 }, ({ mux, peer }) =>
      eff(function* () {
        yield* peer.send(openFrame(1));
        yield* peer.send(openFrame(3));
        yield* settle;
        const first = yield* mux.incoming.take(2).toArray();
        yield* peer.send(openFrame(5));
        yield* settle;
        const second = yield* mux.incoming.take(1).toArray();
        return first.length + second.length;
      }),
    );
    expect(accepted).toBe(3);
  });

  test("data beyond the window fails that channel, not the process's memory", async () => {
    const outcome = await withHostilePeer({ window: 3 }, ({ mux, peer, received }) =>
      eff(function* () {
        yield* peer.send(openFrame(1));
        const [channel] = yield* mux.incoming.take(1).toArray();
        for (let n = 0; n < 50; n++) yield* peer.send(data(1, `m${n}`));
        yield* settle;
        const stats = yield* mux.stats;
        const read = yield* channel!.messages.toArray().either();
        return {
          open: stats.openChannels,
          read: read._tag === "Left" ? read.left._tag : read.right,
          closed: kindsAndIds(received).some(([kind, id]) => kind === CLOSE && id === 1),
        };
      }),
    );
    expect(outcome).toEqual({ open: 0, read: "MessageError", closed: true });
  });

  test("a peer that follows the window is not cut off", async () => {
    const texts = await withHostilePeer({ window: 3 }, ({ mux, peer }) =>
      eff(function* () {
        yield* peer.send(openFrame(1));
        const [channel] = yield* mux.incoming.take(1).toArray();
        for (let n = 0; n < 3; n++) yield* peer.send(data(1, `m${n}`));
        const first = yield* channel!.messages.take(3).toArray();
        for (let n = 3; n < 6; n++) yield* peer.send(data(1, `m${n}`));
        const second = yield* channel!.messages.take(3).toArray();
        return [...first, ...second];
      }),
    );
    expect(texts).toEqual(["m0", "m1", "m2", "m3", "m4", "m5"]);
  });

  test("a duplicate OPEN is a protocol violation that stops the multiplexer", async () => {
    const reopen = await withHostilePeer({}, ({ mux, peer }) =>
      eff(function* () {
        yield* peer.send(openFrame(1));
        yield* peer.send(openFrame(1));
        yield* settle;
        return (yield* mux.open.either())._tag;
      }),
    );
    expect(reopen).toBe("Left");
  });

  test("ids of the wrong parity or going backwards are protocol violations", async () => {
    for (const ids of [[2], [5, 3]]) {
      const reopen = await withHostilePeer({}, ({ mux, peer }) =>
        eff(function* () {
          for (const id of ids) yield* peer.send(openFrame(id));
          yield* settle;
          return (yield* mux.open.either())._tag;
        }),
      );
      expect(reopen).toBe("Left");
    }
  });

  test("a CREDIT frame without its count stops the multiplexer cleanly", async () => {
    const reopen = await withHostilePeer({}, ({ mux, peer }) =>
      eff(function* () {
        yield* peer.send(openFrame(1));
        yield* peer.send(frame(CREDIT, 1, new Uint8Array(2)));
        yield* settle;
        return (yield* mux.open.either())._tag;
      }),
    );
    expect(reopen).toBe("Left");
  });

  test("frames for channels that are gone are ignored, and closed channels leave the table", async () => {
    const stats = await withHostilePeer({}, ({ mux, peer }) =>
      eff(function* () {
        for (let n = 0; n < 20; n++) {
          const channel = yield* mux.open;
          yield* channel.close;
        }
        yield* peer.send(data(2, "late"));
        yield* peer.send(frame(CLOSE, 4));
        yield* peer.send(frame(CREDIT, 6, new Uint8Array(4)));
        yield* settle;
        const alive = yield* mux.open.either();
        return { stats: yield* mux.stats, alive: alive._tag };
      }),
    );
    expect(stats.alive).toBe("Right");
    expect(stats.stats.openChannels).toBe(1);
  });

  test("when the connection closes, waiting senders fail and nothing stays in the tables", async () => {
    const outcome = await withHostilePeer({ window: 1 }, ({ mux, peer }) =>
      eff(function* () {
        const channel = yield* mux.open;
        yield* peer.send(frame(CREDIT, 2, windowOf(1)));
        yield* channel.send("uses the only credit");
        const blocked = yield* fork(channel.send("waits for credit").either());
        yield* settle;
        yield* peer.close;
        yield* settle;
        const sent = yield* join(blocked);
        const stats = yield* mux.stats;
        return {
          sent: sent._tag === "Left",
          open: stats.openChannels,
          reopen: (yield* mux.open.either())._tag,
        };
      }),
    );
    expect(outcome).toEqual({ sent: true, open: 0, reopen: "Left" });
  });
});
