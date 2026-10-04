// DistributedRunner end to end: stateful steps after shuffle(), running
// through an in-memory transport that passes records between stages.

import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type Envelope,
  type ShuffleTransport,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import { ConsumerGroup, DistributedRunner, StreamTopology } from "../src";

interface Click {
  readonly user: string;
  readonly page: string;
  readonly ts: number;
}

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

// A channel's reader yields records as they are published and stops once
// nothing new has arrived for a little while (the test sources are finite).
function liveTransport(): ShuffleTransport<unknown, unknown> {
  const channels = new Map<string, unknown[]>();
  const channel = (name: string) => {
    let items = channels.get(name);
    if (!items) channels.set(name, (items = []));
    return items;
  };
  async function* read(items: unknown[]): AsyncGenerator<Envelope<unknown>> {
    let next = 0;
    let idleSince = Date.now();
    while (Date.now() - idleSince < 300) {
      if (next < items.length) {
        const value = items[next++];
        idleSince = Date.now();
        yield {
          value,
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: {},
        };
      } else {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    }
  }
  return {
    async getOrCreateRepartitionChannel(params) {
      const items = channel(params.name);
      return {
        source: {
          codec: params.codec,
          subscribe: () => Stream.empty(),
          subscribeAck: () => Stream.fromAsyncIterable(read(items), (error) => error),
        },
        sink: {
          codec: params.codec,
          publish: (value: unknown) => sync(() => void items.push(value)),
        },
      } as never;
    },
  };
}

function listSource<T>(values: T[]): Streamable<T> & Acknowledgeable<T> {
  return {
    codec,
    subscribe: () => Stream.fromIterable(values),
    subscribeAck: () =>
      Stream.fromIterable(
        values.map((value) => ({
          value,
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: {},
        })),
      ),
  } as unknown as Streamable<T> & Acknowledgeable<T>;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

const clicks: Click[] = [
  { user: "ann", page: "/a", ts: 100 },
  { user: "bob", page: "/a", ts: 200 },
  { user: "ann", page: "/b", ts: 300 },
  { user: "ann", page: "/b", ts: 1_500 },
];

async function runToEnd(
  topology: Parameters<typeof DistributedRunner.run>[0],
  group: string,
): Promise<void> {
  const handle = await DistributedRunner.run(topology, {
    group: ConsumerGroup(group),
    shuffleTransport: liveTransport(),
    partitionedStateBackend: new InMemoryPartitionedState(),
  });
  const exits = await handle.awaitExit();
  await handle.shutdown();
  expect(exits.every((exit) => exit._tag === "Success")).toBe(true);
}

describe("DistributedRunner with state after shuffle()", () => {
  test("keyBy → shuffle → tumbling window → count (the documented example)", async () => {
    const out = listSink<{ key: string; window: { start: number; end: number }; count: number }>();
    await runToEnd(
      StreamTopology.source(listSource(clicks))
        .keyBy((click) => click.user)
        .shuffle()
        .tumbling(1_000)
        .count()
        .to(out),
      "window-after-shuffle",
    );
    // ann's click at 1500 closes ann's first window.
    expect(out.items).toEqual([{ key: "ann", window: { start: 0, end: 1_000 }, count: 2 }]);
  });

  test("process keeps per-key state after a shuffle", async () => {
    const out = listSink<string>();
    await runToEnd(
      StreamTopology.source(listSource(clicks))
        .keyBy((click) => click.user)
        .shuffle()
        .process({
          init: () => 0,
          process: (seen: number, click: Click) => ({
            state: seen + 1,
            emit: `${click.user}#${seen + 1}`,
          }),
        })
        .to(out),
      "process-after-shuffle",
    );
    expect(out.items).toEqual(["ann#1", "bob#1", "ann#2", "ann#3"]);
  });

  test("a second shuffle re-keys the records", async () => {
    const out = listSink<string>();
    await runToEnd(
      StreamTopology.source(listSource(clicks))
        .keyBy((click) => click.user)
        .shuffle()
        // Stage 2: number each user's clicks, then re-key by page.
        .process({
          init: () => 0,
          process: (n: number, click: Click) => ({ state: n + 1, emit: click }),
        })
        .keyBy((click) => click.page)
        .shuffle()
        // Stage 3: count views per page.
        .process({
          init: () => 0,
          process: (views: number, click: Click) => ({
            state: views + 1,
            emit: `${click.page}:${views + 1}`,
          }),
        })
        .to(out),
      "two-shuffles",
    );
    expect(out.items).toEqual(["/a:1", "/a:2", "/b:1", "/b:2"]);
  });

  test("a join in a shuffled topology is refused with a clear message", async () => {
    const users = StreamTopology.source(listSource([{ user: "ann", name: "Ann", ts: 0 }])).keyBy(
      (u) => u.user,
    );
    const topology = StreamTopology.source(listSource(clicks))
      .keyBy((click) => click.user)
      .shuffle()
      .join(users, { windowMs: 1_000 })
      .to(listSink());
    await expect(
      DistributedRunner.run(topology, {
        group: ConsumerGroup("join"),
        shuffleTransport: liveTransport(),
        partitionedStateBackend: new InMemoryPartitionedState(),
      }),
    ).rejects.toThrow(/join/);
  });
});
