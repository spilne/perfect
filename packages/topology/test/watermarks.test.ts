import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import {
  ConsumerGroup,
  StreamTopology,
  TopologyInstanceId,
  TopologyRunner,
  WindowManager,
  type TopologyMetrics,
} from "../src";

interface Click {
  readonly user: string;
  readonly ts: number;
}
interface Counted {
  readonly key: string;
  readonly window: { readonly start: number; readonly end: number };
  readonly count: number;
}

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function listSource<T>(values: T[], firstOffset = 0): Streamable<T> & Acknowledgeable<T> {
  return {
    codec,
    subscribe: () => Stream.fromIterable(values),
    subscribeAck: () =>
      Stream.fromIterable(
        values.map((value, i) => ({
          value,
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: { topic: "clicks", partition: 0, offset: String(firstOffset + i) },
        })),
      ),
  } as unknown as Streamable<T> & Acknowledgeable<T>;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

async function countClicks(
  clicks: Click[],
  options: {
    window?: (
      topology: ReturnType<typeof keyed>,
    ) => ReturnType<ReturnType<typeof keyed>["tumbling"]>;
    state?: InMemoryPartitionedState<unknown>;
    firstOffset?: number;
  } = {},
): Promise<{ items: Counted[]; metrics: TopologyMetrics }> {
  const out = listSink<Counted>();
  const source = keyed(StreamTopology.source(listSource(clicks, options.firstOffset)));
  const windowed = options.window ? options.window(source) : source.tumbling(1_000);
  const runner = await TopologyRunner.run(windowed.count().to(out), {
    group: ConsumerGroup("clicks"),
    partitionedStateBackend: options.state ?? new InMemoryPartitionedState(),
    instanceId: TopologyInstanceId(`run-${Math.random()}`),
  });
  await runner.awaitExit();
  const metrics = runner.metrics();
  await runner.shutdown();
  return { items: out.items, metrics };
}

const keyed = (topology: StreamTopology<Click>) => topology.keyBy((click) => click.user);

describe("windows close by watermark", () => {
  test("a key's window closes when any key moves time past its end", async () => {
    const { items } = await countClicks([
      { user: "ann", ts: 100 },
      { user: "bob", ts: 1_200 }, // only bob is active now, but ann's window is over
      { user: "bob", ts: 1_300 },
    ]);
    // ann's window comes out as soon as bob's click passes 1000, before the end.
    expect(items[0]).toEqual({ key: "ann", window: { start: 0, end: 1_000 }, count: 1 });
  });

  test("a record for a window that already closed is dropped and counted", async () => {
    const { items, metrics } = await countClicks([
      { user: "ann", ts: 100 },
      { user: "ann", ts: 1_500 }, // closes [0, 1000)
      { user: "ann", ts: 200 }, // late: [0, 1000) was emitted already
    ]);
    expect(items).toEqual([
      { key: "ann", window: { start: 0, end: 1_000 }, count: 1 },
      { key: "ann", window: { start: 1_000, end: 2_000 }, count: 1 },
    ]);
    expect(metrics.lateRecords).toBe(1);
  });

  test("allowedLatenessMs keeps windows open for out-of-order records", async () => {
    const { items, metrics } = await countClicks(
      [
        { user: "ann", ts: 100 },
        { user: "ann", ts: 1_500 },
        { user: "ann", ts: 200 }, // within the 1 s of allowed lateness
        { user: "ann", ts: 2_100 }, // now [0, 1000) closes
      ],
      { window: (source) => source.tumbling(1_000, { allowedLatenessMs: 1_000 }) },
    );
    expect(items[0]).toEqual({ key: "ann", window: { start: 0, end: 1_000 }, count: 2 });
    expect(metrics.lateRecords).toBe(0);
  });

  test("the watermark survives a restart", async () => {
    const state = new InMemoryPartitionedState<unknown>();
    await countClicks([{ user: "ann", ts: 5_000 }], { state });
    // After the restart, a click at 100 belongs to a window that closed long ago.
    const { items, metrics } = await countClicks([{ user: "ann", ts: 100 }], {
      state,
      firstOffset: 1,
    });
    expect(items).toEqual([]);
    expect(metrics.lateRecords).toBe(1);
  });

  test("windows still open when the input ends are emitted", async () => {
    const { items } = await countClicks([
      { user: "ann", ts: 100 },
      { user: "bob", ts: 200 },
    ]);
    expect(items).toEqual([
      { key: "ann", window: { start: 0, end: 1_000 }, count: 1 },
      { key: "bob", window: { start: 0, end: 1_000 }, count: 1 },
    ]);
  });
});

describe("session windows", () => {
  test("a record between two sessions joins them into one", async () => {
    const { items } = await countClicks(
      [
        { user: "ann", ts: 0 },
        { user: "ann", ts: 2_000 }, // a second session (gap > 1000)
        { user: "ann", ts: 1_000 }, // late but within lateness: bridges both
      ],
      { window: (source) => source.session(1_000, { allowedLatenessMs: 5_000 }) as never },
    );
    expect(items).toEqual([{ key: "ann", window: { start: 0, end: 2_000 }, count: 3 }]);
  });

  test("without merge, the sessions stay separate", () => {
    const manager = new WindowManager<{ n: number }, number, { start: number; n: number }>(
      { type: "session", gapMs: 1_000 },
      {
        init: () => ({ n: 0 }),
        add: (state) => ({ n: state.n + 1 }),
        emit: (_key, window, state) => ({ start: window.start, n: state.n }),
      },
    );
    manager.add("ann", 0, 0);
    manager.add("ann", 0, 2_000);
    manager.add("ann", 0, 1_000); // joins the earlier session only
    expect(manager.flushAll()).toEqual([
      { start: 0, n: 2 },
      { start: 2_000, n: 1 },
    ]);
  });
});
