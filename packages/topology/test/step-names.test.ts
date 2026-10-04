import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type PartitionedStateBackend,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import { ConsumerGroup, StreamTopology, TopologyInstanceId, TopologyRunner } from "../src";

interface Order {
  readonly customer: string;
}

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function listSource<T>(values: T[], firstOffset: number): Streamable<T> & Acknowledgeable<T> {
  return {
    codec,
    subscribe: () => Stream.fromIterable(values),
    subscribeAck: () =>
      Stream.fromIterable(
        values.map((value, i) => ({
          value,
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: { topic: "orders", partition: 0, offset: String(firstOffset + i) },
        })),
      ),
  } as unknown as Streamable<T> & Acknowledgeable<T>;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

// A store the runner can't tell is in memory, standing in for Redis/Postgres.
function durableState(): PartitionedStateBackend<unknown> {
  const inner = new InMemoryPartitionedState<unknown>();
  return {
    acquire: (params) => inner.acquire(params),
    renew: (params) => inner.renew(params),
    release: (lease) => inner.release(lease),
    load: (lease) => inner.load(lease),
    commit: (params) => inner.commit(params),
    isProcessed: (params) => inner.isProcessed(params),
  };
}

const counting = {
  init: () => 0,
  process: (count: number) => ({ state: count + 1, emit: count + 1 }),
};

async function run(
  topology: ReturnType<StreamTopology<unknown>["to"]>,
  state: PartitionedStateBackend<unknown>,
  onWarning: (message: string) => void = () => {},
): Promise<void> {
  const runner = await TopologyRunner.run(topology, {
    group: ConsumerGroup("orders"),
    partitionedStateBackend: state,
    instanceId: TopologyInstanceId(`run-${Math.random()}`),
    onWarning,
  });
  await runner.awaitExit();
  await runner.shutdown();
}

describe("step names", () => {
  test("a named step keeps its state when a step is added before it", async () => {
    const state = durableState();
    const first = listSink<number>();
    await run(
      StreamTopology.source(listSource<Order>([{ customer: "c1" }], 0))
        .keyBy((order) => order.customer)
        .process(counting, { name: "orders-per-customer" })
        .to(first),
      state,
    );

    // The next version adds another process step in front.
    const second = listSink<number>();
    await run(
      StreamTopology.source(listSource<Order>([{ customer: "c1" }], 1))
        .keyBy((order) => order.customer)
        .process(
          { init: () => 0, process: (n: number, o: Order) => ({ state: n, emit: o }) },
          {
            name: "audit",
          },
        )
        .keyBy((order) => order.customer)
        .process(counting, { name: "orders-per-customer" })
        .to(second),
      state,
    );

    expect(first.items).toEqual([1]);
    expect(second.items).toEqual([2]); // continued from the saved count
  });

  test('naming a step after its position ("0") keeps its existing state', async () => {
    const state = durableState();
    const before = listSink<number>();
    await run(
      StreamTopology.source(listSource<Order>([{ customer: "c1" }], 0))
        .keyBy((order) => order.customer)
        .process(counting)
        .to(before),
      state,
    );
    const after = listSink<number>();
    await run(
      StreamTopology.source(listSource<Order>([{ customer: "c1" }], 1))
        .keyBy((order) => order.customer)
        .process(counting, { name: "0" })
        .to(after),
      state,
    );
    expect(after.items).toEqual([2]);
  });

  test("unnamed stateful steps with durable state get a warning naming their ids", async () => {
    const warnings: string[] = [];
    await run(
      StreamTopology.source(listSource<Order>([], 0))
        .keyBy((order) => order.customer)
        .process(counting)
        .to(listSink()),
      durableState(),
      (message) => warnings.push(message),
    );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("process:0");
  });

  test("two steps can't share a name, and names are checked", async () => {
    const twice = StreamTopology.source(listSource<Order>([], 0))
      .keyBy((order) => order.customer)
      .process(counting, { name: "same" })
      .keyBy(() => "k")
      .process(counting, { name: "same" })
      .to(listSink());
    await expect(run(twice, durableState())).rejects.toThrow(/same/);

    const badName = StreamTopology.source(listSource<Order>([], 0))
      .keyBy((order) => order.customer)
      .process(counting, { name: "orders:total" })
      .to(listSink());
    await expect(run(badName, durableState())).rejects.toThrow(/may only use/);
  });
});
