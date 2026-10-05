import { describe, expect, test } from "bun:test";
import { fail, succeed, sync, type Throws } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type Envelope,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import {
  ConsumerGroup,
  StreamTopology,
  TopologyInstanceId,
  TopologyRunner,
  type BuiltTopology,
  type DeadLetter,
  type TopologyMetrics,
} from "../src";

interface Order {
  readonly id: string;
  readonly amount: number | string; // a string amount is the bad record
  readonly ts: number;
}
type Source = Streamable<Order> & Acknowledgeable<Order>;

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function listSource(orders: Order[], acked: string[] = [], firstOffset = 0): Source {
  const envelopes: Envelope<Order>[] = orders.map((order, i) => ({
    value: order,
    ack: () => sync(() => void acked.push(order.id)),
    nack: () => succeed(undefined),
    metadata: { topic: "orders", partition: 0, offset: String(firstOffset + i) },
  }));
  return {
    codec,
    subscribe: () => Stream.empty(),
    subscribeAck: () => Stream.fromIterable(envelopes),
  } as unknown as Source;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

const orders: Order[] = [
  { id: "o1", amount: 10, ts: 100 },
  { id: "o2", amount: "ten", ts: 200 },
  { id: "o3", amount: 30, ts: 300 },
];

const cents = (order: Order): number => {
  if (typeof order.amount !== "number") throw new TypeError(`bad amount in ${order.id}`);
  return order.amount * 100;
};

async function run(
  topology: BuiltTopology,
  deadLetter?: Sinkable<DeadLetter, unknown>,
  state = new InMemoryPartitionedState<unknown>(),
): Promise<{ ok: boolean; metrics: TopologyMetrics }> {
  const runner = await TopologyRunner.run(topology, {
    group: ConsumerGroup("orders"),
    partitionedStateBackend: state,
    instanceId: TopologyInstanceId(`run-${Math.random()}`),
    deadLetter,
  });
  const exits = await runner.awaitExit();
  const metrics = runner.metrics();
  await runner.shutdown();
  return { ok: exits.every((exit) => exit._tag === "Success"), metrics };
}

describe("dead letters", () => {
  test("a record a step throws on goes to the dead-letter sink; the rest carry on", async () => {
    const acked: string[] = [];
    const out = listSink<number>();
    const dead = listSink<DeadLetter>();
    const { ok, metrics } = await run(
      StreamTopology.source(listSource(orders, acked)).map(cents).to(out),
      dead,
    );

    expect(ok).toBe(true);
    expect(out.items).toEqual([1_000, 3_000]);
    expect(dead.items).toHaveLength(1);
    expect(dead.items[0]).toMatchObject({
      value: { id: "o2", amount: "ten", ts: 200 },
      error: { name: "TypeError", message: "bad amount in o2" },
      source: { topic: "orders", partition: 0, offset: "1" },
    });
    expect(acked).toEqual(["o1", "o2", "o3"]); // the bad record is acked too
    expect(metrics.deadLetters).toBe(1);
  });

  test("without a dead-letter sink, the error still stops the topology", async () => {
    const { ok } = await run(StreamTopology.source(listSource(orders)).map(cents).to(listSink()));
    expect(ok).toBe(false);
  });

  test("works in mapAsync, process, windows and joins", async () => {
    const asyncDead = listSink<DeadLetter>();
    await run(
      StreamTopology.source(listSource(orders))
        .mapAsync(2, async (order) => cents(order))
        .to(listSink()),
      asyncDead,
    );

    const processDead = listSink<DeadLetter>();
    await run(
      StreamTopology.source(listSource(orders))
        .keyBy((order) => order.id)
        .process(
          { init: () => 0, process: (total: number, o: Order) => ({ state: total + cents(o) }) },
          { name: "totals" },
        )
        .to(listSink()),
      processDead,
    );

    const windowDead = listSink<DeadLetter>();
    await run(
      StreamTopology.source(listSource(orders))
        .keyBy(() => "all")
        .tumbling(1_000)
        .sum(cents)
        .to(listSink()),
      windowDead,
    );

    const joinDead = listSink<DeadLetter>();
    await run(
      StreamTopology.source(listSource(orders))
        .keyBy((order) => {
          if (order.id === "o2") throw new Error("no key");
          return order.id;
        })
        .join(
          StreamTopology.source(listSource([])).keyBy((order) => order.id),
          { windowMs: 1_000 },
        )
        .to(listSink()),
      joinDead,
    );

    for (const dead of [asyncDead, processDead, windowDead, joinDead]) {
      expect(dead.items.map((letter) => (letter.value as Order).id)).toEqual(["o2"]);
    }
  });

  test("a failing dead-letter sink stops the topology", async () => {
    const broken = {
      codec,
      publish: () => fail(new Error("dead-letter queue down")),
    } as unknown as Sinkable<DeadLetter, Throws<Error>>;
    const { ok } = await run(
      StreamTopology.source(listSource(orders)).map(cents).to(listSink()),
      broken,
    );
    expect(ok).toBe(false);
  });

  test("a dead-lettered record isn't processed again after a restart", async () => {
    const state = new InMemoryPartitionedState<unknown>();
    const dead = listSink<DeadLetter>();
    const topology = () => StreamTopology.source(listSource(orders)).map(cents).to(listSink());
    await run(topology(), dead, state);
    await run(topology(), dead, state); // the same records again, already committed
    expect(dead.items).toHaveLength(1);
  });
});
