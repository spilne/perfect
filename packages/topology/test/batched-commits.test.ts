import { describe, expect, test } from "bun:test";
import { async, succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type Envelope,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import { ConsumerGroup, StreamTopology, TopologyInstanceId, TopologyRunner } from "../src";

interface Order {
  readonly customer: string;
}
type Source = Streamable<Order> & Acknowledgeable<Order>;

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function envelope(order: Order, offset: number, events: string[]): Envelope<Order> {
  return {
    value: order,
    ack: () => sync(() => void events.push(`ack ${offset}`)),
    nack: () => succeed(undefined),
    metadata: { topic: "orders", partition: 0, offset: String(offset) },
  };
}

function listSource(envelopes: Envelope<Order>[]): Source {
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

// Counts commits (and can make each one slow, like a round trip to Redis).
function countingState(commitDelayMs = 0) {
  const state = new InMemoryPartitionedState<unknown>();
  const commits: number[] = [];
  const events: string[] = [];
  const commit = state.commit.bind(state);
  state.commit = async (params) => {
    if (commitDelayMs > 0) await new Promise((resolve) => setTimeout(resolve, commitDelayMs));
    const ids = (params.sourceIds ?? []).length + (params.sourceId ? 1 : 0);
    if (ids > 0) {
      commits.push(ids);
      events.push(`commit ${ids}`);
    }
    return commit(params);
  };
  return { state, commits, events };
}

const countPerCustomer = (source: Source, sink: Sinkable<number>) =>
  StreamTopology.source(source)
    .keyBy((order) => order.customer)
    .process(
      { init: () => 0, process: (count: number) => ({ state: count + 1, emit: count + 1 }) },
      { name: "orders" },
    )
    .to(sink);

async function runToEnd(
  topology: ReturnType<typeof countPerCustomer>,
  state: InMemoryPartitionedState<unknown>,
  batch: { ackBatchSize?: number; ackMaxWaitMs?: number },
): Promise<void> {
  const runner = await TopologyRunner.run(topology, {
    group: ConsumerGroup("orders"),
    partitionedStateBackend: state,
    instanceId: TopologyInstanceId(`run-${Math.random()}`),
    ...batch,
  });
  await runner.awaitExit();
  await runner.shutdown();
}

describe("batched commits (ackBatchSize)", () => {
  test("records are committed in batches, state comes out the same", async () => {
    const { state, commits, events } = countingState();
    const out = listSink<number>();
    const envelopes = Array.from({ length: 7 }, (_, i) => envelope({ customer: "c1" }, i, events));
    await runToEnd(countPerCustomer(listSource(envelopes), out), state, { ackBatchSize: 3 });

    expect(out.items).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(commits).toEqual([3, 3, 1]); // the last one when the input ended
    // Every record of a batch is acked only after the batch is committed.
    expect(events.slice(0, 4)).toEqual(["commit 3", "ack 0", "ack 1", "ack 2"]);
  });

  test("a batch that doesn't fill up is committed after ackMaxWaitMs", async () => {
    const { state, commits } = countingState();
    const events: string[] = [];
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    async function* slowSource(): AsyncGenerator<Envelope<Order>> {
      yield envelope({ customer: "c1" }, 0, events);
      yield envelope({ customer: "c1" }, 1, events);
      await held; // nothing more for a while
    }
    const source = {
      codec,
      subscribe: () => Stream.empty(),
      subscribeAck: () => Stream.fromAsyncIterable(slowSource(), (error) => error),
    } as unknown as Source;
    const runner = await TopologyRunner.run(countPerCustomer(source, listSink()), {
      group: ConsumerGroup("orders"),
      partitionedStateBackend: state,
      ackBatchSize: 100,
      ackMaxWaitMs: 50,
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(commits).toEqual([2]);
    expect(events).toEqual(["ack 0", "ack 1"]);
    release();
    await runner.awaitExit();
    await runner.shutdown();
  });

  test("a record delivered again inside a batch is counted once", async () => {
    const { state, events } = countingState();
    const out = listSink<number>();
    await runToEnd(
      countPerCustomer(
        listSource([
          envelope({ customer: "c1" }, 0, events),
          envelope({ customer: "c1" }, 0, events), // redelivered before the first copy committed
          envelope({ customer: "c1" }, 1, events),
        ]),
        out,
      ),
      state,
      { ackBatchSize: 10 },
    );
    expect(out.items).toEqual([1, 2]);
    expect(events).toEqual(["commit 2", "ack 0", "ack 0", "ack 1"]);
  });

  test("shutdown commits a batch that is still waiting", async () => {
    const { state, commits } = countingState();
    const events: string[] = [];
    // One record, then the source stays open (until it is interrupted).
    const source = {
      codec,
      subscribe: () => Stream.empty(),
      subscribeAck: () =>
        Stream.of(envelope({ customer: "c1" }, 0, events)).concat(
          Stream.fromEffect(async<Envelope<Order>>(() => {})),
        ),
    } as unknown as Source;
    const runner = await TopologyRunner.run(countPerCustomer(source, listSink()), {
      group: ConsumerGroup("orders"),
      partitionedStateBackend: state,
      ackBatchSize: 100,
      ackMaxWaitMs: 60_000,
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    await runner.shutdown();
    expect(commits).toEqual([1]);
    expect(events).toEqual(["ack 0"]);
  });

  test("with a slow store, batches are much faster", async () => {
    const timed = async (ackBatchSize: number) => {
      const { state } = countingState(2); // 2 ms per commit
      const envelopes = Array.from({ length: 200 }, (_, i) =>
        envelope({ customer: `c${i % 5}` }, i, []),
      );
      const started = performance.now();
      await runToEnd(countPerCustomer(listSource(envelopes), listSink()), state, { ackBatchSize });
      return performance.now() - started;
    };
    const oneByOne = await timed(1);
    const batched = await timed(50);
    expect(batched * 4).toBeLessThan(oneByOne);
  });
});
