import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
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
  StageId,
  StreamTopology,
  TopologyId,
  TopologyInstanceId,
  TopologyRunner,
} from "../src";
import { Partition } from "@spilne/perfect-core/connect";

interface Order {
  readonly customer: string;
  readonly amount: number;
  readonly ts: number;
}
interface Customer {
  readonly customer: string;
  readonly name: string;
  readonly ts: number;
}
interface Joined {
  readonly left: Order;
  readonly right: Customer;
}

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function listSource<T>(topic: string, values: T[]): Streamable<T> & Acknowledgeable<T> {
  const envelopes: Envelope<T>[] = values.map((value, offset) => ({
    value,
    ack: () => succeed(undefined),
    nack: () => succeed(undefined),
    metadata: { topic, partition: 0, offset: String(offset) },
  }));
  return {
    codec,
    subscribe: () => Stream.empty(),
    subscribeAck: () => Stream.fromIterable(envelopes),
  } as unknown as Streamable<T> & Acknowledgeable<T>;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

// Records the state keys each commit writes.
function recordingState() {
  const state = new InMemoryPartitionedState<unknown>();
  const writes: string[][] = [];
  const commit = state.commit.bind(state);
  state.commit = async (params) => {
    if (params.mutations.length > 0) writes.push(params.mutations.map((m) => m.key));
    return commit(params);
  };
  return { state, writes };
}

const joinTopology = (orders: Order[], customers: Customer[], sink: Sinkable<Joined>) =>
  StreamTopology.source(listSource("orders", orders))
    .keyBy((order) => order.customer)
    .join(
      StreamTopology.source(listSource("customers", customers)).keyBy((c) => c.customer),
      { windowMs: 60_000 },
    )
    .to(sink);

async function runToEnd(
  topology: ReturnType<typeof joinTopology>,
  state: InMemoryPartitionedState<unknown>,
  instance = "A",
): Promise<void> {
  const runner = await TopologyRunner.run(topology, {
    group: ConsumerGroup("joins"),
    partitionedStateBackend: state,
    instanceId: TopologyInstanceId(instance),
  });
  await runner.awaitExit();
  await runner.shutdown();
}

describe("join state", () => {
  test("each record saves only its own key", async () => {
    const { state, writes } = recordingState();
    const out = listSink<Joined>();
    const orders = ["a", "b", "c"].map((customer, i) => ({ customer, amount: i, ts: 1_000 + i }));
    await runToEnd(joinTopology(orders, [], out), state);

    const joinWrites = writes.map((keys) => keys.filter((key) => key.includes("join")));
    expect(joinWrites).toEqual([
      [expect.stringMatching(/:key:a$/)],
      [expect.stringMatching(/:key:b$/)],
      [expect.stringMatching(/:key:c$/)],
    ]);
  });

  test("buffered items survive a restart", async () => {
    const state = new InMemoryPartitionedState<unknown>();
    const first = listSink<Joined>();
    await runToEnd(joinTopology([{ customer: "a", amount: 5, ts: 1_000 }], [], first), state);
    expect(first.items).toEqual([]);

    // The customer arrives after the restart and still finds the order.
    const second = listSink<Joined>();
    await runToEnd(
      joinTopology([], [{ customer: "a", name: "Alice", ts: 2_000 }], second),
      state,
      "B",
    );
    expect(second.items).toEqual([
      {
        left: { customer: "a", amount: 5, ts: 1_000 },
        right: { customer: "a", name: "Alice", ts: 2_000 },
      },
    ]);
  });

  test("state saved in the old one-entry format is still read, then moved", async () => {
    const { state, writes } = recordingState();
    // What older versions saved: the whole buffer under the operator's id.
    const lease = await state.acquire({
      scope: {
        topologyId: TopologyId("joins"),
        stageId: StageId("stage-0"),
        partition: Partition(0),
      },
      ownerId: TopologyInstanceId("old"),
      leaseMs: 1_000,
    });
    await state.commit({
      lease: lease!,
      mutations: [
        {
          type: "put",
          key: "join:0",
          value: {
            left: [["a", [{ value: { customer: "a", amount: 5, ts: 1_000 }, timestamp: 1_000 }]]],
            right: [],
          },
        },
      ],
    });
    await state.release(lease!);
    writes.length = 0;

    const out = listSink<Joined>();
    await runToEnd(joinTopology([], [{ customer: "a", name: "Alice", ts: 2_000 }], out), state);

    expect(out.items).toHaveLength(1);
    // The old entry is removed and the key gets its own entry.
    expect(writes[0]).toEqual(expect.arrayContaining(["join:0", expect.stringMatching(/:key:a$/)]));
  });

  test("many keys stay fast", async () => {
    // Saving the whole buffer on every record took about 3 s for 2 000 records
    // and 50 s for 8 000.
    const orders = Array.from({ length: 4_000 }, (_, i) => ({
      customer: `c${i}`,
      amount: i,
      ts: 1_000 + i,
    }));
    const started = performance.now();
    await runToEnd(joinTopology(orders, [], listSink()), new InMemoryPartitionedState());
    expect(performance.now() - started).toBeLessThan(3_000);
  });
});
