// A partition whose lease another instance still holds must not hold up the
// other partitions.

import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import { Partition, type Envelope } from "@spilne/perfect-core/connect";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import { ConsumerGroup, StreamTopology, TopologyRunner } from "../src";

type Item = { readonly id: string };

/** Records in this order, each on its own partition and offset. */
function sourceOf(
  records: { id: string; partition: number }[],
): Streamable<Item> & Acknowledgeable<Item> {
  return {
    codec: { encode: (v) => JSON.stringify(v), decode: (raw) => JSON.parse(raw as string) },
    subscribe: () => Stream.fromIterable(records.map(({ id }) => ({ id }))),
    subscribeAck: () =>
      Stream.fromIterable(
        records.map(({ id, partition }, offset) => ({
          value: { id },
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: { topic: "items", partition, offset: String(offset) },
        })),
      ),
  };
}

function sinkOf(): Sinkable<string> & { items: string[] } {
  const items: string[] = [];
  return {
    items,
    codec: { encode: (v) => v, decode: (raw) => raw as string },
    publish: (value) => sync(() => void items.push(value)),
  };
}

/** State whose partition 0 another instance holds until `free()` is called. */
function stateWithPartitionZeroTaken() {
  const state = new InMemoryPartitionedState();
  let taken = true;
  const acquire = state.acquire.bind(state);
  state.acquire = async (params) =>
    taken && params.scope.partition === 0 ? undefined : acquire(params);
  return { state, free: () => (taken = false) };
}

const settle = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("a partition another instance still holds", () => {
  test("doesn't hold up the other partitions, and its records follow in order", async () => {
    const { state, free } = stateWithPartitionZeroTaken();
    const sink = sinkOf();
    const topology = StreamTopology.source(
      sourceOf([
        { id: "a", partition: 0 },
        { id: "b", partition: 1 },
        { id: "c", partition: 0 },
        { id: "d", partition: 1 },
      ]),
    )
      .map((item) => item.id)
      .to(sink);

    const handle = await TopologyRunner.run(topology, {
      group: ConsumerGroup("contested"),
      partitionedStateBackend: state,
      partitionLeaseMs: 5_000,
    });
    await settle(100);
    // Partition 1 went ahead while partition 0 waits for its lease.
    expect(sink.items).toEqual(["b", "d"]);

    free();
    await handle.awaitExit();
    expect(sink.items).toEqual(["b", "d", "a", "c"]);
    await handle.shutdown();
  });

  test("if the lease never comes, the topology fails after leaseMs", async () => {
    const { state } = stateWithPartitionZeroTaken();
    const sink = sinkOf();
    const topology = StreamTopology.source(
      sourceOf([
        { id: "a", partition: 0 },
        { id: "b", partition: 1 },
      ]),
    )
      .map((item) => item.id)
      .to(sink);

    const handle = await TopologyRunner.run(topology, {
      group: ConsumerGroup("contested-forever"),
      partitionedStateBackend: state,
      partitionLeaseMs: 200,
    });
    const exits = await handle.awaitExit();
    expect(exits.map((exit) => exit._tag)).toEqual(["Failure"]);
    const cause = exits[0]?._tag === "Failure" ? exits[0].cause : undefined;
    expect(cause?._tag === "Die" && String(cause.defect)).toContain(
      "partition 0 is owned by another instance",
    );
    expect(sink.items).toEqual(["b"]);
    await handle.shutdown().catch(() => undefined);
  });
});

describe("with a source that assigns partitions (like Kafka)", () => {
  test("records of a partition being taken over wait; the others don't", async () => {
    const { state, free } = stateWithPartitionZeroTaken();
    const sink = sinkOf();
    let events:
      | { assigned(params: { partitions: readonly Partition[] }): Promise<void> }
      | undefined;
    const envelope = (id: string, partition: number, offset: number): Envelope<Item> => ({
      value: { id },
      ack: () => succeed(undefined),
      nack: () => succeed(undefined),
      metadata: { topic: "items", partition, offset: String(offset) },
    });
    let release: () => void = () => {};
    const ended = new Promise<void>((resolve) => (release = resolve));
    async function* records(): AsyncGenerator<Envelope<Item>> {
      // The assignment starts, and records arrive before it has finished.
      void events!.assigned({ partitions: [Partition(0), Partition(1)] });
      yield envelope("a", 0, 0);
      yield envelope("b", 1, 0);
      yield envelope("c", 0, 1);
      yield envelope("d", 1, 1);
      await ended;
    }
    const source = {
      codec: { encode: (v: unknown) => v, decode: (v: unknown) => v },
      subscribe: () => Stream.empty(),
      subscribeAck: () => Stream.empty(),
      subscribeAckManaged: () => ({
        stream: Stream.fromAsyncIterable(records(), (error) => error),
        setPartitionLifecycle: (value: typeof events) => {
          events = value;
        },
        close: async () => {},
      }),
    } as unknown as Streamable<Item> & Acknowledgeable<Item>;

    const handle = await TopologyRunner.run(
      StreamTopology.source(source)
        .map((item) => item.id)
        .to(sink),
      {
        group: ConsumerGroup("contested-managed"),
        partitionedStateBackend: state,
        partitionLeaseMs: 5_000,
      },
    );
    await settle(100);
    expect(sink.items).toEqual(["b", "d"]);

    free();
    await settle(100);
    expect(sink.items).toEqual(["b", "d", "a", "c"]);
    release();
    await handle.shutdown();
  });
});
