import { describe, expect, test } from "bun:test";
import { succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  Partition,
  type Acknowledgeable,
  type Envelope,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import { ConsumerGroup, StreamTopology, TopologyInstanceId, TopologyRunner } from "../src";

interface Click {
  readonly key: string;
  readonly ts: number;
}

interface PartitionEvents {
  assigned(params: { partitions: readonly Partition[] }): Promise<void>;
  revoking(params: { partitions: readonly Partition[] }): Promise<void>;
}

type ClickSource = Streamable<Click> & Acknowledgeable<Click>;

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

// State that is copied on the way in and out, like Redis or Postgres store
// it. The plain in-memory store hands back the same objects, which hides
// bugs where a runner keeps using its own stale copy.
function serializingState(): InMemoryPartitionedState<unknown> {
  const state = new InMemoryPartitionedState<unknown>();
  const load = state.load.bind(state);
  const commit = state.commit.bind(state);
  state.load = async (lease) => {
    const loaded = await load(lease);
    return loaded && { ...loaded, values: new Map(structuredClone([...loaded.values])) };
  };
  state.commit = async (params) =>
    commit({ ...params, mutations: structuredClone(params.mutations) });
  return state;
}

function clickEnvelope(click: Click, offset: number, acked: number[]): Envelope<Click> {
  return {
    value: click,
    ack: () => sync(() => void acked.push(offset)),
    nack: () => succeed(undefined),
    metadata: { topic: "clicks", partition: 0, offset: String(offset) },
  };
}

// A managed source driven by the test: it hands out envelopes and tells the
// runner about partition assignment, in the order the test asks for.
function scriptedSource() {
  const steps: Array<Envelope<Click> | (() => Promise<void>) | "end"> = [];
  let wake: (() => void) | undefined;
  let events: PartitionEvents | undefined;

  async function* envelopes(): AsyncGenerator<Envelope<Click>> {
    while (true) {
      while (steps.length === 0) await new Promise<void>((resolve) => (wake = resolve));
      const step = steps.shift()!;
      if (step === "end") return;
      if (typeof step === "function") await step();
      else yield step;
    }
  }
  const push = (step: (typeof steps)[number]) => {
    steps.push(step);
    wake?.();
  };

  const source = {
    codec,
    subscribe: () => Stream.empty(),
    subscribeAck: () => Stream.empty(),
    subscribeAckManaged: () => ({
      stream: Stream.fromAsyncIterable(envelopes(), (error) => error),
      setPartitionLifecycle: (value: PartitionEvents) => {
        events = value;
      },
      close: async () => {},
    }),
  };
  return {
    source: source as unknown as ClickSource,
    assign: () => push(() => events!.assigned({ partitions: [Partition(0)] })),
    revoke: () => push(() => events!.revoking({ partitions: [Partition(0)] })),
    send: (envelope: Envelope<Click>) => push(envelope),
    end: () => push("end"),
  };
}

function listSource(envelopes: Envelope<Click>[]): ClickSource {
  return {
    codec,
    subscribe: () => Stream.empty(),
    subscribeAck: () => Stream.fromIterable(envelopes),
  } as unknown as ClickSource;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

interface ClickCount {
  readonly key: string;
  readonly window: { readonly start: number; readonly end: number };
  readonly count: number;
}

const countPerSecond = (source: ClickSource, sink: Sinkable<ClickCount>) =>
  StreamTopology.source(source)
    .keyBy((click) => click.key)
    .tumbling(1_000)
    .count()
    .to(sink);

const settle = () => new Promise((resolve) => setTimeout(resolve, 100));

describe("partitions moving between instances", () => {
  test("a partition that comes back uses the state the other instance left", async () => {
    const state = serializingState();
    const config = (instance: string) => ({
      group: ConsumerGroup("clicks"),
      partitionedStateBackend: state,
      instanceId: TopologyInstanceId(instance),
      partitionLeaseMs: 2_000,
    });
    const acked: number[] = [];
    const outA = listSink<ClickCount>();
    const outB = listSink<ClickCount>();

    const a = scriptedSource();
    const runnerA = await TopologyRunner.run(countPerSecond(a.source, outA), config("A"));
    a.assign();
    a.send(clickEnvelope({ key: "k", ts: 0 }, 0, acked));
    a.revoke();
    await settle();

    // Instance B owns the partition for a while and adds two clicks to the
    // same window.
    const runnerB = await TopologyRunner.run(
      countPerSecond(
        listSource([
          clickEnvelope({ key: "k", ts: 100 }, 1, acked),
          clickEnvelope({ key: "k", ts: 200 }, 2, acked),
        ]),
        outB,
      ),
      config("B"),
    );
    await runnerB.awaitExit();
    await runnerB.shutdown();

    // The partition comes back to A; a later click closes the window.
    a.assign();
    a.send(clickEnvelope({ key: "k", ts: 1_500 }, 3, acked));
    await settle();
    a.end();
    await runnerA.shutdown();

    expect(outA.items).toEqual([{ key: "k", window: { start: 0, end: 1_000 }, count: 3 }]);
    expect(outB.items).toEqual([]);
  });

  test("a record that arrives after its partition was taken away is left for the new owner", async () => {
    const state = serializingState();
    const config = (instance: string) => ({
      group: ConsumerGroup("late-record"),
      partitionedStateBackend: state,
      instanceId: TopologyInstanceId(instance),
      partitionLeaseMs: 2_000,
    });
    const acked: number[] = [];
    const outA = listSink<Click>();
    const outB = listSink<Click>();

    const a = scriptedSource();
    const runnerA = await TopologyRunner.run(StreamTopology.source(a.source).to(outA), config("A"));
    a.assign();
    a.send(clickEnvelope({ key: "k", ts: 0 }, 0, acked));
    a.revoke();
    // Fetched before the partition moved, so it still reaches A:
    a.send(clickEnvelope({ key: "k", ts: 1 }, 1, acked));
    await settle();

    // B takes the partition over and gets the record redelivered. (Before,
    // A took the lease back for that record and B failed with "owned by
    // another instance".)
    const runnerB = await TopologyRunner.run(
      StreamTopology.source(listSource([clickEnvelope({ key: "k", ts: 1 }, 1, acked)])).to(outB),
      config("B"),
    );
    const exits = await runnerB.awaitExit();
    await runnerB.shutdown();
    a.end();
    await runnerA.shutdown();

    expect(exits.every((exit) => exit._tag === "Success")).toBe(true);
    expect(outA.items).toEqual([{ key: "k", ts: 0 }]);
    expect(outB.items).toEqual([{ key: "k", ts: 1 }]);
    expect(acked).toEqual([0, 1]);
  });
});
