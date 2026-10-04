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
import { ConsumerGroup, StreamTopology, TopologyInstanceId, TopologyRunner } from "../src";

type Source = Streamable<number> & Acknowledgeable<number>;

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function listSource(values: number[], acked: number[] = []): Source {
  const envelopes: Envelope<number>[] = values.map((value, offset) => ({
    value,
    ack: () => sync(() => void acked.push(offset)),
    nack: () => succeed(undefined),
    metadata: { topic: "numbers", partition: 0, offset: String(offset) },
  }));
  return {
    codec,
    subscribe: () => Stream.fromIterable(values),
    subscribeAck: () => Stream.fromIterable(envelopes),
  } as unknown as Source;
}

function listSink(): Sinkable<number> & { items: number[] } {
  const items: number[] = [];
  return { items, codec, publish: (value: number) => sync(() => void items.push(value)) } as never;
}

const leaseMs = 3_000;
const config = (state: InMemoryPartitionedState<unknown>, instance: string) => ({
  group: ConsumerGroup("numbers"),
  partitionedStateBackend: state,
  instanceId: TopologyInstanceId(instance),
  partitionLeaseMs: leaseMs,
});

// Another instance can take the partition over right away: it runs the same
// topology to the end without "owned by another instance".
async function partitionIsFree(state: InMemoryPartitionedState<unknown>): Promise<boolean> {
  const runner = await TopologyRunner.run(
    StreamTopology.source(listSource([9])).to(listSink()),
    config(state, "next-owner"),
  );
  const exits = await runner.awaitExit();
  await runner.shutdown();
  return exits.every((exit) => exit._tag === "Success");
}

describe("stopping a topology", () => {
  test("shutdown doesn't wait for work it has just interrupted", async () => {
    const state = new InMemoryPartitionedState<unknown>();
    const acked: number[] = [];
    const runner = await TopologyRunner.run(
      StreamTopology.source(listSource([1, 2, 3], acked))
        .mapAsync(2, async (n) => {
          await new Promise((resolve) => setTimeout(resolve, 10_000));
          return n;
        })
        .to(listSink()),
      config(state, "A"),
    );
    await new Promise((resolve) => setTimeout(resolve, 50));

    const started = Date.now();
    await runner.shutdown(); // used to wait the whole lease, then throw
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(acked).toEqual([]); // the interrupted records are delivered again later
    expect(await partitionIsFree(state)).toBe(true);
  });

  test("after a sink failure, shutdown returns promptly and frees the partition", async () => {
    const state = new InMemoryPartitionedState<unknown>();
    const sink: Sinkable<number, Throws<Error>> = {
      codec,
      publish: (value) => (value === 2 ? fail(new Error("sink down")) : succeed(undefined)),
    } as Sinkable<number, Throws<Error>>;
    const runner = await TopologyRunner.run(
      StreamTopology.source(listSource([1, 2, 3]))
        .mapAsync(2, async (n) => n)
        .to(sink),
      config(state, "A"),
    );

    const exits = await runner.awaitExit();
    const started = Date.now();
    await runner.shutdown();
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(exits.some((exit) => exit._tag === "Failure")).toBe(true);
    expect(await partitionIsFree(state)).toBe(true);
  });
});
