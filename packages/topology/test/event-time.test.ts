import { describe, expect, test } from "bun:test";
import { Cause, succeed, sync } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import {
  InMemoryPartitionedState,
  type Acknowledgeable,
  type ShuffleTransport,
  type Sinkable,
  type Streamable,
} from "@spilne/perfect-core/connect";
import { ConsumerGroup, DistributedRunner, StreamTopology, TopologyRunner } from "../src";

interface Click {
  readonly user: string;
  readonly at: string; // an ISO date, not one of the field names read by default
}

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

function listSource<T>(values: T[]): Streamable<T> & Acknowledgeable<T> {
  return {
    codec,
    subscribe: () => Stream.fromIterable(values),
    subscribeAck: () =>
      Stream.fromIterable(
        values.map((value, offset) => ({
          value,
          ack: () => succeed(undefined),
          nack: () => succeed(undefined),
          metadata: { topic: "clicks", partition: 0, offset: String(offset) },
        })),
      ),
  } as unknown as Streamable<T> & Acknowledgeable<T>;
}

function listSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return { items, codec, publish: (value: T) => sync(() => void items.push(value)) } as never;
}

const clicks: Click[] = [
  { user: "ann", at: "2026-01-01T00:00:00.100Z" },
  { user: "ann", at: "2026-01-01T00:00:00.900Z" },
  { user: "ann", at: "2026-01-01T00:00:01.500Z" },
];
const start = Date.parse("2026-01-01T00:00:00.000Z");

describe("eventTime()", () => {
  test("windows use it, even after a map that drops the field", async () => {
    const out = listSink<{ key: string; window: { start: number; end: number }; count: number }>();
    const runner = await TopologyRunner.run(
      StreamTopology.source(listSource(clicks))
        .eventTime((click) => Date.parse(click.at))
        .map((click) => ({ user: click.user })) // the time is no longer in the value
        .keyBy((click) => click.user)
        .tumbling(1_000)
        .count()
        .to(out),
      {
        group: ConsumerGroup("event-time"),
        partitionedStateBackend: new InMemoryPartitionedState(),
      },
    );
    await runner.awaitExit();
    await runner.shutdown();

    expect(out.items).toEqual([
      { key: "ann", window: { start, end: start + 1_000 }, count: 2 },
      // emitted when the source ends
      { key: "ann", window: { start: start + 1_000, end: start + 2_000 }, count: 1 },
    ]);
  });

  test("a time that isn't a number fails the topology clearly", async () => {
    const runner = await TopologyRunner.run(
      StreamTopology.source(listSource(clicks))
        .eventTime((click) => Date.parse(`not ${click.at}`))
        .keyBy((click) => click.user)
        .tumbling(1_000)
        .count()
        .to(listSink()),
      { group: ConsumerGroup("bad-time"), partitionedStateBackend: new InMemoryPartitionedState() },
    );
    const exits = await runner.awaitExit();
    await runner.shutdown();
    const failure = exits.find((exit) => exit._tag === "Failure");
    expect(failure?._tag === "Failure" && String(Cause.squash(failure.cause))).toContain(
      "eventTime() must return milliseconds",
    );
  });

  test("DistributedRunner asks for eventTime() after the shuffle", async () => {
    const transport: ShuffleTransport<unknown, unknown> = {
      getOrCreateRepartitionChannel: async (params) =>
        ({
          source: listSource([]),
          sink: { codec: params.codec, publish: () => succeed(undefined) },
        }) as never,
    };
    const topology = StreamTopology.source(listSource(clicks))
      .eventTime((click) => Date.parse(click.at))
      .keyBy((click) => click.user)
      .shuffle()
      .tumbling(1_000)
      .count()
      .to(listSink());
    await expect(
      DistributedRunner.run(topology, {
        group: ConsumerGroup("event-time-shuffle"),
        shuffleTransport: transport,
        partitionedStateBackend: new InMemoryPartitionedState(),
      }),
    ).rejects.toThrow(/after shuffle/);
  });
});
