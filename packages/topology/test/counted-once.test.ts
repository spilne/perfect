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

interface Click {
  readonly key: string;
  readonly ts: number;
}
type Source = Streamable<Click> & Acknowledgeable<Click>;

interface WindowCount {
  readonly key: string;
  readonly window: { readonly start: number; readonly end: number };
  readonly count: number;
}

const codec = { encode: (v: unknown) => v, decode: (v: unknown) => v };

// Each entry is [click, source offset]. The same offset twice is the same
// source record delivered again.
function listSource(records: Array<[Click, number]>, acked: number[] = []): Source {
  const envelopes: Envelope<Click>[] = records.map(([click, offset]) => ({
    value: click,
    ack: () => sync(() => void acked.push(offset)),
    nack: () => succeed(undefined),
    metadata: { topic: "clicks", partition: 0, offset: String(offset) },
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

describe("each source record counts once", () => {
  test("a record delivered again while its first copy is in flight is skipped", async () => {
    const acked: number[] = [];
    const out = listSink<number>();
    const runner = await TopologyRunner.run(
      StreamTopology.source(
        listSource(
          [
            [{ key: "k", ts: 0 }, 0],
            [{ key: "k", ts: 0 }, 0], // delivered again before the first copy committed
            [{ key: "k", ts: 1 }, 1],
          ],
          acked,
        ),
      )
        .keyBy((click) => click.key)
        .process({
          init: () => 0,
          process: (count: number) => ({ state: count + 1, emit: count + 1 }),
        })
        .to(out),
      { group: ConsumerGroup("counts"), partitionedStateBackend: new InMemoryPartitionedState() },
    );
    await runner.awaitExit();
    await runner.shutdown();

    expect(out.items).toEqual([1, 2]);
    expect(acked).toEqual([0, 0, 1]); // the second copy is still acked
  });

  // With batches, the commit happens later, after the failing record has
  // already changed the open window; it must not be saved with the batch.
  test.each([1, 100])(
    "state that was never committed doesn't come back after a restart (ackBatchSize %i)",
    async (ackBatchSize) => {
      const state = new InMemoryPartitionedState<unknown>();
      const clicks: Array<[Click, number]> = [
        [{ key: "k", ts: 0 }, 0],
        [{ key: "k", ts: 1_000 }, 1],
        [{ key: "k", ts: 2_000 }, 2],
        [{ key: "k", ts: 3_000 }, 3],
      ];
      const countWindows = <S>(sink: Sinkable<WindowCount, S>) =>
        StreamTopology.source(listSource(clicks))
          .keyBy((click) => click.key)
          .sliding({ windowMs: 2_000, slideMs: 1_000 })
          .count()
          .to(sink);
      const config = (instance: string) => ({
        group: ConsumerGroup("windows"),
        partitionedStateBackend: state,
        instanceId: TopologyInstanceId(instance),
        partitionLeaseMs: 2_000,
        ackBatchSize,
      });

      // The first run fails while publishing the window that the click at 2000
      // closes, so that click's changes are never committed.
      let publishes = 0;
      const failingSink = {
        codec,
        publish: () => (++publishes === 1 ? fail("sink down") : succeed(undefined)),
      } as unknown as Sinkable<WindowCount, Throws<string>>;
      const first = await TopologyRunner.run(countWindows(failingSink), config("first"));
      await first.awaitExit();
      await first.shutdown();

      // The restart reads every click again from the last committed one.
      const out = listSink<WindowCount>();
      const second = await TopologyRunner.run(countWindows(out), config("second"));
      await second.awaitExit();
      await second.shutdown();

      const window1000 = out.items.find((w) => w.window.start === 1_000);
      expect(window1000?.count).toBe(2); // the clicks at 1000 and 2000, not 3
    },
  );
});
