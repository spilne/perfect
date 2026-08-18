import { describe, expect, test } from "bun:test";
import { run } from "@perfect/core";
import { RedisStream } from "../src/redis-stream";
import type { RedisClient } from "../src/redis-client";

interface FakeStreamState {
  readonly acknowledgements: string[][];
  readonly groups: unknown[][];
  readonly additions: unknown[][];
}

function fakeStreamClient(batches: Array<Array<{ id: string; data: unknown; key?: string }>>): {
  client: RedisClient;
  state: FakeStreamState;
} {
  let readIndex = 0;
  let groupExists = false;
  const state: FakeStreamState = {
    acknowledgements: [],
    groups: [],
    additions: [],
  };

  const client: Partial<RedisClient> = {
    async xgroup(...args) {
      state.groups.push(args);
      if (args[0] === "CREATE") {
        if (groupExists) throw new Error("BUSYGROUP Consumer Group name already exists");
        groupExists = true;
      }
      return "OK";
    },
    async xreadgroup(...args) {
      if (readIndex >= batches.length) {
        await Bun.sleep(25);
        return null;
      }
      const stream = String(args[args.indexOf("STREAMS") + 1]);
      const batch = batches[readIndex++]!;
      return [
        [
          stream,
          batch.map((message) => {
            const fields = ["data", JSON.stringify(message.data)];
            if (message.key !== undefined) fields.push("key", message.key);
            return [message.id, fields];
          }),
        ],
      ];
    },
    async xack(_stream, _group, ...ids) {
      state.acknowledgements.push(ids);
      return ids.length;
    },
    async xadd(...args) {
      state.additions.push(args);
      return "1-0";
    },
    duplicate() {
      return client as RedisClient;
    },
    disconnect() {},
  };

  return { client: client as RedisClient, state };
}

describe("RedisStream", () => {
  test("auto-ack subscription emits a read batch and acknowledges it together", async () => {
    const { client, state } = fakeStreamClient([
      [
        { id: "1-0", data: { n: 1 } },
        { id: "1-1", data: { n: 2 } },
        { id: "1-2", data: { n: 3 } },
      ],
    ]);
    const stream = RedisStream.make<{ n: number }>({ redis: client, stream: "events", group: "g" });

    const values = await run(stream.subscribe().take(3).toArray());
    await Bun.sleep(5);

    expect(values).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    expect(state.acknowledgements).toEqual([["1-0", "1-1", "1-2"]]);
  });

  test("manual subscription exposes durable ids and only acknowledges on ack", async () => {
    const { client, state } = fakeStreamClient([
      [
        { id: "2-0", data: { n: 1 }, key: "account-1" },
        { id: "2-1", data: { n: 2 } },
      ],
    ]);
    const stream = RedisStream.make<{ n: number }>({ redis: client, stream: "events", group: "g" });

    const envelopes = await run(stream.subscribeAck().take(2).toArray());
    expect(state.acknowledgements).toEqual([]);
    expect(envelopes[0]?.metadata).toMatchObject({ id: "2-0", key: "account-1" });

    await envelopes[1]!.ack();
    expect(state.acknowledgements).toEqual([["2-1"]]);
  });

  test("replay offsets reset an existing consumer group", async () => {
    const { client, state } = fakeStreamClient([]);
    const stream = RedisStream.make<number>({ redis: client, stream: "events", group: "g" });

    await stream.ensureGroup();
    await stream.ensureGroup({ offset: { type: "specific", value: "42-7" } });

    expect(state.groups).toEqual([
      ["CREATE", "events", "g", "0-0", "MKSTREAM"],
      ["CREATE", "events", "g", "42-7", "MKSTREAM"],
      ["SETID", "events", "g", "42-7"],
    ]);
  });

  test("publish preserves an optional routing key", async () => {
    const { client, state } = fakeStreamClient([]);
    const stream = RedisStream.make<{ n: number }>({ redis: client, stream: "events", group: "g" });

    await stream.publish({ n: 1 }, { key: "account-1" });

    expect(state.additions).toEqual([
      ["events", "*", "data", JSON.stringify({ n: 1 }), "key", "account-1"],
    ]);
  });
});
