import { describe, expect, test } from "bun:test";
import { run } from "@spilne/perfect-core";
import { KafkaTopic, formatOffsetMap, parseOffsetMap } from "../src/kafka-topic";
import type { KafkaClient, KafkaOffsetCommit } from "../src/kafka-types";
import { GroupId, TopicName } from "../src/brands";

const topic = TopicName("orders");
const group = GroupId("billing");

function fakeKafka(opts: {
  partitions: number;
  committed?: Record<number, string>;
  failFetch?: boolean;
}) {
  const commits: KafkaOffsetCommit[][] = [];
  let adminConnections = 0;
  let producersMade = 0;
  const client = {
    admin: () => ({
      connect: async () => {
        adminConnections++;
      },
      disconnect: async () => {
        adminConnections--;
      },
      fetchTopicPartitionCount: async () => opts.partitions,
      fetchOffsets: async () => {
        if (opts.failFetch) throw new Error("broker down");
        return [
          {
            topic,
            partitions: Object.entries(opts.committed ?? {}).map(([partition, offset]) => ({
              partition: Number(partition),
              offset,
            })),
          },
        ];
      },
    }),
    consumer: () => ({
      connect: async () => {},
      disconnect: async () => {},
      commitOffsets: async (offsets: KafkaOffsetCommit[]) => {
        commits.push(offsets);
      },
    }),
    producer: () => {
      producersMade++;
      return {
        connect: () => new Promise<void>((r) => setTimeout(r, 5)),
        disconnect: async () => {},
        send: async () => {},
      };
    },
  } as unknown as KafkaClient;
  return {
    client,
    commits,
    adminConnections: () => adminConnections,
    producersMade: () => producersMade,
  };
}

const makeTopic = (client: KafkaClient) =>
  new KafkaTopic<unknown>({ kafka: client, topic, groupId: group });

describe("offset text", () => {
  test("round-trips a partition map", () => {
    expect(formatOffsetMap({ 0: "42", 1: "17" })).toBe("0:42,1:17");
    expect(parseOffsetMap("0:42,1:17")).toEqual({ 0: "42", 1: "17" });
    expect(() => parseOffsetMap("x:1")).toThrow();
  });
});

describe("KafkaTopic offsets on a topic with several partitions", () => {
  test("getCommittedOffset reports every partition, not just partition 0", async () => {
    const fake = fakeKafka({ partitions: 2, committed: { 0: "5", 1: "9" } });
    expect(await makeTopic(fake.client).getCommittedOffset({ group })).toBe("0:5,1:9");
  });

  test("commitOffset commits each partition in a partition map", async () => {
    const fake = fakeKafka({ partitions: 2 });
    await makeTopic(fake.client).commitOffset({ group, offset: "0:5,1:9" });
    expect<unknown>(fake.commits[0]!.map((c) => [c.partition, c.offset])).toEqual([
      [0, "5"],
      [1, "9"],
    ]);
  });

  test("a single offset is refused instead of silently going to partition 0", async () => {
    const fake = fakeKafka({ partitions: 3 });
    await expect(makeTopic(fake.client).commitOffset({ group, offset: "5" })).rejects.toThrow(
      /3 partitions/,
    );
    expect(fake.commits).toEqual([]);
  });
});

describe("KafkaTopic offsets on a single-partition topic", () => {
  test("keep the plain offset form", async () => {
    const fake = fakeKafka({ partitions: 1, committed: { 0: "7" } });
    const kt = makeTopic(fake.client);
    expect(await kt.getCommittedOffset({ group })).toBe("7");
    await kt.commitOffset({ group, offset: "8" });
    expect<unknown>(fake.commits[0]!.map((c) => [c.partition, c.offset])).toEqual([[0, "8"]]);
  });

  test("nothing committed yet is null", async () => {
    const fake = fakeKafka({ partitions: 1, committed: { 0: "-1" } });
    expect(await makeTopic(fake.client).getCommittedOffset({ group })).toBeNull();
  });
});

describe("KafkaTopic clients", () => {
  test("the admin client is disconnected even when a call fails", async () => {
    const fake = fakeKafka({ partitions: 1, failFetch: true });
    await expect(makeTopic(fake.client).getCommittedOffset({ group })).rejects.toThrow();
    expect(fake.adminConnections()).toBe(0);
  });

  test("publishes that start together share one producer", async () => {
    const fake = fakeKafka({ partitions: 1 });
    const kt = makeTopic(fake.client);
    await Promise.all([
      run(kt.publish(1).orDie()),
      run(kt.publish(2).orDie()),
      run(kt.publish(3).orDie()),
    ]);
    expect(fake.producersMade()).toBe(1);
  });
});
