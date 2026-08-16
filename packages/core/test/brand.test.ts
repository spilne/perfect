import { describe, test, expect } from "bun:test";
import { type Brand, nominal, refined, BrandError } from "../src";

type TopicName = Brand<string, "TopicName">;
const TopicName = nominal<TopicName>();

type PartitionId = Brand<number, "PartitionId">;
const PartitionId = refined<PartitionId>(
  (n) => Number.isInteger(n) && n >= 0,
  (n) => `PartitionId must be a non-negative integer, got ${n}`,
);

describe("Brand", () => {
  test("nominal brands with zero runtime cost", () => {
    const t = TopicName("orders");
    expect(t).toBe("orders" as TopicName); // same runtime value
    const use = (name: TopicName) => name.toUpperCase();
    expect(use(t)).toBe("ORDERS"); // still a string underneath
  });

  test("refined validates", () => {
    expect(PartitionId(3)).toBe(3 as PartitionId);
    expect(() => PartitionId(-1)).toThrow(BrandError);
    expect(() => PartitionId(1.5)).toThrow(/non-negative integer/);
  });

  test("type-level: raw values and cross-brands are rejected", () => {
    type GroupId = Brand<string, "GroupId">;
    const GroupId = nominal<GroupId>();
    const use = (topic: TopicName, group: GroupId) => `${topic}:${group}`;

    // @ts-expect-error raw string is not a TopicName
    const _bad1: TopicName = "orders";
    // @ts-expect-error GroupId is not a TopicName
    const _bad2 = use(GroupId("g1"), GroupId("g1"));

    expect(use(TopicName("orders"), GroupId("g1"))).toBe("orders:g1");
  });
});
