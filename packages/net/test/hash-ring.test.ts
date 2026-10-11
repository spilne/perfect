import { describe, expect, test } from "bun:test";
import { HashRing, hash32 } from "../src";

const keys = Array.from({ length: 10_000 }, (_, i) => `key-${i}`);

describe("hash32", () => {
  test("matches MurmurHash3's published values", () => {
    expect(hash32("")).toBe(0);
    expect(hash32("", 1)).toBe(0x514e28b7);
    expect(hash32("hello")).toBe(0x248bfa47);
    expect(hash32("The quick brown fox jumps over the lazy dog")).toBe(0x2e4ff723);
  });

  test("hashes the UTF-8 bytes, so different characters don't collide", () => {
    expect(hash32("Ā")).not.toBe(hash32("\u0000"));
  });
});

describe("HashRing", () => {
  test("an empty ring owns nothing", () => {
    expect(HashRing.of([]).owner("x")).toBeUndefined();
    expect(HashRing.of([]).owners("x", 3)).toEqual([]);
  });

  test("the same nodes give the same owners, in whatever order they're listed", () => {
    const one = HashRing.of(["a", "b", "c"]);
    const two = HashRing.of(["c", "a", "b", "a"]);
    expect(two.nodes).toEqual(["a", "b", "c"]);
    expect(keys.slice(0, 100).map((k) => one.owner(k))).toEqual(
      keys.slice(0, 100).map((k) => two.owner(k)),
    );
  });

  test("keys spread evenly over the nodes", () => {
    const ring = HashRing.of(["a", "b", "c", "d"]);
    const counts = new Map<string, number>();
    for (const key of keys) counts.set(ring.owner(key)!, (counts.get(ring.owner(key)!) ?? 0) + 1);
    for (const count of counts.values()) {
      expect(count).toBeGreaterThan(2_000);
      expect(count).toBeLessThan(3_000);
    }
  });

  test("adding a node only moves keys to it", () => {
    const before = HashRing.of(["a", "b", "c"]);
    const after = before.with("d");
    const moves = before.moved(after, keys);
    expect(moves.every((move) => move.to === "d")).toBe(true);
    // About a quarter of the keys go to the new node.
    expect(moves.length).toBeGreaterThan(1_800);
    expect(moves.length).toBeLessThan(3_200);
  });

  test("removing a node only moves its own keys", () => {
    const before = HashRing.of(["a", "b", "c"]);
    const moves = before.moved(before.without("b"), keys);
    expect(moves.every((move) => move.from === "b" && move.to !== "b")).toBe(true);
    expect(moves.length).toBe(keys.filter((k) => before.owner(k) === "b").length);
  });

  test("owners lists different nodes, starting with the owner", () => {
    const ring = HashRing.of(["a", "b", "c"]);
    for (const key of keys.slice(0, 50)) {
      const owners = ring.owners(key, 2);
      expect(owners[0]).toBe(ring.owner(key));
      expect(new Set(owners).size).toBe(2);
    }
    expect(ring.owners("x", 10).sort()).toEqual(["a", "b", "c"]);
  });

  test("the next owner is who takes over when the owner leaves", () => {
    const ring = HashRing.of(["a", "b", "c", "d"]);
    for (const key of keys.slice(0, 200)) {
      const [owner, next] = ring.owners(key, 2);
      expect(ring.without(owner!).owner(key)).toBe(next);
    }
  });
});
