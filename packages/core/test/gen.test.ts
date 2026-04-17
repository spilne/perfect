import { describe, test, expect } from "bun:test";
import {
  Gen,
  forAll,
  succeed,
  provide,
  run,
  runSync,
  Random,
  TestRandom,
  type PropertyFailure,
} from "../src";

const withSeed = <A>(seed: number, eff: any): A =>
  runSync(provide(eff, Random, new TestRandom(seed)));

describe("Gen — primitive generators", () => {
  test("int respects bounds", () => {
    const g = Gen.int(10, 20);
    for (let s = 1; s < 50; s++) {
      const v = withSeed<number>(s, g.generate);
      expect(v).toBeGreaterThanOrEqual(10);
      expect(v).toBeLessThan(20);
    }
  });

  test("bool produces booleans", () => {
    const v = withSeed<boolean>(7, Gen.bool.generate);
    expect(typeof v).toBe("boolean");
  });

  test("string respects length and uses default alphanumeric alphabet", () => {
    const g = Gen.string({ length: 8 });
    const v = withSeed<string>(3, g.generate);
    expect(v.length).toBe(8);
    expect(/^[A-Za-z0-9]+$/.test(v)).toBe(true);
  });

  test("string with custom alphabet", () => {
    const g = Gen.string({ length: 12, alphabet: "AB" });
    const v = withSeed<string>(11, g.generate);
    expect(v.length).toBe(12);
    expect(/^[AB]+$/.test(v)).toBe(true);
  });

  test("choice picks from the given items", () => {
    const items = ["x", "y", "z"];
    for (let s = 1; s < 30; s++) {
      const v = withSeed<string>(s, Gen.choice(...items).generate);
      expect(items).toContain(v);
    }
  });

  test("constant always returns the value", () => {
    const v = withSeed<number>(1, Gen.constant(42).generate);
    expect(v).toBe(42);
  });

  test("array respects length", () => {
    const g = Gen.array(Gen.int(0, 100), { length: 5 });
    const v = withSeed<number[]>(99, g.generate);
    expect(v.length).toBe(5);
    expect(v.every((n) => n >= 0 && n < 100)).toBe(true);
  });

  test("object generates the declared shape", () => {
    const g = Gen.object({
      name: Gen.string({ length: 4 }),
      age: Gen.int(0, 150),
      ok: Gen.bool,
    });
    const v = withSeed<{ name: string; age: number; ok: boolean }>(5, g.generate);
    expect(typeof v.name).toBe("string");
    expect(v.name.length).toBe(4);
    expect(typeof v.age).toBe("number");
    expect(typeof v.ok).toBe("boolean");
  });

  test("tuple generates fixed-arity tuples", () => {
    const g = Gen.tuple(Gen.int(0, 10), Gen.bool, Gen.constant("x"));
    const v = withSeed<[number, boolean, string]>(2, g.generate);
    expect(v.length).toBe(3);
    expect(typeof v[0]).toBe("number");
    expect(typeof v[1]).toBe("boolean");
    expect(v[2]).toBe("x");
  });

  test("oneOf picks from one of the supplied generators", () => {
    const g = Gen.oneOf(Gen.constant("a"), Gen.constant("b"), Gen.constant("c"));
    for (let s = 1; s < 30; s++) {
      expect(["a", "b", "c"]).toContain(withSeed<string>(s, g.generate));
    }
  });

  test("map transforms generated values", () => {
    const g = Gen.map(Gen.int(0, 100), (n) => n * 2);
    const v = withSeed<number>(8, g.generate);
    expect(v % 2).toBe(0);
    expect(v).toBeGreaterThanOrEqual(0);
  });

  test("flatMap chains generators", () => {
    const g = Gen.flatMap(Gen.int(1, 5), (n) => Gen.array(Gen.constant(n), { length: n }));
    const v = withSeed<number[]>(13, g.generate);
    expect(v.length).toBe(v[0]);
    expect(v.every((x) => x === v[0])).toBe(true);
  });
});

describe("forAll", () => {
  test("passes when the property holds for all samples", async () => {
    const r = new TestRandom(1);
    const program = provide(
      forAll(Gen.int(0, 1_000_000), 100, (n) => n >= 0),
      Random,
      r,
    );
    await run(program as any);
  });

  test("fails with the counterexample when the property breaks", async () => {
    const r = new TestRandom(42);
    let failure: PropertyFailure<number> | null = null;

    const program = provide(
      (forAll(Gen.int(0, 1000), 200, (n) => n < 500) as any).catch((f: PropertyFailure<number>) => {
        failure = f;
        return succeed(undefined as any);
      }),
      Random,
      r,
    );

    await run(program as any);
    expect(failure).not.toBeNull();
    expect(failure!.value).toBeGreaterThanOrEqual(500);
    expect(failure!.attempt).toBeGreaterThan(0);
    expect(failure!.reason).toContain("property failed");
  });

  test("predicate may return Eff<boolean>", async () => {
    const r = new TestRandom(3);
    const program = provide(
      forAll(Gen.int(-100, 100), 50, (n) => {
        // Eff-returning predicate
        return (Random.get as any).map((_rnd: Random) => Math.abs(n) <= 100);
      }),
      Random,
      r,
    );
    await run(program as any);
  });

  test("forAll is deterministic under TestRandom", async () => {
    const program = (seed: number) =>
      provide(
        (forAll(Gen.int(0, 1000), 5, (n) => n < 200) as any).catch((f: PropertyFailure<number>) =>
          succeed(f.value),
        ),
        Random,
        new TestRandom(seed),
      );

    const a = await run(program(11) as any);
    const b = await run(program(11) as any);
    expect(a).toBe(b);
  });
});
