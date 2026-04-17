import { describe, test, expect } from "bun:test"
import { provide, run, runSync, Random, TestRandom } from "../src"

describe("TestRandom — seeded determinism", () => {
  test("same seed → same sequence", () => {
    const r1 = new TestRandom(42)
    const r2 = new TestRandom(42)

    const draw = (r: TestRandom) =>
      runSync(provide(r.next().flatMap((a: number) =>
        r.next().flatMap((b: number) =>
          r.next().map((c: number) => [a, b, c]))), Random, r))

    expect(draw(r1)).toEqual(draw(r2))
  })

  test("different seeds → different sequences", () => {
    const r1 = new TestRandom(1)
    const r2 = new TestRandom(2)
    expect(runSync(provide(r1.next(), Random, r1))).not.toBe(
      runSync(provide(r2.next(), Random, r2)),
    )
  })

  test("nextInt is bounded", async () => {
    const r = new TestRandom(99)
    const program = provide(
      Random.get.flatMap((rnd: any) => rnd.nextInt(10)),
      Random,
      r,
    )
    for (let i = 0; i < 50; i++) {
      const v = await run(program)
      expect(v).toBeGreaterThanOrEqual(0)
      expect(v).toBeLessThan(10)
    }
  })

  test("nextRange honors min/max", async () => {
    const r = new TestRandom(7)
    const program = provide(
      Random.get.flatMap((rnd: any) => rnd.nextRange(50, 60)),
      Random,
      r,
    )
    for (let i = 0; i < 50; i++) {
      const v = await run(program)
      expect(v).toBeGreaterThanOrEqual(50)
      expect(v).toBeLessThan(60)
    }
  })

  test("setNextValues forces specific outcomes for the next N calls", async () => {
    const r = new TestRandom()
    r.setNextValues([0.1, 0.9])
    const program = provide(
      r.nextInt(10).flatMap((a: number) => r.nextInt(10).map((b: number) => [a, b])),
      Random,
      r,
    )
    expect(await run(program)).toEqual([1, 9])
    expect(r.queuedCount).toBe(0)
  })

  test("nextBool uses the same source — also forceable", async () => {
    const r = new TestRandom()
    r.setNextValues([0.4, 0.6])
    const program = provide(
      r.nextBool().flatMap((a: boolean) => r.nextBool().map((b: boolean) => [a, b])),
      Random,
      r,
    )
    expect(await run(program)).toEqual([true, false])
  })

  test("shuffle is deterministic for a given seed", () => {
    const arr = [1, 2, 3, 4, 5]
    const r1 = new TestRandom(33)
    const r2 = new TestRandom(33)
    expect(runSync(provide(r1.shuffle(arr), Random, r1)))
      .toEqual(runSync(provide(r2.shuffle(arr), Random, r2)))
  })

  test("choice picks from the array", () => {
    const r = new TestRandom(11)
    const arr = ["a", "b", "c"]
    for (let i = 0; i < 20; i++) {
      const picked = runSync(provide(r.choice(arr), Random, r))
      expect(arr).toContain(picked)
    }
  })

  test("choice on empty throws via Cause.die", async () => {
    const r = new TestRandom()
    await expect(run(provide(r.choice([]), Random, r))).rejects.toBeInstanceOf(Error)
  })

  test("setNextValues rejects out-of-range numbers", () => {
    const r = new TestRandom()
    expect(() => r.setNextValues([1.0])).toThrow(/in \[0, 1\)/)
    expect(() => r.setNextValues([-0.1])).toThrow(/in \[0, 1\)/)
  })

  test("reseed restarts the sequence", () => {
    const r = new TestRandom(5)
    const first = runSync(provide(r.next(), Random, r))
    r.reseed(5)
    const second = runSync(provide(r.next(), Random, r))
    expect(first).toBe(second)
  })

  test("real Random is the default — no provide() needed", async () => {
    const v = await run(Random.get.flatMap((rnd: any) => rnd.next()))
    expect(v).toBeGreaterThanOrEqual(0)
    expect(v).toBeLessThan(1)
  })
})
