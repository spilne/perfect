import { describe, test, expect } from "bun:test"
import {
  succeed, fail, sync, sleep,
  Clock, TestClock, provide,
  type Eff,
} from "../src"

describe("thenable — await eff", () => {
  test("await succeed(x) resolves to x", async () => {
    expect(await succeed(42)).toBe(42)
  })

  test("await fail(e) rejects", async () => {
    await expect(Promise.resolve(fail("boom"))).rejects.toBe("boom")
  })

  test("await composed chain (pure) resolves correctly", async () => {
    const chain: Eff<number, never> = succeed(1)
      .flatMap((x: number) => succeed(x + 2))
      .flatMap((x: number) => succeed(x * 10))
    expect(await chain).toBe(30)
  })

  test("await sync side effects run", async () => {
    let ran = false
    await sync(() => { ran = true })
    expect(ran).toBe(true)
  })

  test("await defect (thrown) rejects with the thrown error", async () => {
    const eff = sync(() => { throw new Error("oops") })
    await expect(Promise.resolve(eff)).rejects.toBeInstanceOf(Error)
  })

  test("await with async effect — sleep completes on real clock", async () => {
    const start = Date.now()
    await sleep(10).flatMap(() => succeed(undefined))
    expect(Date.now() - start).toBeGreaterThanOrEqual(5)
  })

  test("await works with provide (service injection)", async () => {
    const c = new TestClock()
    const eff = provide(sync(() => "hi"), Clock, c)
    expect(await eff).toBe("hi")
  })

  test("catchAll still fires when eff is awaited", async () => {
    const recovered = (fail("e") as any).catch(() => succeed("ok"))
    expect(await recovered).toBe("ok")
  })

  test("await works with Promise.all — mixed effects", async () => {
    const [a, b, c] = await Promise.all([
      succeed(1),
      succeed(2).flatMap((x: number) => succeed(x * 10)),
      succeed(3),
    ])
    expect([a, b, c]).toEqual([1, 20, 3])
  })
})
