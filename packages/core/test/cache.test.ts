import { describe, test, expect } from "bun:test"
import {
  succeed, fail, sync, cached, cachedBy,
  provide, run,
  Clock, TestClock,
} from "../src"

describe("cached", () => {
  test("runs the source once, replays thereafter", async () => {
    let runs = 0
    const getConfig = cached(sync(() => { runs++; return Math.random() }))

    const program = (getConfig as any).flatMap((a: number) =>
      (getConfig as any).flatMap((b: number) =>
        (getConfig as any).map((c: number) => ({ a, b, c })),
      ),
    )
    const { a, b, c } = await run(program) as any
    expect(a).toBe(b)
    expect(b).toBe(c)
    expect(runs).toBe(1)
  })

  test("failures are not cached — next call re-runs the source", async () => {
    let runs = 0
    const getThing = cached(
      (sync(() => ++runs) as any).flatMap((n: number) =>
        n === 1 ? fail("first-fails") : succeed(n),
      ),
    )

    const program = (getThing as any).catch((_e: any) => succeed(0)).flatMap(() => getThing)
    expect(await run(program as any)).toBe(2)
    expect(runs).toBe(2)
  })

  test("each cached() call produces an independent memo", async () => {
    let runs = 0
    const source = sync(() => ++runs)
    const a = cached(source)
    const b = cached(source)

    const program = (a as any).flatMap((va: number) => (b as any).map((vb: number) => ({ va, vb })))
    const { va, vb } = await run(program) as any
    expect(va).not.toBe(vb)
    expect(runs).toBe(2)
  })

  test("TTL expiry re-runs the source", async () => {
    let runs = 0
    const c = new TestClock()
    const getOnce = cached(sync(() => ++runs), { ttlMs: 1000 })

    const program = (getOnce as any).flatMap((a: number) =>
      (getOnce as any).flatMap((b: number) =>
        sync(() => c.advance(1001)).flatMap(() =>
          (getOnce as any).map((cc: number) => ({ a, b, c: cc })),
        ),
      ),
    )
    const { a, b, c: third } = await run(provide(program, Clock, c) as any) as any
    expect(a).toBe(1)
    expect(b).toBe(1)
    expect(third).toBe(2)
    expect(runs).toBe(2)
  })

  test(".invalidate drops the cache", async () => {
    let runs = 0
    const getOnce = cached(sync(() => ++runs))
    const program = (getOnce as any).flatMap((first: number) =>
      getOnce.invalidate.flatMap(() =>
        (getOnce as any).map((second: number) => ({ first, second })),
      ),
    )
    const { first, second } = await run(program as any) as any
    expect(first).toBe(1)
    expect(second).toBe(2)
  })

  test(".current and .isFresh peek without running", async () => {
    let runs = 0
    const getOnce = cached(sync(() => ++runs))
    // before first run
    expect(await run(getOnce.isFresh)).toBe(false)
    expect(await run(getOnce.current)).toBe(undefined)
    // run once, then peek
    await run(getOnce as any)
    expect(await run(getOnce.isFresh)).toBe(true)
    expect(await run(getOnce.current)).toBe(1)
    expect(runs).toBe(1)
  })
})

describe("cachedBy", () => {
  test("same key replays; different keys run separately", async () => {
    let runs = 0
    const cache = cachedBy((id: string) => sync(() => { runs++; return `value-for-${id}` }))

    const program = (cache.get("a") as any).flatMap((a1: string) =>
      cache.get("a").flatMap((a2: string) =>
        cache.get("b").flatMap((b: string) =>
          (cache.get("a") as any).map((a3: string) => ({ a1, a2, a3, b })),
        ),
      ),
    )
    const { a1, a2, a3, b } = await run(program as any) as any
    expect(a1).toBe("value-for-a")
    expect(a2).toBe(a1)
    expect(a3).toBe(a1)
    expect(b).toBe("value-for-b")
    expect(runs).toBe(2)
  })

  test("TTL expiry forces a re-run", async () => {
    let runs = 0
    const c = new TestClock()
    const cache = cachedBy((_id: string) => sync(() => ++runs), { ttlMs: 1000 })

    const program = (cache.get("k") as any).flatMap((first: number) =>
      cache.get("k").flatMap((second: number) =>
        sync(() => c.advance(1001)).flatMap(() =>
          (cache.get("k") as any).map((third: number) => ({ first, second, third })),
        ),
      ),
    )
    const { first, second, third } = await run(provide(program as any, Clock, c) as any) as any
    expect(first).toBe(1)
    expect(second).toBe(1)
    expect(third).toBe(2)
    expect(runs).toBe(2)
  })

  test("invalidate drops a specific key", async () => {
    let runs = 0
    const cache = cachedBy((_: string) => sync(() => ++runs))

    const program = (cache.get("a") as any).flatMap((a1: number) =>
      cache.invalidate("a").flatMap(() =>
        (cache.get("a") as any).map((a2: number) => ({ a1, a2 })),
      ),
    )
    const { a1, a2 } = await run(program as any) as any
    expect(a1).toBe(1)
    expect(a2).toBe(2)
  })

  test("invalidateAll wipes the whole cache", async () => {
    let runs = 0
    const cache = cachedBy((_: string) => sync(() => ++runs))

    const program = (cache.get("a") as any).flatMap(() =>
      cache.get("b").flatMap(() =>
        cache.invalidateAll.flatMap(() =>
          cache.size.flatMap((s: number) =>
            (cache.get("a") as any).map(() => s),
          ),
        ),
      ),
    )
    expect(await run(program as any)).toBe(0)
    expect(runs).toBe(3)
  })

  test("has(key) returns true only for live entries", async () => {
    let runs = 0
    const cache = cachedBy((_: string) => sync(() => ++runs))
    const program = cache.has("x").flatMap((empty: boolean) =>
      (cache.get("x") as any).flatMap(() =>
        cache.has("x").flatMap((filled: boolean) =>
          cache.invalidate("x").flatMap(() =>
            cache.has("x").map((gone: boolean) => ({ empty, filled, gone })),
          ),
        ),
      ),
    )
    const { empty, filled, gone } = await run(program as any) as any
    expect(empty).toBe(false)
    expect(filled).toBe(true)
    expect(gone).toBe(false)
  })

  test("maxSize evicts the oldest entry (LRU on get)", async () => {
    let runs = 0
    const cache = cachedBy((id: string) => sync(() => ({ id, n: ++runs })), { maxSize: 2 })

    const program = (cache.get("a") as any).flatMap(() =>
      cache.get("b").flatMap(() =>
        // touching "a" makes it most-recent, so "b" becomes the eviction target
        (cache.get("a") as any).flatMap(() =>
          cache.get("c").flatMap(() =>
            cache.size.flatMap((s: number) =>
              cache.has("b").map((hasB: boolean) => ({ s, hasB })),
            ),
          ),
        ),
      ),
    )
    const { s, hasB } = await run(program as any) as any
    expect(s).toBe(2)
    expect(hasB).toBe(false)
  })

  test("per-value TTL function — each value gets its own expiry", async () => {
    let runs = 0
    const c = new TestClock()
    type Token = { value: string; expiresIn: number }
    const cache = cachedBy<string, Token, never>(
      (id: string) => sync(() => {
        runs++
        return { value: `tok-${id}`, expiresIn: id === "short" ? 100 : 10_000 }
      }) as any,
      { ttlMs: (t: Token) => t.expiresIn },
    )

    const program = (cache.get("short") as any).flatMap(() =>
      sync(() => c.advance(200)).flatMap(() =>
        // "short" should be expired
        cache.has("short").flatMap((stillShort: boolean) =>
          // "long" lives well past that
          (cache.get("long") as any).flatMap(() =>
            cache.has("long").map((stillLong: boolean) => ({ stillShort, stillLong })),
          ),
        ),
      ),
    )
    const { stillShort, stillLong } = await run(provide(program as any, Clock, c) as any) as any
    expect(stillShort).toBe(false)
    expect(stillLong).toBe(true)
    expect(runs).toBe(2)
  })

  test("custom keyFn supports compound keys", async () => {
    let runs = 0
    interface Req { org: string; id: number }
    const cache = cachedBy(
      (r: Req) => sync(() => { runs++; return `${r.org}/${r.id}` }),
      { keyFn: (r: Req) => `${r.org}:${r.id}` },
    )

    const program = (cache.get({ org: "acme", id: 1 }) as any).flatMap((a: string) =>
      cache.get({ org: "acme", id: 1 }).flatMap((b: string) =>
        (cache.get({ org: "acme", id: 2 }) as any).map((c: string) => ({ a, b, c })),
      ),
    )
    const { a, b, c } = await run(program as any) as any
    expect(a).toBe("acme/1")
    expect(b).toBe(a)
    expect(c).toBe("acme/2")
    expect(runs).toBe(2)
  })
})
