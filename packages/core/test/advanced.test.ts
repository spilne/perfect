import { describe, test, expect } from "bun:test"
import {
  succeed, fail, sync, async,
  fork, join, interrupt,
  sleep, delay, race, timeout,
  ensuring, retry, all,
  service, provide,
  run, runSync, runFiber,
  Ref, Fiber,
  type Eff, type Throws, type Needs,
} from "../src"

// ── Fiber ──────────────────────────────────────────────────────────

describe("fiber", () => {
  test("fork and join", async () => {
    const program = fork(succeed(42)).flatMap(fiber => join(fiber))
    expect(await run(program)).toBe(42)
  })

  test("fork runs concurrently", async () => {
    const log: string[] = []

    const slow = sync(() => log.push("slow-start"))
      .flatMap(() => sleep(20))
      .flatMap(() => sync(() => log.push("slow-end")))

    const fast = sync(() => log.push("fast"))

    const program = fork(slow)
      .flatMap(() => fast)
      .flatMap(() => sleep(50))

    await run(program)
    // fork schedules child for next tick — parent runs first
    expect(log).toEqual(["fast", "slow-start", "slow-end"])
  })

  test("interrupt a fiber", async () => {
    const program = fork(sleep(1000))
      .flatMap(fiber =>
        sleep(10).flatMap(() =>
          interrupt(fiber).as("done")
        )
      )

    expect(await run(program)).toBe("done")
  })

  test("fork method syntax", async () => {
    const program = succeed(42).fork().flatMap(f => join(f))
    expect(await run(program)).toBe(42)
  })
})

// ── Race ───────────────────────────────────────────────────────────

describe("race", () => {
  test("first to complete wins", async () => {
    const fast = delay(succeed("fast"), 5)
    const slow = delay(succeed("slow"), 50)
    expect(await run(race([fast, slow]))).toBe("fast")
  })

  test("race propagates failure", async () => {
    const failing = delay(fail("oops") as Eff<string, Throws<string>>, 5)
    const slow = delay(succeed("slow"), 50)
    await expect(run(race([failing, slow]))).rejects.toBe("oops")
  })
})

// ── Timeout ────────────────────────────────────────────────────────

describe("timeout", () => {
  test("completes before timeout", async () => {
    const eff = timeout(delay(succeed("ok"), 5), 100, () => "timed out")
    expect(await run(eff)).toBe("ok")
  })

  test("times out", async () => {
    const eff = timeout(sleep(200).as("late"), 10, () => "timed out")
    await expect(run(eff)).rejects.toBe("timed out")
  })
})

// ── Ensuring ───────────────────────────────────────────────────────

describe("ensuring", () => {
  test("runs finalizer on success", async () => {
    let finalized = false
    const eff = ensuring(succeed(42), sync(() => { finalized = true }))
    expect(await run(eff)).toBe(42)
    expect(finalized).toBe(true)
  })

  test("runs finalizer on failure", async () => {
    let finalized = false
    const eff = ensuring(fail("boom"), sync(() => { finalized = true }))
    await expect(run(eff)).rejects.toBe("boom")
    expect(finalized).toBe(true)
  })

  test("method syntax", async () => {
    let finalized = false
    const eff = succeed(42).ensuring(sync(() => { finalized = true }))
    expect(await run(eff)).toBe(42)
    expect(finalized).toBe(true)
  })
})

// ── Retry ──────────────────────────────────────────────────────────

describe("retry", () => {
  test("retries on typed failure", async () => {
    let attempts = 0
    const flaky: any = sync(() => ++attempts)
      .flatMap((n: number) => (n < 3 ? fail("not yet") : succeed("ok")))

    expect(await run(retry(flaky, { times: 5 }))).toBe("ok")
    expect(attempts).toBe(3)
  })

  test("does NOT retry defects (thrown errors) by default", async () => {
    let attempts = 0
    const flaky = sync(() => { attempts++; throw new Error("defect") })
    await expect(run(retry(flaky, { times: 5 }))).rejects.toBeInstanceOf(Error)
    expect(attempts).toBe(1)
  })

  test("gives up after max retries", async () => {
    const always_fail = fail("nope")
    await expect(run(retry(always_fail, { times: 3 }))).rejects.toBe("nope")
  })

  test("retry with delay", async () => {
    let attempts = 0
    const flaky: any = sync(() => ++attempts)
      .flatMap((n: number) => (n < 2 ? fail("not yet") : succeed("ok")))

    const start = Date.now()
    await run(retry(flaky, { times: 3, delay: 20 }))
    const elapsed = Date.now() - start
    expect(elapsed).toBeGreaterThanOrEqual(15)
  })
})

// ── Ref ────────────────────────────────────────────────────────────

describe("ref", () => {
  test("make, get, set", () => {
    const program = Ref.make(0)
      .flatMap(ref =>
        ref.set(42).flatMap(() => ref.get)
      )
    expect(runSync(program)).toBe(42)
  })

  test("update", () => {
    const program = Ref.make(10)
      .flatMap(ref =>
        ref.update(x => x + 5).flatMap(() => ref.get)
      )
    expect(runSync(program)).toBe(15)
  })

  test("modify", () => {
    const program = Ref.make(10)
      .flatMap(ref =>
        ref.modify(x => [`was:${x}`, x + 1])
          .zip(ref.get)
      )
    expect(runSync(program)).toEqual(["was:10", 11])
  })

  test("getAndSet", () => {
    const program = Ref.make("old")
      .flatMap(ref =>
        ref.getAndSet("new").zip(ref.get)
      )
    expect(runSync(program)).toEqual(["old", "new"])
  })

  test("concurrent updates", async () => {
    const program = Ref.make(0).flatMap(counter => {
      const increment = counter.update(x => x + 1)
      const batch = all(Array.from({ length: 100 }, () => increment))
      return batch.flatMap(() => counter.get)
    })
    expect(await run(program)).toBe(100)
  })
})

// ── Parallel syntax ────────────────────────────────────────────────

describe("parallel syntax", () => {
  test("parZip", async () => {
    const a = delay(succeed(1), 5)
    const b = delay(succeed("two"), 5)
    expect(await run(a.parZip(b))).toEqual([1, "two"])
  })

  test("parZipWith", async () => {
    const a = delay(succeed(3), 5)
    const b = delay(succeed(4), 5)
    expect(await run(a.parZipWith(b, (x, y) => x + y))).toBe(7)
  })

  test("parFlatMap", async () => {
    const a = delay(succeed(10), 5)
    const b = delay(succeed(20), 5)
    const result = await run(
      a.parFlatMap(b, (x, y) => succeed(x + y))
    )
    expect(result).toBe(30)
  })

  test("parZip runs concurrently (not sequential)", async () => {
    const a = delay(succeed("a"), 30)
    const b = delay(succeed("b"), 30)
    const start = Date.now()
    await run(a.parZip(b))
    const elapsed = Date.now() - start
    // if sequential would be ~60ms, parallel should be ~30ms
    expect(elapsed).toBeLessThan(50)
  })
})

// ── Sleep / delay ──────────────────────────────────────────────────

describe("sleep and delay", () => {
  test("sleep", async () => {
    const start = Date.now()
    await run(sleep(30))
    expect(Date.now() - start).toBeGreaterThanOrEqual(25)
  })

  test("delay", async () => {
    const start = Date.now()
    expect(await run(delay(succeed("delayed"), 30))).toBe("delayed")
    expect(Date.now() - start).toBeGreaterThanOrEqual(25)
  })
})

// ── Complex compositions ───────────────────────────────────────────

describe("complex compositions", () => {
  test("retry + timeout", async () => {
    let attempts = 0
    const flaky: any = sync(() => ++attempts)
      .flatMap((n: number) => (n < 3 ? fail("nope") : succeed("ok")))

    const eff = timeout(
      retry(flaky, { times: 5, delay: 10 }),
      500,
      () => "timed out",
    )
    expect(await run(eff)).toBe("ok")
  })

  test("service + retry + ensuring", async () => {
    interface ApiClient { fetch(url: string): Eff<string, Throws<string>> }
    const ApiClient = service<ApiClient>("ApiClient")

    let attempts = 0
    let cleaned = false

    const program = ApiClient.get.flatMap(api =>
      ensuring(
        retry(api.fetch("/data"), { times: 3 }),
        sync(() => { cleaned = true }),
      )
    )

    const provided = provide(program, ApiClient, {
      fetch: (_url) => sync(() => ++attempts)
        .flatMap((n: number) => (n < 2 ? fail("fail") : succeed("response"))) as any,
    })

    expect(await run(provided)).toBe("response")
    expect(attempts).toBe(2)
    expect(cleaned).toBe(true)
  })

  test("fork + ref + all", async () => {
    const program = Ref.make<string[]>([]).flatMap(log => {
      const worker = (name: string) =>
        sleep(Math.random() * 10)
          .flatMap(() => log.update(l => [...l, name]))

      return all([
        worker("a"), worker("b"), worker("c"),
      ]).flatMap(() => log.get)
    })

    const result = await run(program)
    expect(result.sort()).toEqual(["a", "b", "c"])
  })
})
