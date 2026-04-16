import { describe, test, expect } from "bun:test"
import { rewriteEffBlocks } from "../src/rewrite"

// These tests write for-comprehension source code, transform it,
// then eval() the result with the real Perfect runtime to verify
// it actually works end-to-end.

async function runForComprehension(source: string): Promise<any> {
  const transformed = rewriteEffBlocks(source)
  const { succeed, fail, sync, sleep, service, provide, all, run } = await import("../../core/src")
  const fn = new Function("succeed", "fail", "sync", "sleep", "service", "provide", "all", "run",
    `return (async () => { ${transformed} })()`)
  return fn(succeed, fail, sync, sleep, service, provide, all, run)
}

describe("for-comprehension e2e", () => {
  test("simple bind + yield", async () => {
    const result = await runForComprehension(`
      const program = for {
        x <- succeed(10)
        y <- succeed(20)
      } yield x + y
      return await run(program)
    `)
    expect(result).toBe(30)
  })

  test("three binds", async () => {
    const result = await runForComprehension(`
      const program = for {
        a <- succeed("hello")
        b <- succeed(" ")
        c <- succeed("world")
      } yield a + b + c
      return await run(program)
    `)
    expect(result).toBe("hello world")
  })

  test("discard bind with _", async () => {
    const result = await runForComprehension(`
      let sideEffect = 0
      const program = for {
        x <- succeed(42)
        _ <- sync(() => { sideEffect = x })
      } yield x
      const r = await run(program)
      return { r, sideEffect }
    `)
    expect(result).toEqual({ r: 42, sideEffect: 42 })
  })

  test("guards throw at transform time (no longer supported)", () => {
    expect(() =>
      rewriteEffBlocks(`
        const program = for {
          x <- succeed(10)
          if x > 5
        } yield x * 2
      `),
    ).toThrow(/guards \(if\) are not supported/)
  })

  test("monad-generic — works on Array directly", async () => {
    const result = await runForComprehension(`
      const pairs = for {
        x <- [1, 2, 3]
        y <- [10, 20]
      } yield x + y
      return pairs
    `)
    expect(result).toEqual([11, 21, 12, 22, 13, 23])
  })

  test("val binding", async () => {
    const result = await runForComprehension(`
      const program = for {
        x <- succeed(10)
        val doubled = x * 2
      } yield doubled
      return await run(program)
    `)
    expect(result).toBe(20)
  })

  test("with services", async () => {
    const result = await runForComprehension(`
      const Db = service("Db")
      const program = for {
        db <- Db.get
        name <- db.find("1")
      } yield name
      return await run(provide(program, Db, { find: (id) => succeed("user-" + id) }))
    `)
    expect(result).toBe("user-1")
  })

  test("chained with fluent API", async () => {
    const result = await runForComprehension(`
      const inner = for {
        x <- succeed(5)
        y <- succeed(3)
      } yield x + y

      const program = inner
        .map(n => n * 10)
        .flatMap(n => succeed(n + 1))

      return await run(program)
    `)
    expect(result).toBe(81) // (5+3)*10 + 1
  })

  test("error handling with .catch after for", async () => {
    const result = await runForComprehension(`
      const program = (for {
        x <- fail("boom")
      } yield x).catch(() => succeed("recovered"))
      return await run(program)
    `)
    expect(result).toBe("recovered")
  })

  test("nested for-comprehensions", async () => {
    const result = await runForComprehension(`
      const getX = for {
        a <- succeed(10)
      } yield a

      const program = for {
        x <- getX
        y <- succeed(x + 5)
      } yield y
      return await run(program)
    `)
    expect(result).toBe(15)
  })
})

describe("eff($) e2e", () => {
  test("simple bind + return", async () => {
    const result = await runForComprehension(`
      const program = eff(($) => {
        const x = $(succeed(10))
        const y = $(succeed(20))
        return x + y
      })
      return await run(program)
    `)
    expect(result).toBe(30)
  })

  test("discard + side effect", async () => {
    const result = await runForComprehension(`
      let seen = 0
      const program = eff(($) => {
        const x = $(succeed(42))
        $(sync(() => { seen = x }))
        return x
      })
      const r = await run(program)
      return { r, seen }
    `)
    expect(result).toEqual({ r: 42, seen: 42 })
  })

  test("with services", async () => {
    const result = await runForComprehension(`
      const Logger = service("Logger")
      const logs = []
      const program = eff(($) => {
        const log = $(Logger.get)
        $(log.info("hello"))
        $(log.info("world"))
        return logs.join(", ")
      })
      return await run(provide(program, Logger, {
        info: (msg) => sync(() => { logs.push(msg) })
      }))
    `)
    expect(result).toBe("hello, world")
  })
})
