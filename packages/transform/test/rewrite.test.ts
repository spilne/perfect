import { describe, test, expect } from "bun:test"
import { rewriteEffBlocks } from "../src/rewrite"

describe("eff($) rewriter", () => {
  test("simple bind + return", () => {
    const input = `const program = eff(($) => {
  const x = $(getX())
  return x + 1
})`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("getX().flatMap((x) =>")
    expect(output).toContain("succeed(x + 1)")
  })

  test("multiple binds", () => {
    const input = `const program = eff(($) => {
  const a = $(getA())
  const b = $(getB(a))
  return a + b
})`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("getA().flatMap((a) =>")
    expect(output).toContain("getB(a).flatMap((b) =>")
    expect(output).toContain("succeed(a + b)")
  })

  test("discard bind", () => {
    const input = `const program = eff(($) => {
  const x = $(getX())
  $(log(x))
  return x
})`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("log(x).flatMap(() =>")
  })

  test("preserves non-eff code", () => {
    const input = `const x = 1 + 2\nconst y = foo()`
    expect(rewriteEffBlocks(input)).toBe(input)
  })
})

describe("for-comprehension rewriter (monad-generic)", () => {
  test("single bind + yield uses .map at the tail (no succeed wrapper)", () => {
    const input = `const program = for {
  x <- getX()
} yield x + 1`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("(getX()).map((x)")
    expect(output).toContain("x + 1")
    expect(output).not.toContain("succeed(")
  })

  test("multiple binds — all but last use flatMap; last uses map", () => {
    const input = `const program = for {
  cfg <- Config.get
  user <- Db.findUser(cfg.id)
} yield user.name`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("(Config.get).flatMap((cfg)")
    expect(output).toContain("(Db.findUser(cfg.id)).map((user)")
    expect(output).toContain("user.name")
    expect(output).not.toContain("succeed(")
  })

  test("discard bind at the tail uses .map(() => yieldExpr)", () => {
    const input = `const program = for {
  user <- getUser()
  _ <- Logger.info(user.name)
} yield user`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("(getUser()).flatMap((user)")
    expect(output).toContain("(Logger.info(user.name)).map(() => user)")
  })

  test("guards throw a clear error (no longer supported)", () => {
    const input = `const program = for {
  user <- getUser()
  if user.tier === "pro"
} yield user.name`
    expect(() => rewriteEffBlocks(input)).toThrow(/guards \(if\) are not supported/)
  })

  test("let binding (val / let / const)", () => {
    const input = `const program = for {
  x <- getX()
  val doubled = x * 2
} yield doubled`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("(getX()).map((x)")
    expect(output).toContain("((doubled) =>")
    expect(output).toContain("x * 2")
  })

  test("works on Array (monad-generic)", () => {
    const input = `const pairs = for {
  x <- [1, 2]
  y <- [3, 4]
} yield [x, y]`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("([1, 2]).flatMap((x)")
    expect(output).toContain("([3, 4]).map((y)")
  })

  test("does not touch regular for loops", () => {
    const input = `for (let i = 0; i < 10; i++) { console.log(i) }`
    expect(rewriteEffBlocks(input)).toBe(input)
  })

  test("does not touch for...of", () => {
    const input = `for (const x of items) { process(x) }`
    expect(rewriteEffBlocks(input)).toBe(input)
  })

  test("multiple for-comprehensions in one file", () => {
    const input = `const a = for {
  x <- getX()
} yield x

const b = for {
  y <- getY()
} yield y * 2`
    const output = rewriteEffBlocks(input)
    expect(output).toContain("getX()")
    expect(output).toContain("getY()")
  })
})

describe("integration — desugared code runs", () => {
  test("Eff — monad-generic tail (.map) still works", async () => {
    const { succeed, run } = await import("../../core/src")

    // for { x <- succeed(10); y <- succeed(20) } yield x + y
    const program = (succeed(10)).flatMap((x) =>
      (succeed(20)).map((y) => x + y))

    expect(await run(program)).toBe(30)
  })

  test("Array — same desugaring works on a different monad", () => {
    // for { x <- [1, 2]; y <- [3, 4] } yield [x, y]
    const pairs = [1, 2].flatMap((x) => [3, 4].map((y) => [x, y]))
    expect(pairs).toEqual([[1, 3], [1, 4], [2, 3], [2, 4]])
  })

  test("for-comprehension with services", async () => {
    const { succeed, service, provide, run } = await import("../../core/src")

    interface Db { find(id: string): ReturnType<typeof succeed<string>> }
    const Db = service<Db>("Db")

    // for { db <- Db.get; name <- db.find("1") } yield name
    const program = (Db.get).flatMap((db) =>
      (db.find("1")).map((name) => name))

    const result = await run(
      provide(program, Db, { find: (id) => succeed(`user-${id}`) })
    )
    expect(result).toBe("user-1")
  })
})
