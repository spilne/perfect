import { describe, test, expect } from "bun:test"
import {
  succeed, fail, sync, sleep,
  run, runSync,
  Cause,
} from "../src"

describe("error combinators", () => {
  test("orDie turns a Fail into a defect", async () => {
    const eff = (fail("boom") as any).orDie()
    // defect propagates through run as a thrown value (not the Fail error directly — via squash it's the same)
    await expect(run(eff)).rejects.toBe("boom")
  })

  test("mapError transforms the error", async () => {
    const eff = (fail("low") as any).mapError((e: string) => `HIGH:${e}`)
    await expect(run(eff)).rejects.toBe("HIGH:low")
  })

  test("tapError sees error without consuming it", async () => {
    let seen: string | null = null
    const eff = (fail("oops") as any).tapError((e: string) =>
      sync(() => { seen = e }),
    )
    await expect(run(eff)).rejects.toBe("oops")
    expect(seen).toBe("oops")
  })

  test("option collapses failure to undefined", async () => {
    expect(await run((fail("x") as any).option())).toBe(undefined)
    expect(await run((succeed(7) as any).option())).toBe(7)
  })

  test("catchSome only catches when handler returns a value", async () => {
    const eff = (fail("keep") as any).catchSome((e: string) =>
      e === "other" ? succeed("caught") : undefined,
    )
    await expect(run(eff)).rejects.toBe("keep")

    const eff2 = (fail("other") as any).catchSome((e: string) =>
      e === "other" ? succeed("caught") : undefined,
    )
    expect(await run(eff2)).toBe("caught")
  })

  test("catchAllCause sees the full Cause", async () => {
    let seenCause: Cause | null = null
    const eff = (fail("e") as any).catchAllCause((c: Cause) => {
      seenCause = c
      return succeed("recovered")
    })
    expect(await run(eff)).toBe("recovered")
    expect(seenCause && Cause.firstFail(seenCause)).toEqual({ value: "e" })
  })

  test("tapBoth fires exactly the matching side", async () => {
    let okRan = 0, errRan = 0
    const success = (succeed(1) as any).tapBoth(
      () => sync(() => { errRan++ }),
      () => sync(() => { okRan++ }),
    )
    await run(success)
    expect(okRan).toBe(1)
    expect(errRan).toBe(0)

    const failing = (fail("x") as any).tapBoth(
      () => sync(() => { errRan++ }),
      () => sync(() => { okRan++ }),
    )
    await expect(run(failing)).rejects.toBe("x")
    expect(okRan).toBe(1)
    expect(errRan).toBe(1)
  })
})

describe("control flow", () => {
  test("when runs only when cond is true", async () => {
    let ran = 0
    const side = sync(() => { ran++; return "done" })
    expect(await run((side as any).when(() => true))).toBe("done")
    expect(ran).toBe(1)
    expect(await run((side as any).when(() => false))).toBe(undefined)
    expect(ran).toBe(1)
  })

  test("unless is the inverse of when", async () => {
    let ran = 0
    const side = sync(() => { ran++ })
    await run((side as any).unless(() => true))
    expect(ran).toBe(0)
    await run((side as any).unless(() => false))
    expect(ran).toBe(1)
  })
})

describe("fluent fiber combinators", () => {
  test(".race picks the faster effect", async () => {
    const fast = sleep(5).flatMap(() => succeed("fast"))
    const slow = sleep(50).flatMap(() => succeed("slow"))
    expect(await run((fast as any).race(slow))).toBe("fast")
  })

  test(".timeoutFail is fluent", async () => {
    const eff = (sleep(100).flatMap(() => succeed("done")) as any)
      .timeoutFail(10, () => "nope" as const)
    await expect(run(eff)).rejects.toBe("nope")
  })

  test(".delay is fluent", async () => {
    const start = Date.now()
    await run((succeed(1) as any).delay(20))
    expect(Date.now() - start).toBeGreaterThanOrEqual(15)
  })

  test(".uninterruptible on a method call", async () => {
    let ran = 0
    const work = (sleep(20).flatMap(() => sync(() => { ran++; return 1 })) as any).uninterruptible()
    const eff = (work as any).fork().flatMap((f: any) =>
      sleep(2).flatMap(() => sync(() => { f.interrupt(); return null })).flatMap(() => f.await()),
    )
    await run(eff)
    // ran should be 1 because the uninterruptible body completed before the interrupt could take effect
    expect(ran).toBe(1)
  })
})
