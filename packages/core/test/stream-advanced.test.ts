import { describe, test, expect } from "bun:test"
import {
  succeed, sync, sleep,
  run,
  Stream, Chunk, Pipes,
} from "../src"

describe("Stream.parEvalMap", () => {
  test("processes in parallel, preserves order", async () => {
    const result = await run(
      Stream.of(3, 1, 2)
        .parEvalMap(3, (x) =>
          sleep(x * 10).map(() => `done:${x}`) as any
        )
        .runCollect()
    )
    expect(result).toEqual(["done:3", "done:1", "done:2"])
  })

  test("respects concurrency limit", async () => {
    let maxConcurrent = 0
    let current = 0

    const result = await run(
      Stream.range(0, 8)
        .parEvalMap(2, (x) =>
          sync(() => { current++; maxConcurrent = Math.max(maxConcurrent, current) })
            .flatMap(() => sleep(20))
            .flatMap(() => sync(() => { current--; return x * 10 })) as any
        )
        .runCollect()
    )

    expect(result).toEqual([0, 10, 20, 30, 40, 50, 60, 70])
    expect(maxConcurrent).toBeLessThanOrEqual(2)
  })
})

describe("Stream.parEvalMapUnordered", () => {
  test("processes in parallel, results may be out of order", async () => {
    const result = await run(
      Stream.of(30, 10, 20)
        .parEvalMapUnordered(3, (x) =>
          sleep(x).map(() => x) as any
        )
        .runCollect()
    )
    // all values present, but order depends on completion time
    expect(result.sort()).toEqual([10, 20, 30])
    // fastest should come first
    expect(result[0]).toBe(10)
  })
})

describe("Stream.groupWithin", () => {
  test("groups by size", async () => {
    // use grouped() for pure size-based batching
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .grouped(2)
        .map(c => c.toArray())
        .runCollect()
    )
    expect(result).toEqual([[1, 2], [3, 4], [5]])
  })

  test("groupWithin flushes on size", async () => {
    // Use iterate (one-element chunks) so each evalMap emits separately and
    // groupWithin sees a real timeline rather than a single batch.
    const result = await run(
      Stream.iterate(0, (n: number) => n + 1)
        .take(5)
        .evalMap(x => sleep(5).map(() => x) as any)
        .groupWithin(2, 500)
        .map((c: any) => c.toArray())
        .take(2)
        .runCollect()
    )
    expect(result[0]).toEqual([0, 1])
    expect(result[1]).toEqual([2, 3])
  })

  test("groups by timeout", async () => {
    const result = await run(
      Stream.of(1, 2, 3)
        .groupWithin(100, 50) // size 100 won't be hit, timeout at 50ms
        .map(c => c.toArray())
        .take(1)
        .runCollect()
    )
    expect(result[0]).toEqual([1, 2, 3])
  })
})

describe("Stream.debounce", () => {
  test("emits last value after silence", async () => {
    // emit 1,2,3 rapidly, then wait
    const s = Stream.of(1, 2, 3).debounce(30)
    const result = await run(s.take(1).runCollect())
    expect(result).toEqual([3]) // only last value after debounce
  })
})

describe("Stream.throttle", () => {
  test("limits emission rate", async () => {
    const start = Date.now()
    await run(
      Stream.of(1, 2, 3).throttle(30).runDrain()
    )
    const elapsed = Date.now() - start
    expect(elapsed).toBeGreaterThanOrEqual(55) // ~60ms for 3 items at 30ms apart
  })
})

describe("Stream.merge", () => {
  test("merges two streams concurrently", async () => {
    const s1 = Stream.of(1, 2, 3)
    const s2 = Stream.of(10, 20, 30)
    const result = await run(s1.merge(s2).runCollect())
    expect(result.sort((a, b) => a - b)).toEqual([1, 2, 3, 10, 20, 30])
  })

  test("merge with different speeds", async () => {
    const fast = Stream.of(1, 2, 3)
    const slow = Stream.repeat(sleep(20).map(() => 99) as any).take(2)
    const result = await run(fast.merge(slow).runCollect())
    expect(result).toContain(1)
    expect(result).toContain(99)
    expect(result.length).toBe(5)
  })
})

describe("Pipes", () => {
  test("lines pipe", async () => {
    const result = await run(
      Stream.of("hello\nworld\n", "foo\nbar")
        .through(Pipes.lines)
        .runCollect()
    )
    expect(result).toEqual(["hello", "world", "foo", "bar"])
  })

  test("csv pipe", async () => {
    const result = await run(
      Stream.of("name,age\n", "alice,30\nbob,25\n")
        .through(Pipes.csv)
        .runCollect()
    )
    expect(result).toEqual([
      ["name", "age"],
      ["alice", "30"],
      ["bob", "25"],
    ])
  })

  test("jsonl pipe", async () => {
    const result = await run(
      Stream.of('{"a":1}\n{"b":2}\n')
        .through(Pipes.jsonl)
        .runCollect()
    )
    expect(result).toEqual([{ a: 1 }, { b: 2 }])
  })

  test("pipe composition via through", async () => {
    const result = await run(
      Stream.of(1, 2, 3, 4, 5)
        .through(Pipes.filter((x: number) => x % 2 === 0))
        .through(Pipes.mapPipe((x: number) => x * 10))
        .runCollect()
    )
    expect(result).toEqual([20, 40])
  })
})

describe("Stream realistic example", () => {
  test("event processing pipeline", async () => {
    interface Event { type: string; value: number; ts: number }

    const events: Event[] = Array.from({ length: 20 }, (_, i) => ({
      type: i % 3 === 0 ? "click" : "scroll",
      value: i,
      ts: Date.now() + i * 10,
    }))

    const result = await run(
      Stream.fromArray(events)
        .filter(e => e.type === "click")
        .map(e => e.value)
        .grouped(3)
        .map(chunk => chunk.reduce(0, (a, b) => a + b))
        .runCollect()
    )

    // clicks at indices: 0, 3, 6, 9, 12, 15, 18
    // values: 0, 3, 6, 9, 12, 15, 18
    // grouped(3): [0,3,6], [9,12,15], [18]
    // sums: 9, 36, 18
    expect(result).toEqual([9, 36, 18])
  })
})
