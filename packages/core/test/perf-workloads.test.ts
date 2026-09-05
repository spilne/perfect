import { expect, test } from "bun:test";
import { coreSuite } from "../../../scripts/perf/suites/core";
import {
  cancelDeferredWaiters,
  completeChildren,
  fillSlidingWindow,
  groupedSingletons,
} from "../../../scripts/perf/suites/core-workloads";

test("scaling workloads execute their advertised item counts", () => {
  for (const n of [1, 1000, 8000]) {
    expect(groupedSingletons(n)).toBe(n);
    expect(fillSlidingWindow(n)).toBe(n);
    expect(completeChildren(n)).toBe(0);
    expect(cancelDeferredWaiters(n)).toBe(n);
  }
});

test("core benchmarks have unique names and early termination is measured per run", async () => {
  const cases = await coreSuite.cases();
  expect(new Set(cases.map((c) => c.name)).size).toBe(cases.length);
  const early = cases.find((c) => c.name === "stream map/filter/take end-to-end");
  expect(early?.divisor).toBe(1);
  expect(early?.unit).toBe("ns/op");
  for (const bench of cases) await bench.run();
});
