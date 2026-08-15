import { describe, test, expect } from "bun:test";
import { sync, provide, run, Metrics, MetricsRegistry } from "../src";

describe("instruments", () => {
  test("counter increments", () => {
    const r = new MetricsRegistry();
    const c = r.counter("reqs");
    c.inc();
    c.inc(4);
    expect(c.value).toBe(5);
    // same name+labels → same instrument
    expect(r.counter("reqs")).toBe(c);
  });

  test("labeled instruments are distinct", () => {
    const r = new MetricsRegistry();
    r.counter("reqs", { route: "/a" }).inc();
    r.counter("reqs", { route: "/b" }).inc(2);
    const snap = r.snapshot();
    expect(snap.counters["reqs{route=/a}"]).toBe(1);
    expect(snap.counters["reqs{route=/b}"]).toBe(2);
  });

  test("gauge sets and adjusts", () => {
    const r = new MetricsRegistry();
    const g = r.gauge("pool");
    g.set(10);
    g.adjust(-3);
    expect(g.value).toBe(7);
  });

  test("histogram buckets and totals", () => {
    const r = new MetricsRegistry();
    const h = r.histogram("latency", { buckets: [1, 5, 10] });
    h.record(0.5);
    h.record(3);
    h.record(7);
    h.record(100);
    expect(h.count).toBe(4);
    expect(h.sum).toBeCloseTo(110.5);
    expect(h.bucketCounts).toEqual([1, 2, 3, 4]); // cumulative; last = +Inf
  });
});

describe("Metrics service", () => {
  test("effects read the registry from context", async () => {
    const registry = new MetricsRegistry();
    const program = Metrics.counter("jobs").flatMap((c) =>
      sync(() => c.inc(3)).flatMap(() => Metrics.snapshot),
    );
    const snap = await run(provide(program, Metrics, registry) as any);
    expect(snap.counters["jobs"]).toBe(3);
    expect(registry.snapshot().counters["jobs"]).toBe(3);
  });

  test("default registry works without provide", async () => {
    const c = await run(Metrics.counter("default-reg-probe") as any);
    c.inc();
    expect(c.value).toBe(1);
  });
});
