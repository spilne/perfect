import { describe, test, expect } from "bun:test";
import {
  succeed,
  fail,
  provide,
  run,
  runExit,
  Tracer,
  withSpan,
  MetricsRegistry,
} from "@perfect/core";
import { OtelTracer, OtelMetricsExporter } from "../src";

// ── Fakes over the otel API surface we touch ───────────────────────

interface FakeSpanRecord {
  name: string;
  attributes: Record<string, unknown>;
  status: { code: number; message?: string } | null;
  ended: boolean;
  exceptions: unknown[];
}

function makeFakeOtelTracer() {
  const spans: FakeSpanRecord[] = [];
  const tracer = {
    startSpan(name: string, options?: { attributes?: Record<string, unknown> }) {
      const record: FakeSpanRecord = {
        name,
        attributes: { ...options?.attributes },
        status: null,
        ended: false,
        exceptions: [],
      };
      spans.push(record);
      return {
        setAttribute(k: string, v: unknown) {
          record.attributes[k] = v;
        },
        setStatus(s: { code: number; message?: string }) {
          record.status = s;
        },
        recordException(e: unknown) {
          record.exceptions.push(e);
        },
        end() {
          record.ended = true;
        },
        spanContext: () => ({ traceId: "t", spanId: "s", traceFlags: 1 }),
        isRecording: () => true,
      };
    },
  };
  return { tracer: tracer as any, spans };
}

function makeFakeMeter() {
  const counters = new Map<string, Array<{ value: number; labels: unknown }>>();
  const gauges = new Map<string, Array<{ value: number; labels: unknown }>>();
  const histograms = new Map<string, Array<{ value: number; labels: unknown }>>();
  const recordInto =
    (map: Map<string, Array<{ value: number; labels: unknown }>>, name: string) =>
    (value: number, labels: unknown) => {
      if (!map.has(name)) map.set(name, []);
      map.get(name)!.push({ value, labels });
    };
  const meter = {
    createCounter: (name: string) => ({ add: recordInto(counters, name) }),
    createGauge: (name: string) => ({ record: recordInto(gauges, name) }),
    createHistogram: (name: string) => ({ record: recordInto(histograms, name) }),
    createUpDownCounter: (name: string) => ({ add: recordInto(counters, name) }),
  };
  return { meter: meter as any, counters, gauges, histograms };
}

// ── OtelTracer ─────────────────────────────────────────────────────

describe("OtelTracer bridge", () => {
  test("success span gets OK status, attributes, and ends", async () => {
    const { tracer, spans } = makeFakeOtelTracer();
    const program = withSpan(succeed(1), "op", { route: "/x" });
    await run(provide(program, Tracer, new OtelTracer(tracer)) as any);

    expect(spans.length).toBe(1);
    expect(spans[0]!.name).toBe("op");
    expect(spans[0]!.attributes["route"]).toBe("/x");
    expect(spans[0]!.status?.code).toBe(1); // SpanStatusCode.OK
    expect(spans[0]!.ended).toBe(true);
  });

  test("failure span gets ERROR status with message and exception", async () => {
    const { tracer, spans } = makeFakeOtelTracer();
    const err = new Error("kaput");
    const program = withSpan(fail(err), "failing");
    await runExit(provide(program, Tracer, new OtelTracer(tracer)) as any);

    expect(spans[0]!.status?.code).toBe(2); // SpanStatusCode.ERROR
    expect(spans[0]!.status?.message).toBe("kaput");
    expect(spans[0]!.exceptions).toEqual([err]);
    expect(spans[0]!.ended).toBe(true);
  });

  test("nested spans both end", async () => {
    const { tracer, spans } = makeFakeOtelTracer();
    const program = withSpan(withSpan(succeed(2), "child"), "parent");
    await run(provide(program, Tracer, new OtelTracer(tracer)) as any);

    expect(spans.map((s) => s.name).sort()).toEqual(["child", "parent"]);
    expect(spans.every((s) => s.ended)).toBe(true);
  });
});

// ── OtelMetricsExporter ────────────────────────────────────────────

describe("OtelMetricsExporter", () => {
  test("counters export deltas across repeated exports", () => {
    const registry = new MetricsRegistry();
    const { meter, counters } = makeFakeMeter();
    const exporter = new OtelMetricsExporter(meter, registry);

    registry.counter("reqs", { route: "/a" }).inc(3);
    exporter.export();
    registry.counter("reqs", { route: "/a" }).inc(2);
    exporter.export();
    exporter.export(); // no change → no export

    const recorded = counters.get("reqs")!;
    expect(recorded.map((r) => r.value)).toEqual([3, 2]);
    expect(recorded[0]!.labels).toEqual({ route: "/a" });
  });

  test("gauges export absolute values", () => {
    const registry = new MetricsRegistry();
    const { meter, gauges } = makeFakeMeter();
    const exporter = new OtelMetricsExporter(meter, registry);

    registry.gauge("pool").set(7);
    exporter.export();
    registry.gauge("pool").set(4);
    exporter.export();

    expect(gauges.get("pool")!.map((r) => r.value)).toEqual([7, 4]);
  });

  test("histograms export delta means", () => {
    const registry = new MetricsRegistry();
    const { meter, histograms } = makeFakeMeter();
    const exporter = new OtelMetricsExporter(meter, registry);

    const h = registry.histogram("lat", { buckets: [10, 100] });
    h.record(10);
    h.record(20);
    exporter.export();

    const recorded = histograms.get("lat")!;
    expect(recorded.length).toBe(2);
    expect(recorded[0]!.value).toBe(15); // delta mean
  });
});
