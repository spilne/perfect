// OtelMetricsExporter — feed a @spilne/perfect-core MetricsRegistry into
// OpenTelemetry instruments.
//
// Counters and gauges: call export() on your export cadence (a Stream.tick
// loop, a PeriodicExportingMetricReader callback, a shutdown hook).
// Counters are exported as deltas between calls so repeated exports don't
// double-count; gauges are absolute.
//
// Histograms: every value is forwarded to OpenTelemetry the moment it is
// recorded, so OpenTelemetry sees the real values and computes correct
// percentiles. (Before, export() recorded the batch's mean once per
// observation: wrong percentiles, and work proportional to the number of
// observations on every export.) Values recorded before the exporter was
// created are not forwarded. Call close() to stop forwarding.

import type { Counter, Gauge, Histogram, Meter } from "@opentelemetry/api";
import type {
  Histogram as PerfectHistogram,
  MetricsRegistry,
  MetricsSnapshot,
} from "@spilne/perfect-core";

type Labels = Record<string, string>;

export class OtelMetricsExporter {
  private readonly lastCounters = new Map<string, number>();
  // OpenTelemetry instruments, created once per metric name.
  private readonly counters = new Map<string, Counter>();
  private readonly gauges = new Map<string, Gauge>();
  private readonly histograms = new Map<string, Histogram>();
  // Registry keys ("reqs{route=/a}") split into name and labels, once.
  private readonly parsedKeys = new Map<string, { name: string; labels: Labels }>();
  private readonly stopForwarding: () => void;

  constructor(
    private readonly meter: Meter,
    private readonly registry: MetricsRegistry,
  ) {
    this.stopForwarding = registry.onHistogramRecord((histogram, value) => {
      this.histogramFor(histogram).record(value, histogram.labels ?? {});
    });
  }

  /** Export counters and gauges. Safe to call repeatedly. */
  export(): MetricsSnapshot {
    const snap = this.registry.snapshot();

    for (const [key, value] of Object.entries(snap.counters)) {
      const delta = value - (this.lastCounters.get(key) ?? 0);
      if (delta !== 0) {
        const { name, labels } = this.parse(key);
        this.counterFor(name).add(delta, labels);
      }
      this.lastCounters.set(key, value);
    }

    for (const [key, value] of Object.entries(snap.gauges)) {
      const { name, labels } = this.parse(key);
      this.gaugeFor(name).record(value, labels);
    }

    return snap;
  }

  /** Stop forwarding histogram values. */
  close(): void {
    this.stopForwarding();
  }

  private counterFor(name: string): Counter {
    let counter = this.counters.get(name);
    if (counter === undefined) {
      counter = this.meter.createCounter(name);
      this.counters.set(name, counter);
    }
    return counter;
  }

  private gaugeFor(name: string): Gauge {
    let gauge = this.gauges.get(name);
    if (gauge === undefined) {
      gauge = this.meter.createGauge(name);
      this.gauges.set(name, gauge);
    }
    return gauge;
  }

  private histogramFor(source: PerfectHistogram): Histogram {
    let histogram = this.histograms.get(source.name);
    if (histogram === undefined) {
      // Same bucket boundaries as the registry, so both agree.
      histogram = this.meter.createHistogram(source.name, {
        advice: { explicitBucketBoundaries: [...source.buckets] },
      });
      this.histograms.set(source.name, histogram);
    }
    return histogram;
  }

  private parse(key: string): { name: string; labels: Labels } {
    let parsed = this.parsedKeys.get(key);
    if (parsed === undefined) {
      parsed = { name: stripLabels(key), labels: parseLabels(key) };
      this.parsedKeys.set(key, parsed);
    }
    return parsed;
  }
}

function stripLabels(key: string): string {
  const brace = key.indexOf("{");
  return brace === -1 ? key : key.slice(0, brace);
}

// Keys look like `name{k=v,k2=v2}`; inside the braces, `\` escapes the next
// character (core escapes \ , = { } in label names and values).
function parseLabels(key: string): Labels {
  const brace = key.indexOf("{");
  if (brace === -1) return {};
  const labels: Labels = {};
  let name = "";
  let value = "";
  let inValue = false;
  const finishPair = () => {
    if (name !== "") labels[name] = value;
    name = "";
    value = "";
    inValue = false;
  };
  for (let i = brace + 1; i < key.length - 1; i++) {
    let ch = key[i]!;
    if (ch === "\\") {
      ch = key[++i] ?? "";
    } else if (ch === "=" && !inValue) {
      inValue = true;
      continue;
    } else if (ch === ",") {
      finishPair();
      continue;
    }
    if (inValue) value += ch;
    else name += ch;
  }
  finishPair();
  return labels;
}
