// exportMetrics — push a @spilne/perfect-core MetricsRegistry snapshot into
// OpenTelemetry instruments. Call it on your export cadence (a Stream.tick
// loop, an otel PeriodicExportingMetricReader callback, a shutdown hook).
//
// Counters/histograms are exported as deltas between calls so repeated
// exports don't double-count; gauges are absolute.

import type { Meter } from "@opentelemetry/api";
import type { MetricsRegistry, MetricsSnapshot } from "@spilne/perfect-core";

export class OtelMetricsExporter {
  private lastCounters: Record<string, number> = {};
  private lastHistograms: Record<string, { sum: number; count: number }> = {};

  constructor(
    private readonly meter: Meter,
    private readonly registry: MetricsRegistry,
  ) {}

  /** Export the current snapshot. Safe to call repeatedly. */
  export(): MetricsSnapshot {
    const snap = this.registry.snapshot();

    for (const [key, value] of Object.entries(snap.counters)) {
      const prev = this.lastCounters[key] ?? 0;
      const delta = value - prev;
      if (delta !== 0) {
        this.meter.createCounter(stripLabels(key)).add(delta, parseLabels(key));
      }
      this.lastCounters[key] = value;
    }

    for (const [key, value] of Object.entries(snap.gauges)) {
      this.meter.createGauge(stripLabels(key)).record(value, parseLabels(key));
    }

    for (const [key, h] of Object.entries(snap.histograms)) {
      const prev = this.lastHistograms[key] ?? { sum: 0, count: 0 };
      const deltaCount = h.count - prev.count;
      if (deltaCount > 0) {
        // otel histograms record individual values; approximate the batch
        // with the delta mean recorded deltaCount times
        const mean = (h.sum - prev.sum) / deltaCount;
        const instrument = this.meter.createHistogram(stripLabels(key));
        for (let i = 0; i < deltaCount; i++) instrument.record(mean, parseLabels(key));
      }
      this.lastHistograms[key] = { sum: h.sum, count: h.count };
    }

    return snap;
  }
}

function stripLabels(key: string): string {
  const brace = key.indexOf("{");
  return brace === -1 ? key : key.slice(0, brace);
}

function parseLabels(key: string): Record<string, string> {
  const brace = key.indexOf("{");
  if (brace === -1) return {};
  const inner = key.slice(brace + 1, -1);
  const labels: Record<string, string> = {};
  for (const pair of inner.split(",")) {
    const eq = pair.indexOf("=");
    if (eq > 0) labels[pair.slice(0, eq)] = pair.slice(eq + 1);
  }
  return labels;
}
