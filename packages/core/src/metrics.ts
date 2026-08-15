// Metrics — Counter / Gauge / Histogram over an in-process registry.
//
// Deliberately minimal (anti-goal: no metrics in the hot path). Instruments
// are plain objects with SYNC mutation methods — grab one once, mutate
// cheaply. The registry is a service so tests and exporters (e.g.
// @perfect/otel) can swap it; `Metrics.*` effects read it from context.
//
//   const program = Metrics.counter("requests").flatMap((c) =>
//     handler.tap(() => sync(() => c.inc())),
//   );
//   registry.snapshot()  // → { counters, gauges, histograms }

import { type Eff, Suspend, Op } from "./eff";
import { service, type ServiceTag } from "./service";

export type Labels = Record<string, string>;

function labelKey(name: string, labels?: Labels): string {
  if (!labels) return name;
  const parts = Object.keys(labels)
    .sort()
    .map((k) => `${k}=${labels[k]}`);
  return parts.length === 0 ? name : `${name}{${parts.join(",")}}`;
}

// ── Instruments ────────────────────────────────────────────────────

export class Counter {
  private _value = 0;

  constructor(
    readonly name: string,
    readonly labels?: Labels,
  ) {}

  inc(n = 1): void {
    this._value += n;
  }

  get value(): number {
    return this._value;
  }
}

export class Gauge {
  private _value = 0;

  constructor(
    readonly name: string,
    readonly labels?: Labels,
  ) {}

  set(value: number): void {
    this._value = value;
  }

  adjust(delta: number): void {
    this._value += delta;
  }

  get value(): number {
    return this._value;
  }
}

export const DEFAULT_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] as const;

export class Histogram {
  private _counts: number[];
  private _sum = 0;
  private _count = 0;

  constructor(
    readonly name: string,
    readonly buckets: readonly number[] = DEFAULT_BUCKETS,
    readonly labels?: Labels,
  ) {
    this._counts = new Array(buckets.length + 1).fill(0); // +1 for +Inf
  }

  record(value: number): void {
    this._sum += value;
    this._count++;
    for (let i = 0; i < this.buckets.length; i++) {
      if (value <= this.buckets[i]!) {
        this._counts[i]!++;
        return;
      }
    }
    this._counts[this.buckets.length]!++; // +Inf bucket
  }

  get sum(): number {
    return this._sum;
  }

  get count(): number {
    return this._count;
  }

  /** Cumulative count per bucket boundary (last entry = +Inf = total). */
  get bucketCounts(): number[] {
    const cumulative: number[] = [];
    let acc = 0;
    for (const c of this._counts) {
      acc += c;
      cumulative.push(acc);
    }
    return cumulative;
  }
}

// ── Registry ───────────────────────────────────────────────────────

export interface MetricsSnapshot {
  counters: Record<string, number>;
  gauges: Record<string, number>;
  histograms: Record<string, { sum: number; count: number; bucketCounts: number[] }>;
}

export class MetricsRegistry {
  private counters = new Map<string, Counter>();
  private gauges = new Map<string, Gauge>();
  private histograms = new Map<string, Histogram>();

  counter(name: string, labels?: Labels): Counter {
    const key = labelKey(name, labels);
    let c = this.counters.get(key);
    if (!c) {
      c = new Counter(name, labels);
      this.counters.set(key, c);
    }
    return c;
  }

  gauge(name: string, labels?: Labels): Gauge {
    const key = labelKey(name, labels);
    let g = this.gauges.get(key);
    if (!g) {
      g = new Gauge(name, labels);
      this.gauges.set(key, g);
    }
    return g;
  }

  histogram(name: string, opts?: { buckets?: readonly number[]; labels?: Labels }): Histogram {
    const key = labelKey(name, opts?.labels);
    let h = this.histograms.get(key);
    if (!h) {
      h = new Histogram(name, opts?.buckets ?? DEFAULT_BUCKETS, opts?.labels);
      this.histograms.set(key, h);
    }
    return h;
  }

  snapshot(): MetricsSnapshot {
    const counters: Record<string, number> = {};
    for (const [k, c] of this.counters) counters[k] = c.value;
    const gauges: Record<string, number> = {};
    for (const [k, g] of this.gauges) gauges[k] = g.value;
    const histograms: MetricsSnapshot["histograms"] = {};
    for (const [k, h] of this.histograms) {
      histograms[k] = { sum: h.sum, count: h.count, bucketCounts: h.bucketCounts };
    }
    return { counters, gauges, histograms };
  }

  clear(): void {
    this.counters.clear();
    this.gauges.clear();
    this.histograms.clear();
  }
}

export const Metrics: ServiceTag<MetricsRegistry> & {
  counter: (name: string, labels?: Labels) => Eff<Counter, never>;
  gauge: (name: string, labels?: Labels) => Eff<Gauge, never>;
  histogram: (
    name: string,
    opts?: { buckets?: readonly number[]; labels?: Labels },
  ) => Eff<Histogram, never>;
  snapshot: Eff<MetricsSnapshot, never>;
} = (() => {
  const tag = service<MetricsRegistry>("Metrics");
  const registryEff: Eff<MetricsRegistry, never> = new Suspend(Op.GetCtx, tag.key, null) as any;
  const withRegistry = <A>(f: (r: MetricsRegistry) => A): Eff<A, never> =>
    new Suspend(
      Op.FlatMap,
      registryEff as any,
      (r: MetricsRegistry) => new Suspend(Op.Sync, () => f(r), null),
    ) as any;

  return {
    ...tag,
    counter: (name: string, labels?: Labels) => withRegistry((r) => r.counter(name, labels)),
    gauge: (name: string, labels?: Labels) => withRegistry((r) => r.gauge(name, labels)),
    histogram: (name: string, opts?: { buckets?: readonly number[]; labels?: Labels }) =>
      withRegistry((r) => r.histogram(name, opts)),
    snapshot: withRegistry((r) => r.snapshot()),
  };
})();

export const defaultMetricsRegistry = new MetricsRegistry();
