// RawStream<A> — the hot-path escape hatch.
//
// Synchronous, pull-based, fused iteration with no Eff machinery: no Suspend
// nodes, no fiber, no scheduler. Terminal operators return plain values, not
// effects. Use it for tight in-memory loops where Stream's per-chunk effect
// allocation dominates; use Stream for anything that needs resources, typed
// failures, concurrency or interruption.
//
// Pure ops accumulate into the same FusibleOp buffer Stream uses, so a run of
// map/filter/filterMap/tap compiles to ONE per-element function and ONE pass
// over the source — no intermediate arrays.
//
// Position-dependent ops (take/drop/takeWhile/dropWhile/flatMap) can't fuse
// into a per-element function, so they materialize the pipeline so far into a
// fresh lazy source and start a new op buffer. Semantics stay exact — `take`
// after `filter` takes post-filter elements — while runs of pure ops on either
// side still fuse.

import { type FusibleOp, SKIP, compileFused } from "./fusion";

export class RawStream<A> implements Iterable<A> {
  private constructor(
    private readonly source: Iterable<any>,
    private readonly ops: readonly FusibleOp[],
  ) {}

  // ── Constructors ─────────────────────────────────────────────────

  static from<A>(source: Iterable<A>): RawStream<A> {
    return new RawStream<A>(source, []);
  }

  static of<A>(...values: A[]): RawStream<A> {
    return new RawStream<A>(values, []);
  }

  static empty<A = never>(): RawStream<A> {
    return new RawStream<A>([], []);
  }

  static range(start: number, end: number, step = 1): RawStream<number> {
    if (step === 0) throw new RangeError("RawStream.range: step must not be 0");
    return new RawStream<number>(
      {
        *[Symbol.iterator]() {
          if (step > 0) for (let i = start; i < end; i += step) yield i;
          else for (let i = start; i > end; i += step) yield i;
        },
      },
      [],
    );
  }

  /** Unbounded — pair with `take`/`takeWhile`. */
  static iterate<A>(seed: A, f: (a: A) => A): RawStream<A> {
    return new RawStream<A>(
      {
        *[Symbol.iterator]() {
          let current = seed;
          while (true) {
            yield current;
            current = f(current);
          }
        },
      },
      [],
    );
  }

  // ── Fusible ops ──────────────────────────────────────────────────

  private withOp<B>(op: FusibleOp): RawStream<B> {
    return new RawStream<B>(this.source, [...this.ops, op]);
  }

  map<B>(f: (a: A) => B): RawStream<B> {
    return this.withOp<B>({ _tag: "map", fn: f });
  }

  filter(predicate: (a: A) => boolean): RawStream<A> {
    return this.withOp<A>({ _tag: "filter", fn: predicate });
  }

  /** Drops elements for which `f` returns undefined. */
  filterMap<B>(f: (a: A) => B | undefined): RawStream<B> {
    return this.withOp<B>({ _tag: "filterMap", fn: f });
  }

  tap(f: (a: A) => void): RawStream<A> {
    return this.withOp<A>({ _tag: "tap", fn: f });
  }

  // ── Position-dependent ops (restart the fusion buffer) ───────────

  take(n: number): RawStream<A> {
    const upstream = this.materialize();
    return new RawStream<A>(
      {
        *[Symbol.iterator]() {
          if (n <= 0) return;
          let taken = 0;
          for (const value of upstream) {
            yield value;
            if (++taken >= n) return;
          }
        },
      },
      [],
    );
  }

  drop(n: number): RawStream<A> {
    const upstream = this.materialize();
    return new RawStream<A>(
      {
        *[Symbol.iterator]() {
          let dropped = 0;
          for (const value of upstream) {
            if (dropped < n) {
              dropped++;
              continue;
            }
            yield value;
          }
        },
      },
      [],
    );
  }

  takeWhile(predicate: (a: A) => boolean): RawStream<A> {
    const upstream = this.materialize();
    return new RawStream<A>(
      {
        *[Symbol.iterator]() {
          for (const value of upstream) {
            if (!predicate(value)) return;
            yield value;
          }
        },
      },
      [],
    );
  }

  dropWhile(predicate: (a: A) => boolean): RawStream<A> {
    const upstream = this.materialize();
    return new RawStream<A>(
      {
        *[Symbol.iterator]() {
          let dropping = true;
          for (const value of upstream) {
            if (dropping && predicate(value)) continue;
            dropping = false;
            yield value;
          }
        },
      },
      [],
    );
  }

  flatMap<B>(f: (a: A) => Iterable<B>): RawStream<B> {
    const upstream = this.materialize();
    return new RawStream<B>(
      {
        *[Symbol.iterator]() {
          for (const value of upstream) yield* f(value);
        },
      },
      [],
    );
  }

  scan<B>(zero: B, f: (acc: B, a: A) => B): RawStream<B> {
    const upstream = this.materialize();
    return new RawStream<B>(
      {
        *[Symbol.iterator]() {
          let acc = zero;
          yield acc;
          for (const value of upstream) {
            acc = f(acc, value);
            yield acc;
          }
        },
      },
      [],
    );
  }

  concat(that: RawStream<A>): RawStream<A> {
    const upstream = this.materialize();
    return new RawStream<A>(
      {
        *[Symbol.iterator]() {
          yield* upstream;
          yield* that;
        },
      },
      [],
    );
  }

  zipWithIndex(): RawStream<[A, number]> {
    const upstream = this.materialize();
    return new RawStream<[A, number]>(
      {
        *[Symbol.iterator]() {
          let index = 0;
          for (const value of upstream) yield [value, index++] as [A, number];
        },
      },
      [],
    );
  }

  // ── Iteration ────────────────────────────────────────────────────

  /** The pipeline as a lazy Iterable, with pending pure ops applied. */
  private materialize(): Iterable<A> {
    if (this.ops.length === 0) return this.source as Iterable<A>;
    const source = this.source;
    const fused = compileFused([...this.ops]);
    return {
      *[Symbol.iterator]() {
        for (const value of source) {
          const out = fused(value);
          if (out !== SKIP) yield out as A;
        }
      },
    };
  }

  [Symbol.iterator](): Iterator<A> {
    return this.materialize()[Symbol.iterator]();
  }

  // ── Terminal operators (plain values — no Eff) ───────────────────

  toArray(): A[] {
    const out: A[] = [];
    for (const value of this) out.push(value);
    return out;
  }

  forEach(f: (a: A) => void): void {
    for (const value of this) f(value);
  }

  fold<B>(zero: B, f: (acc: B, a: A) => B): B {
    let acc = zero;
    for (const value of this) acc = f(acc, value);
    return acc;
  }

  count(): number {
    let n = 0;
    for (const _ of this) n++;
    return n;
  }

  first(): A | undefined {
    for (const value of this) return value;
    return undefined;
  }

  last(): A | undefined {
    let out: A | undefined;
    for (const value of this) out = value;
    return out;
  }

  find(predicate: (a: A) => boolean): A | undefined {
    for (const value of this) if (predicate(value)) return value;
    return undefined;
  }

  some(predicate: (a: A) => boolean): boolean {
    for (const value of this) if (predicate(value)) return true;
    return false;
  }

  every(predicate: (a: A) => boolean): boolean {
    for (const value of this) if (!predicate(value)) return false;
    return true;
  }
}
