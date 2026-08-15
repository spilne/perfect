// Stream<A, S> — lazy, pull-based, chunked, resource-safe
//
// Internally: each step is an Eff that produces either:
//   Emit(chunk, nextStream) — a chunk of values + continuation
//   Done                    — stream exhausted
//
// Consumer drives: upstream only runs when downstream pulls.
// Backpressure is structural — no buffering, no highWaterMark.

import { type Eff, type Throws, Suspend, Op } from "../eff";
import {
  succeed,
  fail,
  failCause,
  sync,
  suspend,
  async,
  sleep,
  fork,
  interrupt,
  timeoutOption,
  retry as effRetry,
  type RetryConfig,
} from "../constructors";
import type { RetryPolicy } from "../retry-policy";
import { type Queue, Queue as QueueNS, QueueClosed } from "../queue";
import { type Deferred, Deferred as DeferredNS } from "../deferred";
import { Semaphore } from "../semaphore";
import { Cause } from "../cause";
import { clockNow } from "../clock";
import type { Exit } from "../exit";
import type { Fiber } from "../fiber";

// Run an effect to its Exit without an error channel — worker fibers carry
// full Causes to the consumer this way.
function exitOf<B>(e: Eff<B, any>): Eff<Exit<unknown, B>, never> {
  return (e as any)
    .map((value: B) => ({ _tag: "Success", value }))
    .catchAllCause((cause: Cause) => succeed({ _tag: "Failure", cause }));
}

// Finalizer for driver fibers: interrupt every fiber registered so far.
function interruptAllEff(drivers: Fiber<any>[]): Eff<void, never> {
  return suspend(() => {
    const fs = drivers.splice(0);
    return fs.reduce<Eff<void, never>>(
      (acc, f) => (acc as any).flatMap(() => interrupt(f)),
      succeed(undefined) as any,
    );
  }) as any;
}
import { Chunk } from "./chunk";
import { type FusibleOp, compileFused, hasFilterOps, SKIP } from "./fusion";

// ── Step type ──────────────────────────────────────────────────────

export type Step<A> =
  | { readonly _tag: "Emit"; readonly chunk: Chunk<A>; readonly next: Stream<A, any> }
  | { readonly _tag: "Done" };

const DONE: Step<any> = { _tag: "Done" };

function emit<A>(chunk: Chunk<A>, next: Stream<A, any>): Step<A> {
  return { _tag: "Emit", chunk, next };
}

// ── Stream class ───────────────────────────────────────────────────

// ── Fusion helper ──────────────────────────────────────────────────
// Wraps a raw step so every emitted chunk has `compiled` applied. Recursive
// through s.next so continuations inherit the same compilation.
function fuseStep<A, S>(
  rawStep: Eff<Step<A>, S>,
  compiled: (v: any) => any,
  hasFilter: boolean,
): Eff<Step<A>, S> {
  return (rawStep as any).map((s: Step<A>) => {
    if (s._tag === "Done") return DONE;
    let out: Chunk<A>;
    if (hasFilter) {
      // Use the underlying array directly — avoids per-element method-call overhead.
      const src = s.chunk.toArray();
      const len = src.length;
      const arr: any[] = [];
      for (let i = 0; i < len; i++) {
        const v = compiled(src[i]);
        if (v !== SKIP) arr.push(v);
      }
      out = Chunk.fromArray(arr);
    } else {
      out = s.chunk.map(compiled);
    }
    // s.next.step triggers s.next's own flush (if it has _pending).
    // We then wrap its raw step with THIS compilation.
    const nextStream = new Stream<A, any>(
      fuseStep(s.next.step, compiled, hasFilter),
      s.next._finalizer,
    );
    return { _tag: "Emit", chunk: out, next: nextStream };
  });
}

export class Stream<A, S = never> {
  // Raw step; `step` getter below flushes pending fusible ops lazily.
  private _rawStep: Eff<Step<A>, S>;
  // Accumulated pure ops (map/filter/filterMap/tap) awaiting compilation.
  private _pending: FusibleOp[] = [];
  _finalizer: Eff<void, any> | null;

  constructor(step: Eff<Step<A>, S>, finalizer: Eff<void, any> | null = null) {
    this._rawStep = step;
    this._finalizer = finalizer;
  }

  /** The stream's step effect. Flushes any pending fused ops before returning. */
  get step(): Eff<Step<A>, S> {
    if (this._pending.length === 0) return this._rawStep;
    const ops = this._pending;
    const compiled = compileFused(ops);
    const fused = fuseStep<A, S>(this._rawStep, compiled, hasFilterOps(ops));
    // Cache the fused step so subsequent reads are O(1).
    this._rawStep = fused;
    this._pending = [];
    return fused;
  }

  /** Append a pure op to the fusion buffer; returns a new Stream sharing the
   *  raw step but with its own pending list. */
  private _withOp(op: FusibleOp): Stream<any, S> {
    const s = new Stream<any, S>(this._rawStep, this._finalizer);
    s._pending = this._pending.length === 0 ? [op] : [...this._pending, op];
    return s;
  }

  private _withFinalizer<S2>(finalizer: Eff<void, S2>): Stream<A, S | S2> {
    const combined =
      this._finalizer === null
        ? finalizer
        : (new Suspend(Op.Ensuring, this._finalizer, finalizer) as any);
    const s = new Stream<A, any>(this._rawStep, combined as any);
    s._pending = this._pending.slice();
    return s as Stream<A, S | S2>;
  }

  private _finalize<B, S2>(eff: Eff<B, S2>): Eff<B, S2> {
    if (this._finalizer === null) return eff;
    return new Suspend(Op.Ensuring, eff, this._finalizer) as any;
  }

  // ── Constructors ─────────────────────────────────────────────────

  static empty<A = never>(): Stream<A, never> {
    return new Stream(succeed(DONE));
  }

  static succeed<A>(value: A): Stream<A, never> {
    return new Stream(succeed(emit(Chunk.single(value), Stream.empty())));
  }

  static of<A>(...values: A[]): Stream<A, never> {
    if (values.length === 0) return Stream.empty();
    return new Stream(succeed(emit(Chunk.fromArray(values), Stream.empty())));
  }

  static fromChunk<A>(chunk: Chunk<A>): Stream<A, never> {
    if (chunk.isEmpty) return Stream.empty();
    return new Stream(succeed(emit(chunk, Stream.empty())));
  }

  static fromArray<A>(arr: ReadonlyArray<A>): Stream<A, never> {
    return Stream.fromChunk(Chunk.fromArray(arr));
  }

  static fromIterable<A>(iter: Iterable<A>): Stream<A, never> {
    return Stream.fromArray(Array.from(iter));
  }

  static fromEffect<A, S>(eff: Eff<A, S>): Stream<A, S> {
    return new Stream((eff as any).map((a: A) => emit(Chunk.single(a), Stream.empty())));
  }

  static fail<E>(error: E): Stream<never, Throws<E>> {
    return new Stream(fail(error) as any);
  }

  static suspend<A, S>(f: () => Stream<A, S>): Stream<A, S> {
    let current: Stream<A, S> | null = null;
    return new Stream(
      suspend(() => {
        current = f();
        return current.step;
      }),
      suspend(() => {
        const finalizer = current?._finalizer;
        current = null;
        return finalizer ?? succeed(undefined);
      }),
    );
  }

  static unfold<A, B>(seed: B, f: (b: B) => [A, B] | null): Stream<A, never> {
    function go(s: B): Stream<A, never> {
      return new Stream(
        sync(() => {
          const result = f(s);
          if (result === null) return DONE;
          const [value, next] = result;
          return emit(Chunk.single(value), go(next));
        }),
      );
    }
    return go(seed);
  }

  static unfoldEffect<A, B, S>(seed: B, f: (b: B) => Eff<[A, B] | null, S>): Stream<A, S> {
    function go(s: B): Stream<A, S> {
      return new Stream(
        (f(s) as any).map((result: [A, B] | null) => {
          if (result === null) return DONE;
          const [value, next] = result;
          return emit(Chunk.single(value), go(next));
        }),
      );
    }
    return go(seed);
  }

  static iterate<A>(seed: A, f: (a: A) => A): Stream<A, never> {
    return Stream.unfold(seed, (s) => [s, f(s)]);
  }

  static range(start: number, end: number, step = 1): Stream<number, never> {
    // Emit in chunks of up to 4096 — avoids one Suspend per element and
    // stays memory-bounded on very large ranges.
    const CHUNK_SIZE = 4096;
    function chunkFrom(from: number): Stream<number, never> {
      if (from >= end) return Stream.empty();
      const remaining = Math.max(0, Math.ceil((end - from) / step));
      const size = Math.min(remaining, CHUNK_SIZE);
      const arr = new Array<number>(size);
      for (let i = 0; i < size; i++) arr[i] = from + i * step;
      return new Stream(succeed(emit(Chunk.fromArray(arr), chunkFrom(from + size * step))));
    }
    return chunkFrom(start);
  }

  static repeat<A, S>(eff: Eff<A, S>): Stream<A, S> {
    const self: Stream<A, S> = new Stream((eff as any).map((a: A) => emit(Chunk.single(a), self)));
    return self;
  }

  static repeatValue<A>(value: A): Stream<A, never> {
    return Stream.suspend(() => {
      const chunk = Chunk.single(value);
      return new Stream(succeed(emit(chunk, Stream.repeatValue(value))));
    });
  }

  static tick(intervalMs: number): Stream<void, never> {
    function go(): Stream<void, never> {
      return new Stream(
        (sleep(intervalMs) as any).map(() => emit(Chunk.single(undefined as void), go())),
      );
    }
    return go();
  }

  static fromQueue<A>(queue: { take(): Eff<A, any> }): Stream<A, any> {
    function go(): Stream<A, any> {
      return new Stream(
        (queue.take() as any)
          .map((a: A) => emit(Chunk.single(a), go()))
          // queue shutdown ends the stream; any other failure propagates
          .catch((e: unknown) => (e instanceof QueueClosed ? DONE : fail(e))),
      );
    }
    return go();
  }

  // ── Push-source bridges ──────────────────────────────────────────
  //
  // Wrap a push-style source (callback, EventEmitter) into a pull-based
  // Stream via an internal buffer. When the buffer fills, new emits are
  // dropped silently — pass a bufferSize you're comfortable with, or use
  // fromQueue(Queue.bounded(N)) directly for proper backpressure.

  /**
   * Create a Stream from a push-callback API.
   *
   * @param register called once with `emit(value)` and `close()`; may return
   *   a cleanup function that fires when the stream terminates.
   * @param bufferSize how many queued items to hold before dropping (default: 1024).
   */
  static fromCallback<A>(
    register: (emit: (value: A) => void, close: () => void) => (() => void) | void,
    bufferSize = 1024,
  ): Stream<A, never> {
    return Stream.suspend(() => {
      const buffer: A[] = [];
      let closed = false;
      let cleanup: (() => void) | void;
      let cleaned = false;
      let waiter: ((step: Step<A>) => void) | null = null;

      const cleanupOnce = (): void => {
        if (cleaned) return;
        cleaned = true;
        if (cleanup) cleanup();
      };

      const pushEmit = (value: A): void => {
        if (closed) return;
        if (waiter !== null) {
          const w = waiter;
          waiter = null;
          w(emit(Chunk.single(value), next()));
          return;
        }
        if (buffer.length < bufferSize) buffer.push(value);
        // else drop
      };
      const pushClose = (): void => {
        if (closed) return;
        closed = true;
        if (waiter !== null && buffer.length === 0) {
          const w = waiter;
          waiter = null;
          cleanupOnce();
          w(DONE);
        }
      };

      cleanup = register(pushEmit, pushClose) ?? undefined;

      function next(): Stream<A, never> {
        return new Stream(
          new Suspend(
            Op.Async,
            (resume: (eff: any) => void) => {
              if (buffer.length > 0) {
                const chunkArr: A[] = buffer.splice(0, buffer.length);
                resume(succeed(emit(Chunk.fromArray(chunkArr), next())));
                return;
              }
              if (closed) {
                cleanupOnce();
                resume(succeed(DONE));
                return;
              }
              waiter = (step) => resume(succeed(step));
              // interrupt handle: drop the waiter so a late emit doesn't call into nothing
              return () => {
                closed = true;
                buffer.length = 0;
                waiter = null;
                cleanupOnce();
              };
            },
            null,
          ) as any,
        );
      }

      return next()._withFinalizer(sync(cleanupOnce));
    });
  }

  /**
   * Create a Stream from a Node-style EventEmitter.
   * Listens for `event`; the stream terminates if a `close`/`end`/`finish`/`error`
   * event is observed. On cleanup the listener is removed.
   */
  static fromEventEmitter<A = unknown>(
    emitter: {
      on(event: string, listener: (...args: any[]) => void): any;
      off?(event: string, listener: (...args: any[]) => void): any;
      removeListener?(event: string, listener: (...args: any[]) => void): any;
    },
    event: string,
    bufferSize = 1024,
  ): Stream<A, never> {
    const removeListener = (ev: string, l: (...args: any[]) => void) => {
      if (typeof emitter.off === "function") emitter.off(ev, l);
      else if (typeof emitter.removeListener === "function") emitter.removeListener(ev, l);
    };
    return Stream.fromCallback<A>((emit, close) => {
      const onValue = (...args: any[]) => emit(args.length <= 1 ? args[0] : (args as any));
      const onEnd = () => close();
      emitter.on(event, onValue);
      emitter.on("error", onEnd);
      emitter.on("end", onEnd);
      emitter.on("close", onEnd);
      emitter.on("finish", onEnd);
      return () => {
        removeListener(event, onValue);
        removeListener("error", onEnd);
        removeListener("end", onEnd);
        removeListener("close", onEnd);
        removeListener("finish", onEnd);
      };
    }, bufferSize);
  }

  /**
   * Like fromCallback, but the registration itself is an effect — useful when
   * setting up the push source requires IO (opening a socket, subscribing).
   * The cleanup effect runs when the stream terminates.
   */
  static async<A, S>(
    register: (emit: (value: A) => void, close: () => void) => Eff<(() => void) | void, S>,
    bufferSize = 1024,
  ): Stream<A, S> {
    return new Stream(
      (succeed(null) as any).flatMap(() => {
        const buffer: A[] = [];
        let closed = false;
        let waiter: ((step: Step<A>) => void) | null = null;
        let cleanup: (() => void) | void;
        let cleaned = false;

        const cleanupOnce = (): void => {
          if (cleaned) return;
          cleaned = true;
          if (cleanup) cleanup();
        };

        const pushEmit = (value: A) => {
          if (closed) return;
          if (waiter !== null) {
            const w = waiter;
            waiter = null;
            w(emit(Chunk.single(value), next()));
            return;
          }
          if (buffer.length < bufferSize) buffer.push(value);
        };
        const pushClose = () => {
          if (closed) return;
          closed = true;
          if (waiter !== null && buffer.length === 0) {
            const w = waiter;
            waiter = null;
            cleanupOnce();
            w(DONE);
          }
        };

        function next(): Stream<A, never> {
          return new Stream(
            new Suspend(
              Op.Async,
              (resume: (eff: any) => void) => {
                if (buffer.length > 0) {
                  const chunkArr: A[] = buffer.splice(0, buffer.length);
                  resume(succeed(emit(Chunk.fromArray(chunkArr), next())));
                  return;
                }
                if (closed) {
                  cleanupOnce();
                  resume(succeed(DONE));
                  return;
                }
                waiter = (step) => resume(succeed(step));
                return () => {
                  closed = true;
                  buffer.length = 0;
                  waiter = null;
                  cleanupOnce();
                };
              },
              null,
            ) as any,
          );
        }

        return (register(pushEmit, pushClose) as any)
          .map((c: (() => void) | void) => {
            cleanup = c ?? undefined;
            return next()._withFinalizer(sync(cleanupOnce)).step;
          })
          .flatMap((s: any) => s);
      }),
    );
  }

  static bracket<A, S>(acquire: Eff<A, S>, release: (a: A) => Eff<void, never>): Stream<A, S> {
    return new Stream(
      (acquire as any).map((resource: A) => {
        const done = new Stream<A, never>(
          new Suspend(Op.Ensuring, succeed(DONE), release(resource)) as any,
        );
        return emit(Chunk.single(resource), done);
      }),
    );
  }

  // ── Transform operators ──────────────────────────────────────────

  map<B>(f: (a: A) => B): Stream<B, S> {
    return this._withOp({ _tag: "map", fn: f as any }) as Stream<B, S>;
  }

  /** Like `map` but returning `undefined` means "skip this element". Fused
   *  together with adjacent map/filter/tap ops for a single chunk walk. */
  filterMap<B>(f: (a: A) => B | undefined): Stream<B, S> {
    return this._withOp({ _tag: "filterMap", fn: f as any }) as Stream<B, S>;
  }

  mapChunks<B>(f: (chunk: Chunk<A>) => Chunk<B>): Stream<B, S> {
    return new Stream(
      (this.step as any).map((s: Step<A>) => {
        if (s._tag === "Done") return DONE;
        return emit(f(s.chunk), s.next.mapChunks(f));
      }),
      this._finalizer,
    );
  }

  /**
   * Keep (or drop) elements matching the predicate.
   * @param p predicate
   * @param action "keep" (default) keeps matches; "drop" inverts.
   */
  filter(p: (a: A) => boolean, action: "keep" | "drop" = "keep"): Stream<A, S> {
    const fn: (a: A) => boolean = action === "drop" ? (a) => !p(a) : p;
    return this._withOp({ _tag: "filter", fn: fn as any }) as Stream<A, S>;
  }

  /** Drop null/undefined values. Fused with adjacent pure ops. */
  unNone(): Stream<NonNullable<A>, S> {
    return this._withOp({ _tag: "filter", fn: (a: any) => a != null } as any) as Stream<
      NonNullable<A>,
      S
    >;
  }

  collect<B>(f: (a: A) => B | undefined): Stream<B, S> {
    return this.map(f).filter((b): b is B => b !== undefined) as any;
  }

  flatMap<B, S2>(f: (a: A) => Stream<B, S2>): Stream<B, S | S2> {
    // Pull one element at a time, drain its inner stream, then move on.
    // Avoids the O(N²) concat-reduce the previous implementation used.
    type OuterS = S;
    const self = this;

    const drainInner = (
      inner: Stream<B, S2>,
      outer: Stream<A, OuterS>,
      pending: Chunk<A>,
      idx: number,
    ): Stream<B, OuterS | S2> =>
      new Stream(
        (inner.step as any).flatMap((s: Step<B>) => {
          if (s._tag === "Emit") {
            return succeed(emit(s.chunk, drainInner(s.next, outer, pending, idx + 0)));
          }
          return nextElement(outer, pending, idx + 1).step;
        }),
      );

    const nextElement = (
      outer: Stream<A, OuterS>,
      pending: Chunk<A>,
      idx: number,
    ): Stream<B, OuterS | S2> => {
      if (idx < pending.length) {
        return drainInner(f(pending.get(idx)), outer, pending, idx);
      }
      return new Stream(
        (outer.step as any).flatMap((s: Step<A>) => {
          if (s._tag === "Done") return succeed(DONE);
          return nextElement(s.next, s.chunk, 0).step;
        }),
      );
    };

    return nextElement(self, Chunk.empty(), 0);
  }

  evalMap<B, S2>(f: (a: A) => Eff<B, S2>): Stream<B, S | S2> {
    return new Stream(
      (this.step as any).flatMap((s: Step<A>) => {
        if (s._tag === "Done") return succeed(DONE);
        return evalMapChunk(s.chunk, f).map((mapped: Chunk<B>) => emit(mapped, s.next.evalMap(f)));
      }),
      this._finalizer,
    );
  }

  evalFilter<S2>(p: (a: A) => Eff<boolean, S2>): Stream<A, S | S2> {
    return this.evalMap<A | typeof FILTER_SENTINEL, S2>((a) =>
      (p(a) as any).map((b: boolean) => (b ? a : FILTER_SENTINEL)),
    ).filter((a): a is A => a !== FILTER_SENTINEL) as any;
  }

  take(n: number): Stream<A, S> {
    if (n <= 0) return new Stream(succeed(DONE), this._finalizer);
    return new Stream(
      (this.step as any).map((s: Step<A>) => {
        if (s._tag === "Done") return DONE;
        if (s.chunk.length <= n) {
          return emit(s.chunk, s.next.take(n - s.chunk.length));
        }
        return emit(s.chunk.take(n), Stream.empty());
      }),
      this._finalizer,
    );
  }

  drop(n: number): Stream<A, S> {
    if (n <= 0) return this;
    return new Stream(
      (this.step as any)
        .map((s: Step<A>) => {
          if (s._tag === "Done") return DONE;
          if (n >= s.chunk.length) {
            return s.next.drop(n - s.chunk.length).step;
          }
          return emit(s.chunk.drop(n), s.next);
        })
        .flatMap((r: any) => (r instanceof Suspend ? r : succeed(r))),
      this._finalizer,
    );
  }

  takeWhile(p: (a: A) => boolean): Stream<A, S> {
    return new Stream(
      (this.step as any).map((s: Step<A>) => {
        if (s._tag === "Done") return DONE;
        const taken: A[] = [];
        for (const item of s.chunk) {
          if (!p(item)) return emit(Chunk.fromArray(taken), Stream.empty());
          taken.push(item);
        }
        return emit(Chunk.fromArray(taken), s.next.takeWhile(p));
      }),
      this._finalizer,
    );
  }

  dropWhile(p: (a: A) => boolean): Stream<A, S> {
    return new Stream(
      (this.step as any)
        .map((s: Step<A>) => {
          if (s._tag === "Done") return DONE;
          let dropCount = 0;
          for (const item of s.chunk) {
            if (!p(item)) break;
            dropCount++;
          }
          if (dropCount === s.chunk.length) {
            return s.next.dropWhile(p).step;
          }
          return emit(s.chunk.drop(dropCount), s.next);
        })
        .flatMap((r: any) => (r instanceof Suspend ? r : succeed(r))),
      this._finalizer,
    );
  }

  scan<B>(zero: B, f: (acc: B, a: A) => B): Stream<B, S> {
    function go(acc: B, stream: Stream<A, any>): Stream<B, any> {
      return new Stream(
        (stream.step as any).map((s: Step<A>) => {
          if (s._tag === "Done") return DONE;
          const results: B[] = [];
          let current = acc;
          for (const item of s.chunk) {
            current = f(current, item);
            results.push(current);
          }
          return emit(Chunk.fromArray(results), go(current, s.next));
        }),
        stream._finalizer,
      );
    }
    return new Stream(succeed(emit(Chunk.single(zero), go(zero, this))), this._finalizer);
  }

  tap(f: (a: A) => void): Stream<A, S> {
    return this._withOp({ _tag: "tap", fn: f as any });
  }

  tapEffect<S2>(f: (a: A) => Eff<any, S2>): Stream<A, S | S2> {
    return this.evalMap((a) => (f(a) as any).map(() => a));
  }

  zipWithIndex(): Stream<[A, number], S> {
    let index = 0;
    return this.map((a) => [a, index++] as [A, number]);
  }

  changes(): Stream<A, S> {
    let last: A | typeof SENTINEL = SENTINEL;
    return this.filter((a) => {
      if (a === last) return false;
      last = a;
      return true;
    });
  }

  // ── Batching ─────────────────────────────────────────────────────

  grouped(size: number): Stream<Chunk<A>, S> {
    function go(buffer: A[], stream: Stream<A, any>): Stream<Chunk<A>, any> {
      return new Stream(
        (stream.step as any)
          .map((s: Step<A>) => {
            if (s._tag === "Done") {
              if (buffer.length > 0) {
                return emit(Chunk.single(Chunk.fromArray(buffer)), Stream.empty());
              }
              return DONE;
            }
            const combined = [...buffer];
            for (const item of s.chunk) combined.push(item);
            const groups: Chunk<A>[] = [];
            let i = 0;
            while (i + size <= combined.length) {
              groups.push(Chunk.fromArray(combined.slice(i, i + size)));
              i += size;
            }
            const remainder = combined.slice(i);
            const next = go(remainder, s.next);
            if (groups.length === 0) return next.step;
            return emit(Chunk.fromArray(groups), next);
          })
          .flatMap((r: any) => (r instanceof Suspend ? r : succeed(r))),
        stream._finalizer,
      );
    }
    return go([], this);
  }

  rechunk(size: number): Stream<A, S> {
    return this.grouped(size).flatMap((chunk) => Stream.fromChunk(chunk));
  }

  // ── Combination ──────────────────────────────────────────────────

  concat<S2>(that: Stream<A, S2>): Stream<A, S | S2> {
    return new Stream(
      (this.step as any)
        .map((s: Step<A>) => {
          if (s._tag === "Done") return that.step;
          return emit(s.chunk, s.next.concat(that));
        })
        .flatMap((r: any) => (r instanceof Suspend ? r : succeed(r))),
      this._finalizer === null
        ? that._finalizer
        : that._finalizer === null
          ? this._finalizer
          : (new Suspend(Op.Ensuring, this._finalizer, that._finalizer) as any),
    );
  }

  zip<B, S2>(that: Stream<B, S2>): Stream<[A, B], S | S2> {
    return this.zipWith(that, (a, b) => [a, b] as [A, B]);
  }

  zipWith<B, C, S2>(that: Stream<B, S2>, f: (a: A, b: B) => C): Stream<C, S | S2> {
    function go(
      left: Stream<A, any>,
      right: Stream<B, any>,
      leftBuf: Chunk<A>,
      rightBuf: Chunk<B>,
    ): Stream<C, any> {
      // if both buffers have elements, pair them
      if (!leftBuf.isEmpty && !rightBuf.isEmpty) {
        const len = Math.min(leftBuf.length, rightBuf.length);
        const results: C[] = [];
        for (let i = 0; i < len; i++) {
          results.push(f(leftBuf.get(i), rightBuf.get(i)));
        }
        return new Stream(
          succeed(
            emit(Chunk.fromArray(results), go(left, right, leftBuf.drop(len), rightBuf.drop(len))),
          ),
        );
      }

      // need to pull from whichever buffer is empty
      if (leftBuf.isEmpty) {
        return new Stream(
          (left.step as any).flatMap((s: Step<A>) => {
            if (s._tag === "Done") return succeed(DONE);
            return go(s.next, right, s.chunk, rightBuf).step;
          }),
        );
      }

      return new Stream(
        (right.step as any).flatMap((s: Step<B>) => {
          if (s._tag === "Done") return succeed(DONE);
          return go(left, s.next, leftBuf, s.chunk).step;
        }),
      );
    }

    return go(this, that, Chunk.empty(), Chunk.empty());
  }

  interleave<S2>(that: Stream<A, S2>): Stream<A, S | S2> {
    return this.zip(that).flatMap(([a, b]) => Stream.of(a, b));
  }

  merge<S2>(that: Stream<A, S2>): Stream<A, S | S2> {
    // Concurrent merge on the fiber runtime: one driver fiber per source,
    // both offering chunks into a shared bounded queue (backpressure).
    // Drivers are structured children of the consuming fiber; the stream
    // finalizer interrupts them on early termination.
    const self = this;
    type Slot =
      | { _tag: "chunk"; chunk: Chunk<A> }
      | { _tag: "end" }
      | { _tag: "fail"; cause: Cause };

    const drivers: Fiber<any>[] = [];

    const drain = (slots: Queue<Slot>, s: Stream<A, any>): Eff<void, any> =>
      (s.step as any)
        .flatMap((step: Step<A>) =>
          step._tag === "Done"
            ? slots.offer({ _tag: "end" })
            : (slots.offer({ _tag: "chunk", chunk: step.chunk }) as any).flatMap(() =>
                drain(slots, step.next),
              ),
        )
        .catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause) ? failCause(cause) : slots.offer({ _tag: "fail", cause }),
        );

    const pull = (slots: Queue<Slot>, open: { count: number }): Eff<Step<A>, any> =>
      (slots.take() as any).flatMap((slot: Slot): any => {
        if (slot._tag === "fail") return failCause(slot.cause);
        if (slot._tag === "end") {
          open.count--;
          return open.count === 0 ? succeed(DONE) : pull(slots, open);
        }
        return succeed(emit(slot.chunk, new Stream(suspend(() => pull(slots, open)) as any)));
      });

    const setup: Eff<Step<A>, any> = (QueueNS.bounded<Slot>(2) as any).flatMap(
      (slots: Queue<Slot>) =>
        (fork(drain(slots, self) as any) as any).flatMap((f1: Fiber<any>) =>
          (fork(drain(slots, that as any) as any) as any).flatMap((f2: Fiber<any>) => {
            drivers.push(f1, f2);
            return pull(slots, { count: 2 });
          }),
        ),
    );

    return new Stream<A, any>(suspend(() => setup) as any).onFinalize(
      interruptAllEff(drivers),
    ) as any;
  }

  // ── Concurrency ──────────────────────────────────────────────────
  //
  // Both parEvalMap variants share a shape: a driver fiber pulls the source
  // and forks one worker per item, bounded by a semaphore; results reach the
  // consumer through a bounded queue (backpressure). After the source ends
  // the driver atomically re-acquires every permit — a barrier for in-flight
  // workers — before signalling end. Everything runs on the fiber runtime,
  // so interrupting the consumer propagates to driver and workers, and no
  // work escapes structured concurrency.

  parEvalMap<B, S2>(concurrency: number, f: (a: A) => Eff<B, S2>): Stream<B, S | S2> {
    // ordered: each input claims a queue slot holding a Deferred; workers
    // resolve their Deferred whenever they finish, the consumer awaits slots
    // in input order.
    const self = this;
    const n = Math.max(1, Math.floor(concurrency));
    type Slot =
      | { _tag: "item"; deferred: Deferred<Exit<unknown, B>> }
      | { _tag: "end" }
      | { _tag: "fail"; cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<B>, any> = (QueueNS.bounded<Slot>(n) as any).flatMap(
      (slots: Queue<Slot>) =>
        (Semaphore.make(n) as any).flatMap((sem: Semaphore) => {
          const enqueue = (item: A): Eff<void, any> =>
            (sem.acquire() as any).flatMap(() =>
              (DeferredNS.make<Exit<unknown, B>>() as any).flatMap(
                (d: Deferred<Exit<unknown, B>>) =>
                  (slots.offer({ _tag: "item", deferred: d }) as any).flatMap(() =>
                    fork(
                      (exitOf(f(item)) as any).flatMap((exit: Exit<unknown, B>) =>
                        (d.succeed(exit) as any).flatMap(() => sem.release()),
                      ),
                    ),
                  ),
              ),
            );

          const drainChunk = (items: A[], i: number, next: Stream<A, any>): Eff<void, any> =>
            i >= items.length
              ? drain(next)
              : (enqueue(items[i]!) as any).flatMap(() => drainChunk(items, i + 1, next));

          const drain = (s: Stream<A, any>): Eff<void, any> =>
            (s.step as any).flatMap((step: Step<A>) =>
              step._tag === "Done"
                ? (sem.withPermits(n, succeed(undefined)) as any).flatMap(() =>
                    slots.offer({ _tag: "end" }),
                  )
                : drainChunk(Array.from(step.chunk), 0, step.next),
            );

          const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
            Cause.isInterruptedOnly(cause)
              ? failCause(cause)
              : slots.offer({ _tag: "fail", cause }),
          );

          const pull = (): Eff<Step<B>, any> =>
            (slots.take() as any).flatMap((slot: Slot): any => {
              if (slot._tag === "fail") return failCause(slot.cause);
              if (slot._tag === "end") return succeed(DONE);
              return (slot.deferred.await as any).flatMap((exit: Exit<unknown, B>) =>
                exit._tag === "Success"
                  ? succeed(
                      emit(Chunk.single(exit.value), new Stream(suspend(() => pull()) as any)),
                    )
                  : failCause(exit.cause),
              );
            });

          return (fork(driver) as any).flatMap((fb: Fiber<any>) => {
            drivers.push(fb);
            return pull();
          });
        }),
    );

    return new Stream<B, any>(suspend(() => setup) as any).onFinalize(
      interruptAllEff(drivers),
    ) as any;
  }

  parEvalMapUnordered<B, S2>(concurrency: number, f: (a: A) => Eff<B, S2>): Stream<B, S | S2> {
    // unordered: workers offer results directly as they complete. A worker
    // blocked on offer still holds its permit, so at most `concurrency`
    // results are buffered.
    const self = this;
    const n = Math.max(1, Math.floor(concurrency));
    type Slot =
      | { _tag: "item"; exit: Exit<unknown, B> }
      | { _tag: "end" }
      | { _tag: "fail"; cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<B>, any> = (QueueNS.bounded<Slot>(n) as any).flatMap(
      (slots: Queue<Slot>) =>
        (Semaphore.make(n) as any).flatMap((sem: Semaphore) => {
          const enqueue = (item: A): Eff<void, any> =>
            (sem.acquire() as any).flatMap(() =>
              fork(
                (exitOf(f(item)) as any).flatMap((exit: Exit<unknown, B>) =>
                  (slots.offer({ _tag: "item", exit }) as any).flatMap(() => sem.release()),
                ),
              ),
            );

          const drainChunk = (items: A[], i: number, next: Stream<A, any>): Eff<void, any> =>
            i >= items.length
              ? drain(next)
              : (enqueue(items[i]!) as any).flatMap(() => drainChunk(items, i + 1, next));

          const drain = (s: Stream<A, any>): Eff<void, any> =>
            (s.step as any).flatMap((step: Step<A>) =>
              step._tag === "Done"
                ? (sem.withPermits(n, succeed(undefined)) as any).flatMap(() =>
                    slots.offer({ _tag: "end" }),
                  )
                : drainChunk(Array.from(step.chunk), 0, step.next),
            );

          const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
            Cause.isInterruptedOnly(cause)
              ? failCause(cause)
              : slots.offer({ _tag: "fail", cause }),
          );

          const pull = (): Eff<Step<B>, any> =>
            (slots.take() as any).flatMap((slot: Slot): any => {
              if (slot._tag === "fail") return failCause(slot.cause);
              if (slot._tag === "end") return succeed(DONE);
              return slot.exit._tag === "Success"
                ? succeed(
                    emit(Chunk.single(slot.exit.value), new Stream(suspend(() => pull()) as any)),
                  )
                : failCause(slot.exit.cause);
            });

          return (fork(driver) as any).flatMap((fb: Fiber<any>) => {
            drivers.push(fb);
            return pull();
          });
        }),
    );

    return new Stream<B, any>(suspend(() => setup) as any).onFinalize(
      interruptAllEff(drivers),
    ) as any;
  }

  // ── Time-based operators ─────────────────────────────────────────
  //
  // Timing lives on the consumer side and is Clock-routed (timeoutOption /
  // clockNow), so a TestClock drives these deterministically. A driver
  // fiber pumps the source into a queue; sentinel slots carry end/failure.

  groupWithin(maxSize: number, timeoutMs: number): Stream<Chunk<A>, S> {
    const self = this;
    const cap = Math.max(1, maxSize);
    type Slot = { _tag: "item"; value: A } | { _tag: "end" } | { _tag: "fail"; cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<Chunk<A>>, any> = (QueueNS.bounded<Slot>(cap) as any).flatMap(
      (slots: Queue<Slot>) => {
        const offerChunk = (items: A[], i: number, next: Stream<A, any>): Eff<void, any> =>
          i >= items.length
            ? drain(next)
            : (slots.offer({ _tag: "item", value: items[i]! }) as any).flatMap(() =>
                offerChunk(items, i + 1, next),
              );

        const drain = (s: Stream<A, any>): Eff<void, any> =>
          (s.step as any).flatMap((step: Step<A>) =>
            step._tag === "Done"
              ? slots.offer({ _tag: "end" })
              : offerChunk(Array.from(step.chunk), 0, step.next),
          );

        const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause) ? failCause(cause) : slots.offer({ _tag: "fail", cause }),
        );

        // the window opens when the first item of a batch arrives; the batch
        // flushes at maxSize items, window expiry, or end of input
        const collect = (buf: A[], deadline: number): Eff<Step<Chunk<A>>, any> => {
          if (buf.length >= maxSize) return emitBuf(buf);
          return (clockNow as any).flatMap((now: number) => {
            const remaining = deadline - now;
            if (remaining <= 0) return emitBuf(buf);
            return (timeoutOption(slots.take() as any, remaining) as any).flatMap(
              (slot: Slot | undefined): any => {
                if (slot === undefined) return emitBuf(buf);
                if (slot._tag === "fail") return failCause(slot.cause);
                if (slot._tag === "end")
                  return succeed(emit(Chunk.single(Chunk.fromArray(buf)), Stream.empty()));
                buf.push(slot.value);
                return collect(buf, deadline);
              },
            );
          });
        };

        const emitBuf = (buf: A[]): Eff<Step<Chunk<A>>, any> =>
          succeed(
            emit(Chunk.single(Chunk.fromArray(buf)), new Stream(suspend(() => firstPull()) as any)),
          );

        const firstPull = (): Eff<Step<Chunk<A>>, any> =>
          (slots.take() as any).flatMap((slot: Slot): any => {
            if (slot._tag === "fail") return failCause(slot.cause);
            if (slot._tag === "end") return succeed(DONE);
            return (clockNow as any).flatMap((now: number) =>
              collect([slot.value], now + timeoutMs),
            );
          });

        return (fork(driver) as any).flatMap((fb: Fiber<any>) => {
          drivers.push(fb);
          return firstPull();
        });
      },
    );

    return new Stream<Chunk<A>, any>(suspend(() => setup) as any).onFinalize(
      interruptAllEff(drivers),
    ) as any;
  }

  debounce(ms: number): Stream<A, S> {
    // emit the latest value once `ms` elapses with no newer one; the driver
    // free-runs (unbounded queue) and the consumer conflates to the latest
    const self = this;
    type Slot = { _tag: "item"; value: A } | { _tag: "end" } | { _tag: "fail"; cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<A>, any> = (QueueNS.unbounded<Slot>() as any).flatMap(
      (slots: Queue<Slot>) => {
        const offerChunk = (items: A[], i: number, next: Stream<A, any>): Eff<void, any> =>
          i >= items.length
            ? drain(next)
            : (slots.offer({ _tag: "item", value: items[i]! }) as any).flatMap(() =>
                offerChunk(items, i + 1, next),
              );

        const drain = (s: Stream<A, any>): Eff<void, any> =>
          (s.step as any).flatMap((step: Step<A>) =>
            step._tag === "Done"
              ? slots.offer({ _tag: "end" })
              : offerChunk(Array.from(step.chunk), 0, step.next),
          );

        const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause) ? failCause(cause) : slots.offer({ _tag: "fail", cause }),
        );

        const idle = (): Eff<Step<A>, any> =>
          (slots.take() as any).flatMap((slot: Slot): any => {
            if (slot._tag === "fail") return failCause(slot.cause);
            if (slot._tag === "end") return succeed(DONE);
            return settle(slot.value);
          });

        const settle = (latest: A): Eff<Step<A>, any> =>
          (timeoutOption(slots.take() as any, ms) as any).flatMap((slot: Slot | undefined): any => {
            if (slot === undefined)
              return succeed(emit(Chunk.single(latest), new Stream(suspend(() => idle()) as any)));
            if (slot._tag === "fail") return failCause(slot.cause);
            if (slot._tag === "end") return succeed(emit(Chunk.single(latest), Stream.empty()));
            return settle(slot.value);
          });

        return (fork(driver) as any).flatMap((fb: Fiber<any>) => {
          drivers.push(fb);
          return idle();
        });
      },
    );

    return new Stream<A, any>(suspend(() => setup) as any).onFinalize(
      interruptAllEff(drivers),
    ) as any;
  }

  /**
   * Decouple producer from consumer — a driver fiber prefetches up to
   * `capacity` elements ahead into a bounded queue while the consumer is
   * busy. Same driver/sentinel machinery as merge; the consumer drains
   * whatever is buffered per pull, so chunking downstream reflects
   * consumption timing.
   */
  buffer(capacity: number): Stream<A, S> {
    const self = this;
    const cap = Math.max(1, Math.floor(capacity));
    type Slot = { _tag: "item"; value: A } | { _tag: "end" } | { _tag: "fail"; cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<A>, any> = (QueueNS.bounded<Slot>(cap) as any).flatMap(
      (slots: Queue<Slot>) => {
        const offerChunk = (items: A[], i: number, next: Stream<A, any>): Eff<void, any> =>
          i >= items.length
            ? drain(next)
            : (slots.offer({ _tag: "item", value: items[i]! }) as any).flatMap(() =>
                offerChunk(items, i + 1, next),
              );

        const drain = (s: Stream<A, any>): Eff<void, any> =>
          (s.step as any).flatMap((step: Step<A>) =>
            step._tag === "Done"
              ? slots.offer({ _tag: "end" })
              : offerChunk(Array.from(step.chunk), 0, step.next),
          );

        const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause) ? failCause(cause) : slots.offer({ _tag: "fail", cause }),
        );

        // slot delivered after the values of a batch (driver stops offering
        // after a sentinel, so a sentinel is always last in a drained batch)
        let terminal: Slot | null = null;

        const finish = (): Eff<Step<A>, any> =>
          terminal!._tag === "fail" ? failCause((terminal as any).cause) : succeed(DONE);

        const pull = (): Eff<Step<A>, any> => {
          if (terminal !== null) return suspend(() => finish()) as any;
          return (slots.take() as any).flatMap((first: Slot): any => {
            if (first._tag !== "item") {
              terminal = first;
              return finish();
            }
            return (slots.takeAll() as any).flatMap((rest: Slot[]) => {
              const values: A[] = [first.value];
              for (const slot of rest) {
                if (slot._tag === "item") values.push(slot.value);
                else terminal = slot;
              }
              return succeed(
                emit(Chunk.fromArray(values), new Stream(suspend(() => pull()) as any)),
              );
            });
          });
        };

        return (fork(driver) as any).flatMap((fb: Fiber<any>) => {
          drivers.push(fb);
          return pull();
        });
      },
    );

    return new Stream<A, any>(suspend(() => setup) as any).onFinalize(
      interruptAllEff(drivers),
    ) as any;
  }

  throttle(ms: number): Stream<A, S> {
    let lastEmit = 0;
    return this.evalMap(
      (a) =>
        (clockNow as any).flatMap((now: number) => {
          const wait = Math.max(0, ms - (now - lastEmit));
          lastEmit = now + wait;
          return wait > 0 ? sleep(wait).map(() => a) : succeed(a);
        }) as any,
    );
  }

  // ── Pipe ─────────────────────────────────────────────────────────

  through<B, S2>(pipe: Pipe<A, B, S2>): Stream<B, S | S2> {
    return pipe(this) as any;
  }

  runSink<B, S2>(sink: { run(input: Stream<A, any>): Eff<B, S2> }): Eff<B, S | S2> {
    return sink.run(this) as any;
  }

  // ── Error handling ───────────────────────────────────────────────

  catch<B, S2>(handler: (error: any) => Stream<B, S2>): Stream<A | B, S2> {
    return new Stream(
      (this.step as any)
        .map((s: Step<A>) => {
          if (s._tag === "Done") return DONE;
          return emit(s.chunk, s.next.catch(handler));
        })
        .catch((e: any) => handler(e).step),
      this._finalizer,
    );
  }

  onFinalize<S2>(finalizer: Eff<void, S2>): Stream<A, S | S2> {
    return this._withFinalizer(finalizer);
  }

  /**
   * Retry each pull according to the given policy. If a step fails, retry it
   * up to the policy's limit. Once a chunk emits, retry resets — failures in
   * the next pull are retried independently.
   *
   * For "restart the entire stream from scratch on failure" semantics, build
   * the stream inside `Stream.suspend(() => makeStream())` and apply retry
   * around that — the suspend re-creates the source on each retry.
   */
  retry(policy: RetryPolicy | RetryConfig): Stream<A, S> {
    const wrap = (s: Stream<A, S>): Stream<A, S> =>
      new Stream(
        (effRetry(s.step as any, policy as any) as any).map((step: Step<A>) => {
          if (step._tag === "Done") return DONE;
          return emit(step.chunk, wrap(step.next));
        }),
        s._finalizer,
      );
    return wrap(this);
  }

  private mapContinuation(f: (next: Stream<A, any>) => Stream<A, any>): Stream<A, S> {
    return new Stream(
      (this.step as any).map((s: Step<A>) => {
        if (s._tag === "Done") return DONE;
        return emit(s.chunk, f(s.next));
      }),
      this._finalizer,
    );
  }

  // ── Terminal operators ────────────────────────────────────────────
  // Each materializes the stream into an `Eff`. Execute the result with
  // `.run()` / `.runSync()` — e.g. `stream.map(f).toArray().run()`.

  fold<B>(zero: B, f: (acc: B, a: A) => B): Eff<B, S> {
    function go(acc: B, stream: Stream<A, any>): Eff<B, any> {
      return (stream.step as any).flatMap((s: Step<A>) => {
        if (s._tag === "Done") return succeed(acc);
        const next = s.chunk.reduce(acc, f);
        return go(next, s.next);
      });
    }
    return this._finalize(go(zero, this) as any) as any;
  }

  toArray(): Eff<A[], S> {
    return this.fold<A[]>([], (acc, a) => {
      acc.push(a);
      return acc;
    });
  }

  drain(): Eff<void, S> {
    function go(stream: Stream<any, any>): Eff<void, any> {
      return (stream.step as any).flatMap((s: Step<any>) => {
        if (s._tag === "Done") return succeed(undefined);
        return go(s.next);
      });
    }
    return this._finalize(go(this) as any) as any;
  }

  forEach<S2>(f: (a: A) => Eff<void, S2>): Eff<void, S | S2> {
    function go(stream: Stream<A, any>): Eff<void, any> {
      return (stream.step as any).flatMap((s: Step<A>) => {
        if (s._tag === "Done") return succeed(undefined);
        return runChunkForEach(s.chunk, f).flatMap(() => go(s.next));
      });
    }
    return this._finalize(go(this) as any) as any;
  }

  head(): Eff<A | undefined, S> {
    return this._finalize(
      (this.step as any).map((s: Step<A>) => {
        if (s._tag === "Done") return undefined;
        return s.chunk.head();
      }) as any,
    ) as any;
  }

  last(): Eff<A | undefined, S> {
    function go(lastSeen: A | undefined, stream: Stream<A, any>): Eff<A | undefined, any> {
      return (stream.step as any).flatMap((s: Step<A>) => {
        if (s._tag === "Done") return succeed(lastSeen);
        return go(s.chunk.last() ?? lastSeen, s.next);
      });
    }
    return this._finalize(go(undefined, this) as any) as any;
  }

  count(): Eff<number, S> {
    return this.fold(0, (n, _) => n + 1);
  }
}

// ── Pipe type ──────────────────────────────────────────────────────

export type Pipe<I, O, S = never> = (input: Stream<I, any>) => Stream<O, S>;

// ── Helpers ────────────────────────────────────────────────────────

const SENTINEL = Symbol("sentinel");
const FILTER_SENTINEL = Symbol("filter-sentinel");

function evalMapChunk<A, B, S>(chunk: Chunk<A>, f: (a: A) => Eff<B, S>): Eff<Chunk<B>, S> {
  if (chunk.isEmpty) return succeed(Chunk.empty()) as any;
  const items = Array.from(chunk);
  return items
    .reduce<Eff<B[], S>>(
      (acc, item) =>
        (acc as any).flatMap((arr: B[]) =>
          (f(item) as any).map((b: B) => {
            arr.push(b);
            return arr;
          }),
        ),
      succeed([]) as any,
    )
    .map((arr) => Chunk.fromArray(arr)) as any;
}

function runChunkForEach<A, S>(chunk: Chunk<A>, f: (a: A) => Eff<void, S>): Eff<void, S> {
  const items = Array.from(chunk);
  return items.reduce<Eff<void, S>>(
    (acc, item) => (acc as any).flatMap(() => f(item)),
    succeed(undefined) as any,
  );
}
