// Stream<A, S> — lazy, pull-based, chunked, resource-safe
//
// Internally: each step is an Eff that produces either:
//   Emit(chunk, nextStream) — a chunk of values + continuation
//   Done                    — stream exhausted
//
// Consumer drives: upstream only runs when downstream pulls.
// Backpressure is structural — no buffering, no highWaterMark.

import { type Eff, type Throws, type ErrorsOf, type ExcludeTags, Suspend, Op } from "../eff";
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
  race,
  timeoutOption,
  fromPromise,
  ensuring,
  awaitFiber,
  yieldNow,
  retry as effRetry,
  type RetryConfig,
} from "../constructors";
import { TaggedError } from "../tagged-error";
import type { RetryPolicy } from "../retry-policy";
import { type Queue, Queue as QueueNS, QueueClosed } from "../queue";
import { type Deferred, Deferred as DeferredNS } from "../deferred";
import { Semaphore } from "../semaphore";
import { Cause } from "../cause";
import { clockNow } from "../clock";
import { Exit } from "../exit";
import type { Fiber } from "../fiber";
import type { StateBackend } from "../connect/state-backend";

export interface StatefulMapOptions<A, K, V, B, S> {
  readonly stateBackend: StateBackend<K, V>;
  readonly keyBy: (value: A) => K;
  readonly process: (value: A, state: StateBackend<K, V>, key: K) => Eff<B, S>;
}

type BroadcastBranch<A, S> = (stream: Stream<A, S>) => Stream<any, any>;
type StreamValue<T> = T extends Stream<infer A, any> ? A : never;
type StreamEffects<T> = T extends Stream<any, infer S> ? S : never;
type Either<E, A> =
  | { readonly _tag: "Left"; readonly left: E }
  | { readonly _tag: "Right"; readonly right: A };

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
      (acc, f) =>
        (acc as any)
          .flatMap(() => interrupt(f))
          .flatMap(() => awaitFiber(f))
          .map(() => undefined),
      succeed(undefined) as any,
    );
  }) as any;
}

function combineFinalizers(
  first: Eff<void, unknown> | null,
  second: Eff<void, unknown> | null,
): Eff<void, unknown> | null {
  if (first === null) return second;
  if (second === null) return first;
  return new Suspend(Op.Ensuring, first, second) as any;
}
import { Chunk } from "./chunk";
import { type FusibleOp, compileFused, hasFilterOps, SKIP } from "./fusion";

// ── Errors ─────────────────────────────────────────────────────────

/** Typed failure produced by {@link Stream.timeout} when the gap between
 *  element-producing pulls exceeds the limit. */
export class StreamTimeoutError extends TaggedError("StreamTimeoutError")<{
  readonly ms: number;
}>() {}

// ── Step type ──────────────────────────────────────────────────────

export type Step<A> =
  // `next: Stream<A, unknown>`: the continuation's effect union is erased —
  // the driving terminal op re-attaches S at the Stream level.
  | { readonly _tag: "Emit"; readonly chunk: Chunk<A>; readonly next: Stream<A, unknown> }
  | { readonly _tag: "Done" };

const DONE: Step<any> = { _tag: "Done" };

function emit<A>(chunk: Chunk<A>, next: Stream<A, unknown>): Step<A> {
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
  _finalizer: Eff<void, unknown> | null;

  constructor(step: Eff<Step<A>, S>, finalizer: Eff<void, unknown> | null = null) {
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
    const combined = combineFinalizers(this._finalizer, finalizer);
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

  static fromAsyncIterable<A, E>(
    iterable: AsyncIterable<A>,
    onError: (error: unknown) => E,
  ): Stream<A, Throws<E>> {
    let iterator: AsyncIterator<A> | null = null;
    let completed = false;

    const next = (): Stream<A, Throws<E>> =>
      new Stream(
        suspend(() => {
          return fromPromise(
            () =>
              Promise.resolve().then(() => {
                iterator ??= iterable[Symbol.asyncIterator]();
                return iterator.next();
              }),
            onError,
          ).map((result) => {
            if (result.done) {
              completed = true;
              return DONE;
            }
            return emit(Chunk.single(result.value), next());
          });
        }),
      );

    return next().onFinalize(
      suspend(() => {
        if (completed || iterator?.return === undefined) return succeed(undefined);
        completed = true;
        return fromPromise(() => Promise.resolve().then(() => iterator!.return!()), onError).map(
          () => undefined,
        );
      }),
    );
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

  /**
   * Run the stream produced by `factory` end-to-end `n` times, concatenating
   * the runs.
   *
   * This is a static taking a factory rather than an instance `repeatN`
   * because perfect streams are single-shot pull cursors — once a step chain
   * has been consumed it cannot be rewound, so an instance method would have
   * nothing left to replay. The factory re-creates a fresh stream (lazily,
   * via {@link Stream.suspend}) for each repetition.
   */
  static repeatWith<A, S>(factory: () => Stream<A, S>, n: number): Stream<A, S> {
    const count = Math.floor(n);
    if (count <= 0) return Stream.empty() as any;
    const go = (remaining: number): Stream<A, S> =>
      remaining <= 1
        ? Stream.suspend(factory)
        : (Stream.suspend(factory).concat(Stream.suspend(() => go(remaining - 1))) as any);
    return go(count);
  }

  /**
   * Retry a factory-built stream from its source acquisition when it fails.
   * Values emitted before a failure remain emitted, so restarting a source can
   * produce duplicates unless the source resumes from a durable offset.
   *
   * The retry policy resets after the restarted stream emits a chunk. Every
   * failed attempt is finalized before the next source is acquired, and an
   * active attempt is finalized when downstream stops early.
   */
  static retryFrom<A, S>(
    factory: () => Stream<A, S>,
    policy: RetryPolicy | RetryConfig,
  ): Stream<A, S> {
    let activeFinalizer: Eff<void, unknown> | null = null;

    const releaseActive = (): Eff<void, unknown> =>
      suspend(() => {
        const finalizer = activeFinalizer;
        activeFinalizer = null;
        return finalizer ?? succeed(undefined);
      });

    const acquire = (): Eff<Step<A>, S> =>
      suspend(() => {
        const source = factory();
        activeFinalizer = source._finalizer;
        return (source.step as any)
          .flatMap((step: Step<A>) =>
            step._tag === "Done" ? (releaseActive() as any).map(() => DONE) : succeed(step),
          )
          .catchAllCause((cause: Cause) =>
            (releaseActive() as any).flatMap(() => failCause(cause)),
          );
      }) as any;

    const wrapStep = (stepEffect: Eff<Step<A>, S>): Eff<Step<A>, S> =>
      (stepEffect as any).map((step: Step<A>) =>
        step._tag === "Done" ? DONE : emit(step.chunk, follow(step.next as Stream<A, S>)),
      );

    const retryAfter = (cause: Cause): Eff<Step<A>, S> => {
      let first = true;
      const attempt: Eff<Step<A>, S> = suspend(() => {
        if (first) {
          first = false;
          return failCause(cause) as any;
        }
        return acquire();
      }) as any;
      return wrapStep(effRetry(attempt, policy as any) as Eff<Step<A>, S>);
    };

    function follow(current: Stream<A, S>): Stream<A, S> {
      return new Stream(
        (current.step as any)
          .flatMap((step: Step<A>) => {
            if (step._tag === "Done") {
              return (releaseActive() as any).map(() => DONE);
            }
            return succeed(emit(step.chunk, follow(step.next as Stream<A, S>)));
          })
          .catchAllCause((cause: Cause) =>
            (releaseActive() as any).flatMap(() => retryAfter(cause)),
          ),
      );
    }

    const initial = suspend(() => wrapStep(effRetry(acquire(), policy as any) as Eff<Step<A>, S>));
    return new Stream(initial as Eff<Step<A>, S>, suspend(releaseActive) as any);
  }

  static tick(intervalMs: number): Stream<void, never> {
    function go(): Stream<void, never> {
      return new Stream(
        (sleep(intervalMs) as any).map(() => emit(Chunk.single(undefined as void), go())),
      );
    }
    return go();
  }

  static fromQueue<A, S>(queue: { take(): Eff<A, S> }): Stream<A, ExcludeTags<S, "QueueClosed">> {
    function go(): Stream<A, ExcludeTags<S, "QueueClosed">> {
      return new Stream(
        (queue.take() as any)
          .map((a: A) => emit(Chunk.single(a), go()))
          .catchTag("QueueClosed", () => succeed(DONE)),
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
    // Listener args stay `any[]` for structural compat with Node's
    // EventEmitter and typed-listener emitters; returns are ignored.
    emitter: {
      on(event: string, listener: (...args: any[]) => void): unknown;
      off?(event: string, listener: (...args: any[]) => void): unknown;
      removeListener?(event: string, listener: (...args: any[]) => void): unknown;
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
    register: (
      emit: (value: A) => void,
      close: () => void,
      failStream: (error: unknown) => void,
    ) => Eff<(() => void) | void, S>,
    bufferSize = 1024,
  ): Stream<A, S> {
    // the finalizer lives on the OUTER stream (via onFinalize below) so
    // terminals run it on normal completion too — inner `next()` streams'
    // step effects would silently drop it
    let activeCleanup: (() => void) | null = null;

    const source = new Stream(
      (succeed(null) as any).flatMap(() => {
        const buffer: A[] = [];
        let closed = false;
        let failure: { readonly error: unknown } | null = null;
        let waiter: ((effect: Eff<Step<A>, unknown>) => void) | null = null;
        let cleanup: (() => void) | void;
        let cleaned = false;

        const cleanupOnce = (): void => {
          if (cleaned) return;
          cleaned = true;
          if (cleanup) cleanup();
        };
        activeCleanup = cleanupOnce;

        const pushEmit = (value: A) => {
          if (closed) return;
          if (waiter !== null) {
            const w = waiter;
            waiter = null;
            w(succeed(emit(Chunk.single(value), next())));
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
            w(succeed(DONE));
          }
        };

        const pushFail = (error: unknown) => {
          if (closed) return;
          closed = true;
          failure = { error };
          if (waiter !== null && buffer.length === 0) {
            const w = waiter;
            waiter = null;
            cleanupOnce();
            w(fail(error));
          }
        };

        function next(): Stream<A, S> {
          return new Stream(
            new Suspend(
              Op.Async,
              (resume: (eff: any) => void) => {
                if (buffer.length > 0) {
                  const chunkArr: A[] = buffer.splice(0, buffer.length);
                  resume(succeed(emit(Chunk.fromArray(chunkArr), next())));
                  return;
                }
                if (failure !== null) {
                  cleanupOnce();
                  resume(fail(failure.error));
                  return;
                }
                if (closed) {
                  cleanupOnce();
                  resume(succeed(DONE));
                  return;
                }
                waiter = (effect) => resume(effect);
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

        return (register(pushEmit, pushClose, pushFail) as any)
          .map((c: (() => void) | void) => {
            cleanup = c ?? undefined;
            return next().step;
          })
          .flatMap((s: any) => s);
      }),
    );

    return source.onFinalize(
      suspend(() =>
        sync(() => {
          activeCleanup?.();
          activeCleanup = null;
        }),
      ) as any,
    ) as any;
  }

  /**
   * Chunk-preserving variant of {@link Stream.async}. Each emitted chunk
   * remains one stream step, allowing callback-based batch sources to retain
   * their native batch boundaries.
   */
  static asyncChunks<A, S>(
    register: (
      emit: (chunk: Chunk<A>) => void,
      close: () => void,
      failStream: (error: unknown) => void,
    ) => Eff<(() => void) | void, S>,
    bufferSize = 1024,
  ): Stream<A, S> {
    let activeCleanup: (() => void) | null = null;

    const source = new Stream(
      (succeed(null) as any).flatMap(() => {
        const buffer: Chunk<A>[] = [];
        let closed = false;
        let failure: { readonly error: unknown } | null = null;
        let waiter: ((effect: Eff<Step<A>, unknown>) => void) | null = null;
        let cleanup: (() => void) | void;
        let cleaned = false;

        const cleanupOnce = (): void => {
          if (cleaned) return;
          cleaned = true;
          if (cleanup) cleanup();
        };
        activeCleanup = cleanupOnce;

        const pushEmit = (chunk: Chunk<A>) => {
          if (closed || chunk.isEmpty) return;
          if (waiter !== null) {
            const w = waiter;
            waiter = null;
            w(succeed(emit(chunk, next())));
            return;
          }
          if (buffer.length < bufferSize) buffer.push(chunk);
        };
        const pushClose = () => {
          if (closed) return;
          closed = true;
          if (waiter !== null && buffer.length === 0) {
            const w = waiter;
            waiter = null;
            cleanupOnce();
            w(succeed(DONE));
          }
        };

        const pushFail = (error: unknown) => {
          if (closed) return;
          closed = true;
          failure = { error };
          if (waiter !== null && buffer.length === 0) {
            const w = waiter;
            waiter = null;
            cleanupOnce();
            w(fail(error));
          }
        };

        function next(): Stream<A, S> {
          return new Stream(
            new Suspend(
              Op.Async,
              (resume: (eff: any) => void) => {
                const chunk = buffer.shift();
                if (chunk) {
                  resume(succeed(emit(chunk, next())));
                  return;
                }
                if (failure !== null) {
                  cleanupOnce();
                  resume(fail(failure.error));
                  return;
                }
                if (closed) {
                  cleanupOnce();
                  resume(succeed(DONE));
                  return;
                }
                waiter = (effect) => resume(effect);
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

        return (register(pushEmit, pushClose, pushFail) as any)
          .map((c: (() => void) | void) => {
            cleanup = c ?? undefined;
            return next().step;
          })
          .flatMap((s: any) => s);
      }),
    );

    return source.onFinalize(
      suspend(() =>
        sync(() => {
          activeCleanup?.();
          activeCleanup = null;
        }),
      ) as any,
    ) as any;
  }

  static bracket<A, S>(acquire: Eff<A, S>, release: (a: A) => Eff<void, never>): Stream<A, S> {
    let activeFinalizer: Eff<void, never> | null = null;
    return new Stream(
      (acquire as any).map((resource: A) => {
        activeFinalizer = release(resource);
        return emit(Chunk.single(resource), Stream.empty());
      }),
      suspend(() => {
        const finalizer = activeFinalizer;
        activeFinalizer = null;
        return finalizer ?? succeed(undefined);
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
    let activeInnerFinalizer: Eff<void, unknown> | null = null;

    const releaseInner = (): Eff<void, unknown> => {
      const finalizer = activeInnerFinalizer;
      activeInnerFinalizer = null;
      return finalizer ?? succeed(undefined);
    };

    const drainInner = (
      inner: Stream<B, S2>,
      outer: Stream<A, OuterS>,
      pending: Chunk<A>,
      idx: number,
    ): Stream<B, OuterS | S2> =>
      new Stream(
        (inner.step as any).flatMap((s: Step<B>) => {
          if (s._tag === "Emit") {
            return succeed(emit(s.chunk, drainInner(s.next as any, outer, pending, idx + 0)));
          }
          return releaseInner().flatMap(() => nextElement(outer, pending, idx + 1).step);
        }),
      );

    const nextElement = (
      outer: Stream<A, OuterS>,
      pending: Chunk<A>,
      idx: number,
    ): Stream<B, OuterS | S2> => {
      if (idx < pending.length) {
        const inner = f(pending.get(idx));
        activeInnerFinalizer = inner._finalizer;
        return drainInner(inner, outer, pending, idx);
      }
      return new Stream(
        (outer.step as any).flatMap((s: Step<A>) => {
          if (s._tag === "Done") return succeed(DONE);
          return nextElement(s.next as any, s.chunk, 0).step;
        }),
      );
    };

    const source = nextElement(self, Chunk.empty(), 0);
    return new Stream(
      source.step,
      combineFinalizers(suspend(releaseInner), self._finalizer),
    ) as Stream<B, S | S2>;
  }

  switchMap<B, S2>(f: (value: A) => Stream<B, S2>): Stream<B, S | S2> {
    return this.concurrentFlatMap("switch", f);
  }

  exhaustMap<B, S2>(f: (value: A) => Stream<B, S2>): Stream<B, S | S2> {
    return this.concurrentFlatMap("exhaust", f);
  }

  private concurrentFlatMap<B, S2>(
    mode: "switch" | "exhaust",
    f: (value: A) => Stream<B, S2>,
  ): Stream<B, S | S2> {
    const self = this;
    type Event =
      | { readonly _tag: "outerItem"; readonly value: A; readonly ready: Deferred<void> }
      | { readonly _tag: "outerEnd" }
      | { readonly _tag: "outerFail"; readonly cause: Cause }
      | { readonly _tag: "innerChunk"; readonly generation: number; readonly chunk: Chunk<B> }
      | { readonly _tag: "innerEnd"; readonly generation: number }
      | { readonly _tag: "innerFail"; readonly generation: number; readonly cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<B>, any> = (QueueNS.bounded<Event>(16) as any).flatMap(
      (events: Queue<Event>) => {
        let current: Fiber<any> | null = null;
        let generation = 0;
        let active = false;
        let outerDone = false;

        const offerOuterChunk = (
          items: A[],
          index: number,
          nextOuter: Stream<A, any>,
        ): Eff<void, any> =>
          index >= items.length
            ? drainOuter(nextOuter)
            : (DeferredNS.make<void>() as any).flatMap((ready: Deferred<void>) =>
                (events.offer({ _tag: "outerItem", value: items[index]!, ready }) as any).flatMap(
                  () =>
                    (ready.await as any).flatMap(() =>
                      offerOuterChunk(items, index + 1, nextOuter),
                    ),
                ),
              );

        const drainOuter = (outer: Stream<A, any>): Eff<void, any> =>
          (outer.step as any).flatMap((step: Step<A>) =>
            step._tag === "Done"
              ? events.offer({ _tag: "outerEnd" })
              : offerOuterChunk(Array.from(step.chunk), 0, step.next),
          );

        const drainInner = (inner: Stream<B, any>, innerGeneration: number): Eff<void, any> =>
          (inner.step as any).flatMap((step: Step<B>) =>
            step._tag === "Done"
              ? succeed(undefined)
              : (
                  events.offer({
                    _tag: "innerChunk",
                    generation: innerGeneration,
                    chunk: step.chunk,
                  }) as any
                ).flatMap(() => drainInner(step.next, innerGeneration)),
          );

        const runInner = (inner: Stream<B, any>, innerGeneration: number): Eff<void, any> =>
          (
            ensuring(
              drainInner(inner, innerGeneration),
              inner._finalizer ?? succeed(undefined),
            ) as any
          )
            .flatMap(() => events.offer({ _tag: "innerEnd", generation: innerGeneration }))
            .catchAllCause((cause: Cause) =>
              Cause.isInterruptedOnly(cause)
                ? failCause(cause)
                : events.offer({ _tag: "innerFail", generation: innerGeneration, cause }),
            );

        const launch = (value: A): Eff<void, any> =>
          suspend(() => {
            const previous = current;
            const innerGeneration = ++generation;
            active = true;
            current = null;

            const start = suspend(() => fork(runInner(f(value), innerGeneration)))
              .map((fiber: Fiber<any>) => {
                current = fiber;
                drivers.push(fiber);
              })
              .flatMap(() => yieldNow);

            if (mode === "switch" && previous !== null) {
              return interrupt(previous)
                .flatMap(() => awaitFiber(previous))
                .flatMap(() => start);
            }
            return start;
          });

        const pull = (): Eff<Step<B>, any> =>
          (events.take() as any).flatMap((event: Event): any => {
            switch (event._tag) {
              case "outerItem":
                if (mode === "exhaust" && active) {
                  return event.ready.succeed(undefined).flatMap(() => pull());
                }
                return launch(event.value)
                  .flatMap(() => event.ready.succeed(undefined))
                  .flatMap(() => pull());
              case "outerEnd":
                outerDone = true;
                return active ? pull() : succeed(DONE);
              case "outerFail":
                return failCause(event.cause);
              case "innerChunk":
                if (!active || event.generation !== generation) return pull();
                return succeed(emit(event.chunk, new Stream(suspend(() => pull()) as any)));
              case "innerEnd":
                if (event.generation !== generation) return pull();
                active = false;
                current = null;
                return outerDone ? succeed(DONE) : pull();
              case "innerFail":
                return event.generation === generation ? failCause(event.cause) : pull();
            }
          });

        const outerDriver = (drainOuter(self) as any).catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause)
            ? failCause(cause)
            : events.offer({ _tag: "outerFail", cause }),
        );

        return (fork(outerDriver) as any).flatMap((fiber: Fiber<any>) => {
          drivers.push(fiber);
          return pull();
        });
      },
    );

    return new Stream<B, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
    ) as any;
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

  /**
   * Emit values until `signal` emits its first value. An empty signal leaves
   * the source unchanged; a failing signal fails the result. Source and signal
   * finalizers are awaited when either side wins or downstream stops.
   */
  takeUntil<S2>(signal: Stream<unknown, S2>): Stream<A, S | S2> {
    type SignalEvent =
      | { readonly _tag: "stop" }
      | { readonly _tag: "empty" }
      | { readonly _tag: "failure"; readonly cause: Cause };
    type RaceEvent =
      | { readonly _tag: "source"; readonly step: Step<A> }
      | { readonly _tag: "signal"; readonly event: SignalEvent };

    const self = this;
    const drivers: Fiber<any>[] = [];
    let signalFinalizer = signal._finalizer;

    const releaseSignal = (): Eff<void, unknown> =>
      suspend(() => {
        const finalizer = signalFinalizer;
        signalFinalizer = null;
        return finalizer ?? succeed(undefined);
      });

    const setup: Eff<Step<A>, any> = (DeferredNS.make<SignalEvent>() as any).flatMap(
      (control: Deferred<SignalEvent>) => {
        let signalFinished = false;

        const watchSignal = (
          ensuring(
            (signal.step as any).flatMap((step: Step<unknown>) =>
              (releaseSignal() as any).map(
                (): SignalEvent => (step._tag === "Done" ? { _tag: "empty" } : { _tag: "stop" }),
              ),
            ),
            suspend(releaseSignal),
          ) as any
        )
          .catchAllCause((cause: Cause) => succeed<SignalEvent>({ _tag: "failure", cause }))
          .flatMap((event: SignalEvent) => control.succeed(event).map(() => undefined));

        const pullSource = (
          source: Stream<A, any>,
        ): Eff<Extract<RaceEvent, { readonly _tag: "source" }>, any> =>
          (source.step as any).map((step: Step<A>) => ({ _tag: "source" as const, step }));

        const pull = (source: Stream<A, any>): Eff<Step<A>, any> => {
          if (signalFinished) {
            return pullSource(source).map((event) => {
              const step = event.step;
              return step._tag === "Done"
                ? DONE
                : emit(step.chunk, new Stream(suspend(() => pull(step.next))));
            });
          }

          return (
            race([
              pullSource(source),
              (control.await as any).map(
                (event: SignalEvent): RaceEvent => ({ _tag: "signal", event }),
              ),
            ]) as any
          ).flatMap((winner: RaceEvent): Eff<Step<A>, any> => {
            if (winner._tag === "source") {
              const step = winner.step;
              return succeed(
                step._tag === "Done"
                  ? DONE
                  : emit(step.chunk, new Stream(suspend(() => pull(step.next)))),
              );
            }

            if (winner.event._tag === "stop") return succeed(DONE);
            if (winner.event._tag === "failure") return failCause(winner.event.cause);
            signalFinished = true;
            return pull(source);
          });
        };

        return (fork(watchSignal) as any).flatMap((fiber: Fiber<any>) => {
          drivers.push(fiber);
          return pull(self);
        });
      },
    );

    return new Stream(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
    ) as any;
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

  /**
   * Stateful map — thread an accumulator through the stream, emitting the
   * mapped value for each element. `f` returns `[nextState, output]`; the
   * state carries across chunk boundaries. Unlike `scan` the state itself is
   * never emitted.
   */
  mapAccumulate<St, B>(initial: St, f: (state: St, a: A) => readonly [St, B]): Stream<B, S> {
    function go(state: St, stream: Stream<A, any>): Stream<B, any> {
      return new Stream(
        (stream.step as any).map((s: Step<A>) => {
          if (s._tag === "Done") return DONE;
          const out: B[] = [];
          let current = state;
          for (const item of s.chunk) {
            const [next, b] = f(current, item);
            current = next;
            out.push(b);
          }
          return emit(Chunk.fromArray(out), go(current, s.next));
        }),
        stream._finalizer,
      );
    }
    return go(initial, this);
  }

  statefulMap<K, V, B, S2>(options: StatefulMapOptions<A, K, V, B, S2>): Stream<B, S | S2>;
  statefulMap<St, B>(initial: St, f: (state: St, value: A) => readonly [St, B]): Stream<B, S>;
  statefulMap<K, V, B, S2, St>(
    optionsOrInitial: StatefulMapOptions<A, K, V, B, S2> | St,
    accumulate?: (state: St, value: A) => readonly [St, B],
  ): Stream<B, S | S2> {
    if (accumulate) return this.mapAccumulate(optionsOrInitial as St, accumulate);
    const options = optionsOrInitial as StatefulMapOptions<A, K, V, B, S2>;
    return this.evalMap((value) =>
      options.process(value, options.stateBackend, options.keyBy(value)),
    );
  }

  tap(f: (a: A) => void): Stream<A, S> {
    return this._withOp({ _tag: "tap", fn: f as any });
  }

  tapEffect<S2>(f: (a: A) => Eff<unknown, S2>): Stream<A, S | S2> {
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

  /** Pair each element with its predecessor — the first element pairs with
   *  `undefined`: `[undefined, first], [first, second], …`. */
  zipWithPrevious(): Stream<[A | undefined, A], S> {
    return this.mapAccumulate<A | undefined, [A | undefined, A]>(undefined, (prev, curr) => [
      curr,
      [prev, curr] as [A | undefined, A],
    ]);
  }

  /**
   * Global deduplication — drop any element whose key was already seen (first
   * occurrence wins). Keys are compared by Set identity (`SameValueZero`).
   *
   * Unlike {@link changes}, which only suppresses *consecutive* duplicates,
   * this tracks every key seen so far — memory grows with the number of
   * distinct keys, so prefer `changes` for sorted/bursty inputs.
   */
  dedupe(keyFn?: (a: A) => unknown): Stream<A, S> {
    const key = keyFn ?? ((a: A) => a as unknown);
    const seen = new Set<unknown>();
    return this.filter((a) => {
      const k = key(a);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  /** Alias of {@link dedupe} with a required key function — keeps the first
   *  occurrence of each key. */
  distinctBy(keyFn: (a: A) => unknown): Stream<A, S> {
    return this.dedupe(keyFn);
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

  /**
   * Sliding windows of `size` elements, advancing by `step` (default 1)
   * between windows. Only full windows are emitted — a stream shorter than
   * `size` emits nothing — and `step > size` skips elements between windows.
   */
  sliding(size: number, step = 1): Stream<Chunk<A>, S> {
    const sz = Math.max(1, Math.floor(size));
    const st = Math.max(1, Math.floor(step));
    function go(buffer: A[], skip: number, stream: Stream<A, any>): Stream<Chunk<A>, any> {
      return new Stream(
        (stream.step as any)
          .map((s: Step<A>) => {
            if (s._tag === "Done") return DONE;
            const windows: Chunk<A>[] = [];
            let buf = buffer.slice();
            let toSkip = skip;
            for (const item of s.chunk) {
              if (toSkip > 0) {
                toSkip--;
                continue;
              }
              buf.push(item);
              if (buf.length === sz) {
                windows.push(Chunk.fromArray(buf));
                if (st >= sz) {
                  toSkip = st - sz;
                  buf = [];
                } else {
                  buf = buf.slice(st);
                }
              }
            }
            const next = go(buf, toSkip, s.next);
            if (windows.length === 0) return next.step;
            return emit(Chunk.fromArray(windows), next);
          })
          .flatMap((r: any) => (r instanceof Suspend ? r : succeed(r))),
        stream._finalizer,
      );
    }
    return go([], 0, this);
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

    const source = go(this, that, Chunk.empty(), Chunk.empty());
    return new Stream(source.step, combineFinalizers(this._finalizer, that._finalizer)) as Stream<
      C,
      S | S2
    >;
  }

  combineLatest<B, S2>(that: Stream<B, S2>): Stream<[A, B], S | S2> {
    const self = this;
    type Event =
      | { readonly _tag: "left"; readonly chunk: Chunk<A> }
      | { readonly _tag: "right"; readonly chunk: Chunk<B> }
      | { readonly _tag: "leftEnd" }
      | { readonly _tag: "rightEnd" }
      | { readonly _tag: "fail"; readonly cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<[A, B]>, any> = (QueueNS.bounded<Event>(2) as any).flatMap(
      (events: Queue<Event>) => {
        let hasLeft = false;
        let hasRight = false;
        let latestLeft!: A;
        let latestRight!: B;
        let leftDone = false;
        let rightDone = false;

        const drain = <T>(
          stream: Stream<T, any>,
          chunkEvent: (chunk: Chunk<T>) => Event,
          endEvent: Event,
        ): Eff<void, any> =>
          (stream.step as any).flatMap((step: Step<T>) =>
            step._tag === "Done"
              ? events.offer(endEvent)
              : (events.offer(chunkEvent(step.chunk)) as any).flatMap(() =>
                  drain(step.next, chunkEvent, endEvent),
                ),
          );

        const guardedDrain = <T>(
          stream: Stream<T, any>,
          chunkEvent: (chunk: Chunk<T>) => Event,
          endEvent: Event,
        ) =>
          (drain(stream, chunkEvent, endEvent) as any).catchAllCause((cause: Cause) =>
            Cause.isInterruptedOnly(cause)
              ? failCause(cause)
              : events.offer({ _tag: "fail", cause }),
          );

        const pull = (): Eff<Step<[A, B]>, any> =>
          (events.take() as any).flatMap((event: Event): any => {
            switch (event._tag) {
              case "left": {
                const output: [A, B][] = [];
                for (const value of event.chunk) {
                  latestLeft = value;
                  hasLeft = true;
                  if (hasRight) output.push([value, latestRight]);
                }
                return output.length === 0
                  ? pull()
                  : succeed(
                      emit(Chunk.fromArray(output), new Stream(suspend(() => pull()) as any)),
                    );
              }
              case "right": {
                const output: [A, B][] = [];
                for (const value of event.chunk) {
                  latestRight = value;
                  hasRight = true;
                  if (hasLeft) output.push([latestLeft, value]);
                }
                return output.length === 0
                  ? pull()
                  : succeed(
                      emit(Chunk.fromArray(output), new Stream(suspend(() => pull()) as any)),
                    );
              }
              case "leftEnd":
                leftDone = true;
                return !hasLeft || rightDone ? succeed(DONE) : pull();
              case "rightEnd":
                rightDone = true;
                return !hasRight || leftDone ? succeed(DONE) : pull();
              case "fail":
                return failCause(event.cause);
            }
          });

        return (
          fork(guardedDrain(self, (chunk) => ({ _tag: "left", chunk }), { _tag: "leftEnd" })) as any
        ).flatMap((left: Fiber<any>) => {
          drivers.push(left);
          return (
            fork(
              guardedDrain(that, (chunk) => ({ _tag: "right", chunk }), { _tag: "rightEnd" }),
            ) as any
          ).flatMap((right: Fiber<any>) => {
            drivers.push(right);
            return pull();
          });
        });
      },
    );

    return new Stream<[A, B], any>(
      suspend(() => setup) as any,
      combineFinalizers(
        interruptAllEff(drivers),
        combineFinalizers(self._finalizer, that._finalizer),
      ),
    ) as any;
  }

  withLatest<B, S2>(that: Stream<B, S2>): Stream<[A, B], S | S2> {
    const self = this;
    type Event =
      | { readonly _tag: "main"; readonly chunk: Chunk<A> }
      | { readonly _tag: "side"; readonly chunk: Chunk<B> }
      | { readonly _tag: "mainEnd" }
      | { readonly _tag: "sideEnd" }
      | { readonly _tag: "fail"; readonly cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<[A, B]>, any> = (QueueNS.bounded<Event>(2) as any).flatMap(
      (events: Queue<Event>) => {
        let hasLatest = false;
        let latest!: B;

        const drain = <T>(
          stream: Stream<T, any>,
          chunkEvent: (chunk: Chunk<T>) => Event,
          endEvent: Event,
        ): Eff<void, any> =>
          (stream.step as any).flatMap((step: Step<T>) =>
            step._tag === "Done"
              ? events.offer(endEvent)
              : (events.offer(chunkEvent(step.chunk)) as any).flatMap(() =>
                  drain(step.next, chunkEvent, endEvent),
                ),
          );

        const guardedDrain = <T>(
          stream: Stream<T, any>,
          chunkEvent: (chunk: Chunk<T>) => Event,
          endEvent: Event,
        ) =>
          (drain(stream, chunkEvent, endEvent) as any).catchAllCause((cause: Cause) =>
            Cause.isInterruptedOnly(cause)
              ? failCause(cause)
              : events.offer({ _tag: "fail", cause }),
          );

        const pull = (): Eff<Step<[A, B]>, any> =>
          (events.take() as any).flatMap((event: Event): any => {
            switch (event._tag) {
              case "side":
                for (const value of event.chunk) {
                  latest = value;
                  hasLatest = true;
                }
                return pull();
              case "main":
                if (!hasLatest) return pull();
                return succeed(
                  emit(
                    event.chunk.map((value) => [value, latest] as [A, B]),
                    new Stream(suspend(() => pull()) as any),
                  ),
                );
              case "mainEnd":
                return succeed(DONE);
              case "sideEnd":
                return pull();
              case "fail":
                return failCause(event.cause);
            }
          });

        return (
          fork(guardedDrain(that, (chunk) => ({ _tag: "side", chunk }), { _tag: "sideEnd" })) as any
        ).flatMap((side: Fiber<any>) => {
          drivers.push(side);
          return (
            fork(
              guardedDrain(self, (chunk) => ({ _tag: "main", chunk }), { _tag: "mainEnd" }),
            ) as any
          ).flatMap((main: Fiber<any>) => {
            drivers.push(main);
            return pull();
          });
        });
      },
    );

    return new Stream<[A, B], any>(
      suspend(() => setup) as any,
      combineFinalizers(
        interruptAllEff(drivers),
        combineFinalizers(self._finalizer, that._finalizer),
      ),
    ) as any;
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

    return new Stream<A, any>(
      suspend(() => setup) as any,
      combineFinalizers(
        interruptAllEff(drivers),
        combineFinalizers(self._finalizer, that._finalizer),
      ),
    ) as any;
  }

  /**
   * Pull the source once, fan every chunk out to every branch, and merge the
   * branch outputs. Each branch has a one-chunk bounded input queue, so the
   * slowest active branch backpressures the source.
   */
  broadcastThrough(): Stream<A, S>;
  broadcastThrough<
    const Branches extends readonly [BroadcastBranch<A, S>, ...BroadcastBranch<A, S>[]],
  >(
    ...branches: Branches
  ): Stream<
    StreamValue<ReturnType<Branches[number]>>,
    S | StreamEffects<ReturnType<Branches[number]>>
  >;
  broadcastThrough(...branches: readonly BroadcastBranch<A, S>[]): Stream<any, any> {
    if (branches.length === 0) return this;

    const self = this;
    type InputSlot =
      | { readonly _tag: "chunk"; readonly chunk: Chunk<A> }
      | { readonly _tag: "end" }
      | { readonly _tag: "fail"; readonly cause: Cause };
    type OutputSlot =
      | { readonly _tag: "item"; readonly value: unknown }
      | { readonly _tag: "end" }
      | { readonly _tag: "fail"; readonly cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<unknown>, any> = (
      branches.reduce<Eff<Queue<InputSlot>[], never>>(
        (acc) =>
          (acc as any).flatMap((queues: Queue<InputSlot>[]) =>
            QueueNS.bounded<InputSlot>(1).map((queue) => {
              queues.push(queue);
              return queues;
            }),
          ),
        succeed([]) as Eff<Queue<InputSlot>[], never>,
      ) as any
    ).flatMap((inputs: Queue<InputSlot>[]) =>
      (QueueNS.bounded<OutputSlot>(branches.length) as any).flatMap(
        (outputs: Queue<OutputSlot>) => {
          const active = branches.map(() => true);

          const inputStream = (queue: Queue<InputSlot>): Stream<A, S> => {
            const pull = (): Eff<Step<A>, any> =>
              (queue.take() as any)
                .flatMap((slot: InputSlot): any => {
                  if (slot._tag === "fail") return failCause(slot.cause);
                  if (slot._tag === "end") return succeed(DONE);
                  return succeed(
                    emit(slot.chunk, new Stream<A, any>(suspend(() => pull()) as any)),
                  );
                })
                .catch((error: unknown) =>
                  error instanceof QueueClosed ? succeed(DONE) : fail(error),
                );
            return new Stream(suspend(() => pull()) as any) as Stream<A, S>;
          };

          const closeInput = (index: number): Eff<void, never> =>
            sync(() => {
              active[index] = false;
            }).flatMap(() => inputs[index]!.close());

          const branchDriver = (index: number): Eff<void, any> => {
            const branch = branches[index]!(inputStream(inputs[index]!));
            const drain = branch.forEach((value) =>
              (outputs.offer({ _tag: "item", value }) as any).map(() => undefined),
            );
            return (exitOf(ensuring(drain, closeInput(index))) as any).flatMap(
              (exit: Exit<unknown, void>) => {
                if (exit._tag === "Success") return outputs.offer({ _tag: "end" });
                return Cause.isInterruptedOnly(exit.cause)
                  ? failCause(exit.cause)
                  : outputs.offer({ _tag: "fail", cause: exit.cause });
              },
            );
          };

          const startBranches = (index: number): Eff<void, never> =>
            index >= branches.length
              ? succeed(undefined)
              : (fork(branchDriver(index)) as any).flatMap((fiber: Fiber<any>) => {
                  drivers.push(fiber);
                  return startBranches(index + 1);
                });

          const offerInput = (slot: InputSlot, index = 0): Eff<void, any> => {
            if (index >= inputs.length) return succeed(undefined);
            if (!active[index]) return offerInput(slot, index + 1);
            return (inputs[index]!.offer(slot) as any)
              .catch((error: unknown) =>
                error instanceof QueueClosed ? succeed(false) : fail(error),
              )
              .flatMap(() => offerInput(slot, index + 1));
          };

          const drainUpstream = (stream: Stream<A, any>): Eff<void, any> =>
            (stream.step as any).flatMap((step: Step<A>) => {
              if (step._tag === "Done") return offerInput({ _tag: "end" });
              return (offerInput({ _tag: "chunk", chunk: step.chunk }) as any).flatMap(() =>
                active.some(Boolean) ? drainUpstream(step.next) : succeed(undefined),
              );
            });

          const upstreamDriver = (drainUpstream(self) as any).catchAllCause((cause: Cause) =>
            Cause.isInterruptedOnly(cause) ? failCause(cause) : offerInput({ _tag: "fail", cause }),
          );

          const pullOutput = (open: { count: number }): Eff<Step<unknown>, any> =>
            (outputs.take() as any).flatMap((slot: OutputSlot): any => {
              if (slot._tag === "fail") return failCause(slot.cause);
              if (slot._tag === "end") {
                open.count--;
                return open.count === 0 ? succeed(DONE) : pullOutput(open);
              }
              return succeed(
                emit(
                  Chunk.single(slot.value),
                  new Stream<unknown, any>(suspend(() => pullOutput(open)) as any),
                ),
              );
            });

          return (startBranches(0) as any).flatMap(() =>
            (fork(upstreamDriver) as any).flatMap((fiber: Fiber<any>) => {
              drivers.push(fiber);
              return pullOutput({ count: branches.length });
            }),
          );
        },
      ),
    );

    return new Stream<unknown, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
    );
  }

  /**
   * Run a side pipeline over the same single-pass source while preserving only
   * the source values in the result. The observer is reliable and
   * backpressured: its failure fails the stream, and downstream completion
   * waits for its interruption and finalization.
   */
  observe<B, S2>(observer: (stream: Stream<A, S>) => Stream<B, S2>): Stream<A, S | S2> {
    type Observed = { readonly _tag: "source"; readonly value: A } | { readonly _tag: "observer" };

    return this.broadcastThrough(
      (stream) =>
        stream.map<Observed>((value) => ({
          _tag: "source",
          value,
        })),
      (stream) => observer(stream).map<Observed>(() => ({ _tag: "observer" })),
    ).filterMap((event: Observed) => (event._tag === "source" ? event.value : undefined)) as any;
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

    return new Stream<B, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
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

    return new Stream<B, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
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

    return new Stream<Chunk<A>, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
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

    return new Stream<A, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
    ) as any;
  }

  sample(intervalMs: number): Stream<A, S> {
    const self = this;
    const interval = Math.max(1, Math.floor(intervalMs));
    type Event = { readonly _tag: "end" } | { readonly _tag: "fail"; readonly cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<A>, any> = (QueueNS.bounded<Event>(1) as any).flatMap(
      (events: Queue<Event>) => {
        let dirty = false;
        let latest!: A;

        const drain = (stream: Stream<A, any>): Eff<void, any> =>
          (stream.step as any).flatMap((step: Step<A>) => {
            if (step._tag === "Done") return events.offer({ _tag: "end" });
            return sync(() => {
              for (const value of step.chunk) {
                latest = value;
                dirty = true;
              }
            }).flatMap(() => drain(step.next));
          });

        const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause) ? failCause(cause) : events.offer({ _tag: "fail", cause }),
        );

        const pull = (): Eff<Step<A>, any> =>
          (timeoutOption(events.take() as any, interval) as any).flatMap(
            (event: Event | undefined): any => {
              if (event?._tag === "fail") return failCause(event.cause);
              if (event?._tag === "end") return succeed(DONE);
              if (!dirty) return pull();
              return sync(() => {
                const value = latest;
                dirty = false;
                return emit(Chunk.single(value), new Stream(suspend(() => pull()) as any));
              });
            },
          );

        return (fork(driver) as any).flatMap((fiber: Fiber<any>) => {
          drivers.push(fiber);
          return pull();
        });
      },
    );

    return new Stream<A, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
    ) as any;
  }

  audit(ms: number): Stream<A, S> {
    const self = this;
    const duration = Math.max(1, Math.floor(ms));
    type Event =
      | { readonly _tag: "start" }
      | { readonly _tag: "end" }
      | { readonly _tag: "fail"; readonly cause: Cause };

    const drivers: Fiber<any>[] = [];

    const setup: Eff<Step<A>, any> = (QueueNS.unbounded<Event>() as any).flatMap(
      (events: Queue<Event>) => {
        let windowOpen = false;
        let dirty = false;
        let latest!: A;

        const drain = (stream: Stream<A, any>): Eff<void, any> =>
          (stream.step as any).flatMap((step: Step<A>) => {
            if (step._tag === "Done") return events.offer({ _tag: "end" });
            return sync(() => {
              let open = false;
              for (const value of step.chunk) {
                latest = value;
                dirty = true;
                if (!windowOpen) {
                  windowOpen = true;
                  open = true;
                }
              }
              return open;
            }).flatMap((open) =>
              (open ? events.offer({ _tag: "start" }) : succeed(undefined)).flatMap(() =>
                drain(step.next),
              ),
            );
          });

        const driver = (drain(self) as any).catchAllCause((cause: Cause) =>
          Cause.isInterruptedOnly(cause) ? failCause(cause) : events.offer({ _tag: "fail", cause }),
        );

        const emitLatest = (next: Stream<A, any>): Eff<Step<A>, any> =>
          sync(() => {
            const value = latest;
            dirty = false;
            windowOpen = false;
            return emit(Chunk.single(value), next);
          });

        const waitForWindow = (): Eff<Step<A>, any> =>
          (timeoutOption(events.take() as any, duration) as any).flatMap(
            (event: Event | undefined): any => {
              if (event === undefined) {
                return emitLatest(new Stream(suspend(() => idle()) as any));
              }
              if (event._tag === "fail") return failCause(event.cause);
              if (event._tag === "end") {
                return dirty ? emitLatest(Stream.empty()) : succeed(DONE);
              }
              return waitForWindow();
            },
          );

        const idle = (): Eff<Step<A>, any> =>
          (events.take() as any).flatMap((event: Event): any => {
            if (event._tag === "fail") return failCause(event.cause);
            if (event._tag === "end") return succeed(DONE);
            return waitForWindow();
          });

        return (fork(driver) as any).flatMap((fiber: Fiber<any>) => {
          drivers.push(fiber);
          return idle();
        });
      },
    );

    return new Stream<A, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
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

    return new Stream<A, any>(
      suspend(() => setup) as any,
      combineFinalizers(interruptAllEff(drivers), self._finalizer),
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

  /**
   * Fail with {@link StreamTimeoutError} if any single pull takes longer
   * than `ms` to produce a step — i.e. the gap between emitted chunks (or
   * between subscription and the first chunk) exceeds the limit.
   *
   * Consumer-side and Clock-routed (`timeoutOption`), so a TestClock drives
   * it deterministically; the in-flight pull is interrupted when the timer
   * fires.
   */
  timeout(ms: number): Stream<A, S | Throws<StreamTimeoutError>> {
    const wrap = (s: Stream<A, any>): Stream<A, any> =>
      new Stream(
        (timeoutOption(s.step as any, ms) as any).flatMap((step: Step<A> | undefined) => {
          if (step === undefined) return fail(new StreamTimeoutError({ ms }));
          if (step._tag === "Done") return succeed(DONE);
          return succeed(emit(step.chunk, wrap(step.next)));
        }),
        s._finalizer,
      );
    return wrap(this) as any;
  }

  /**
   * End the stream gracefully (Done, not a failure) when the AbortSignal
   * aborts — even while a pull is blocked mid-wait. Each pull races the
   * upstream step against an async that resolves on abort; whichever side
   * loses is interrupted, which also removes the abort listener, so nothing
   * leaks after the stream terminates.
   */
  interruptOn(signal: AbortSignal): Stream<A, S> {
    const abortStep: Eff<Step<A>, never> = async<Step<A>>((resume) => {
      if (signal.aborted) {
        resume(succeed(DONE) as any);
        return;
      }
      const onAbort = () => resume(succeed(DONE) as any);
      signal.addEventListener("abort", onAbort, { once: true });
      return () => signal.removeEventListener("abort", onAbort);
    }) as any;
    const wrap = (s: Stream<A, any>): Stream<A, any> =>
      new Stream(
        suspend(() =>
          signal.aborted
            ? (succeed(DONE) as any)
            : race([
                (s.step as any).map((step: Step<A>) =>
                  step._tag === "Done" ? DONE : emit(step.chunk, wrap(step.next)),
                ),
                abortStep as any,
              ]),
        ) as any,
        s._finalizer,
      );
    return wrap(this);
  }

  /**
   * End the stream gracefully once `ms` milliseconds (Clock time, anchored
   * at the first pull) have elapsed. A pull still blocked when the deadline
   * hits is interrupted and the stream completes with Done rather than
   * failing. Clock-routed — a TestClock drives it deterministically.
   */
  interruptAfter(ms: number): Stream<A, S> {
    const self = this;
    const wrap = (deadline: number, s: Stream<A, any>): Stream<A, any> =>
      new Stream(
        (clockNow as any).flatMap((now: number) => {
          const remaining = deadline - now;
          if (remaining <= 0) return succeed(DONE);
          return (timeoutOption(s.step as any, remaining) as any).map(
            (step: Step<A> | undefined) => {
              if (step === undefined || step._tag === "Done") return DONE;
              return emit(step.chunk, wrap(deadline, step.next));
            },
          );
        }),
        s._finalizer,
      );
    return new Stream(
      (clockNow as any).flatMap((now: number) => wrap(now + ms, self).step),
      self._finalizer,
    );
  }

  // ── Pipe ─────────────────────────────────────────────────────────

  through<B, S2>(pipe: Pipe<A, B, S2>): Stream<B, S | S2> {
    return pipe(this) as any;
  }

  runSink<B, S2>(sink: { run<S3>(input: Stream<A, S3>): Eff<B, S2 | S3> }): Eff<B, S | S2> {
    return sink.run(this) as any;
  }

  // ── Error handling ───────────────────────────────────────────────

  catch<B, S2>(
    handler: (error: ErrorsOf<S>) => Stream<B, S2>,
  ): Stream<A | B, Exclude<S, Throws<unknown>> | S2> {
    const self = this;
    let recoveryFinalizer: Eff<void, unknown> | null = null;

    const releaseRecovery = (): Eff<void, unknown> =>
      suspend(() => {
        const finalizer = recoveryFinalizer;
        recoveryFinalizer = null;
        return finalizer ?? succeed(undefined);
      });

    const wrap = (source: Stream<A, any>): Stream<A | B, any> =>
      new Stream(
        (source.step as any)
          .map((step: Step<A>) =>
            step._tag === "Done" ? DONE : emit(step.chunk, wrap(step.next as Stream<A, any>)),
          )
          .catch((error: ErrorsOf<S>) => {
            const recovery = handler(error);
            recoveryFinalizer = recovery._finalizer;
            return recovery.step;
          }),
      );

    return new Stream(
      wrap(self).step,
      combineFinalizers(self._finalizer, suspend(releaseRecovery)),
    ) as any;
  }

  catchTag<Tag extends string, B, S2>(
    tag: Tag,
    handler: (error: Extract<ErrorsOf<S>, { readonly _tag: Tag }>) => Stream<B, S2>,
  ): Stream<A | B, ExcludeTags<S, Tag> | S2> {
    return this.catch(((error: ErrorsOf<S>) => {
      if (error !== null && typeof error === "object" && (error as any)._tag === tag) {
        return handler(error as Extract<ErrorsOf<S>, { readonly _tag: Tag }>);
      }
      return Stream.fail(error);
    }) as any) as any;
  }

  catchSome<B, S2>(
    handler: (error: ErrorsOf<S>) => Stream<B, S2> | undefined,
  ): Stream<A | B, S | S2> {
    return this.catch(((error: ErrorsOf<S>) => handler(error) ?? Stream.fail(error)) as any) as any;
  }

  catchAllCause<B, S2>(
    handler: (cause: Cause<ErrorsOf<S>>) => Stream<B, S2>,
  ): Stream<A | B, Exclude<S, Throws<unknown>> | S2> {
    const self = this;
    let recoveryFinalizer: Eff<void, unknown> | null = null;

    const releaseRecovery = (): Eff<void, unknown> =>
      suspend(() => {
        const finalizer = recoveryFinalizer;
        recoveryFinalizer = null;
        return finalizer ?? succeed(undefined);
      });

    const wrap = (source: Stream<A, any>): Stream<A | B, any> =>
      new Stream(
        (source.step as any)
          .map((step: Step<A>) =>
            step._tag === "Done" ? DONE : emit(step.chunk, wrap(step.next as Stream<A, any>)),
          )
          .catchAllCause((cause: Cause<ErrorsOf<S>>) => {
            const recovery = handler(cause);
            recoveryFinalizer = recovery._finalizer;
            return recovery.step;
          }),
      );

    return new Stream(
      wrap(self).step,
      combineFinalizers(self._finalizer, suspend(releaseRecovery)),
    ) as any;
  }

  mapError<E2>(f: (error: ErrorsOf<S>) => E2): Stream<A, Exclude<S, Throws<unknown>> | Throws<E2>> {
    return this.catch((error) => Stream.fail(f(error))) as any;
  }

  tapError<S2>(f: (error: ErrorsOf<S>) => Eff<unknown, S2>): Stream<A, S | S2> {
    return this.catch((error) =>
      Stream.fromEffect(f(error)).flatMap(() => Stream.fail(error)),
    ) as any;
  }

  tapErrorCause<S2>(f: (cause: Cause<ErrorsOf<S>>) => Eff<unknown, S2>): Stream<A, S | S2> {
    return this.catchAllCause((cause) =>
      Stream.fromEffect(f(cause)).flatMap(() => Stream.fromEffect(failCause(cause))),
    ) as any;
  }

  either(): Stream<Either<ErrorsOf<S>, A>, Exclude<S, Throws<unknown>>> {
    return this.map<Either<ErrorsOf<S>, A>>((value) => ({ _tag: "Right", right: value })).catch(
      (error) => Stream.succeed({ _tag: "Left", left: error } as Either<ErrorsOf<S>, A>),
    ) as any;
  }

  attempt(): Stream<Either<ErrorsOf<S>, A>, Exclude<S, Throws<unknown>>> {
    return this.either();
  }

  exit(): Stream<Exit<ErrorsOf<S>, A>, Exclude<S, Throws<unknown>>> {
    return this.map<Exit<ErrorsOf<S>, A>>((value) => Exit.succeed(value)).catchAllCause((cause) =>
      Stream.succeed(Exit.failure(cause)),
    ) as any;
  }

  attemptCause(): Stream<Exit<ErrorsOf<S>, A>, Exclude<S, Throws<unknown>>> {
    return this.exit();
  }

  /**
   * If this stream fails, switch to the fallback stream. Elements emitted
   * before the failure are preserved; the failure itself is replaced by
   * whatever the (lazily built) fallback produces.
   */
  orElse<B, S2>(that: () => Stream<B, S2>): Stream<A | B, Exclude<S, Throws<unknown>> | S2> {
    return this.catch(() => that());
  }

  onFinalize<S2>(finalizer: Eff<void, S2>): Stream<A, S | S2> {
    return this._withFinalizer(finalizer);
  }

  /**
   * Retry each pull according to the given policy. If a step fails, retry it
   * up to the policy's limit. Once a chunk emits, retry resets — failures in
   * the next pull are retried independently.
   *
   * Use {@link Stream.retryFrom} when failure must reacquire and restart the
   * whole source rather than retrying only its current pull.
   */
  retry(policy: RetryPolicy | RetryConfig): Stream<A, S> {
    const wrap = (s: Stream<A, S>): Stream<A, S> =>
      new Stream(
        (effRetry(s.step as any, policy as any) as any).map((step: Step<A>) => {
          if (step._tag === "Done") return DONE;
          return emit(step.chunk, wrap(step.next as any));
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

// Polymorphic in the input's effect union: a pipe THREADS the upstream S
// through to its output (adding its own S2, e.g. a parse error) instead of
// erasing it. `.through()` therefore preserves requirements end to end.
export type Pipe<I, O, S2 = never> = (<S>(input: Stream<I, S>) => Stream<O, S | S2>) & {
  readonly "~perfect/PipeOutput"?: O;
  readonly "~perfect/PipeEffects"?: S2;
};

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
