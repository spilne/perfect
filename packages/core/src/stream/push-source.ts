// The engine behind Stream.fromCallback, Stream.async and Stream.asyncChunks.
//
// These are "push" sources: some outside code (a callback, an event emitter,
// a Kafka consumer) calls emit() whenever it has a value, but the stream
// only takes values when the consumer asks for the next chunk. In between,
// values wait in a buffer.
//
// When the buffer is full, `overflow` decides what happens:
//   "backpressure" (default) - keep the value, and make emit() return a
//                              Promise that resolves once there is room.
//                              A producer that awaits it slows down to the
//                              consumer's speed. Nothing is ever dropped.
//   "dropNewest"             - throw away the new value.
//   "dropOldest"             - throw away the oldest buffered value.
//
// Before, every source silently dropped new values once 1024 were buffered.

import { type Eff, Suspend, Op } from "../eff.js";
import { succeed, fail, sync } from "../constructors.js";
import { Deque } from "../internal/deque.js";
import { Chunk } from "./chunk.js";
import { Stream, type Step } from "./stream.js";

export type PushOverflow = "backpressure" | "dropNewest" | "dropOldest";

export interface PushOptions {
  /** How many items can wait in the buffer. Default 1024. */
  readonly bufferSize?: number;
  /** What to do when the buffer is full. Default "backpressure". */
  readonly overflow?: PushOverflow;
}

/** What emit() returns: nothing when there is room, or a Promise to await. */
export type EmitResult = void | Promise<void>;

const DONE: Step<never> = { _tag: "Done" };

export function pushOptions(arg: number | PushOptions | undefined): Required<PushOptions> {
  const options = typeof arg === "number" ? { bufferSize: arg } : (arg ?? {});
  const bufferSize = options.bufferSize ?? 1024;
  if (!(bufferSize >= 1)) throw new RangeError("bufferSize must be at least 1");
  return { bufferSize, overflow: options.overflow ?? "backpressure" };
}

export function pushStream<A, T, S>(params: {
  // true: emit() takes a whole Chunk, and each chunk becomes one step.
  // false: emit() takes one value, and a pull takes everything buffered.
  readonly chunked: boolean;
  readonly options: Required<PushOptions>;
  readonly register: (
    emit: (item: T) => EmitResult,
    close: () => void,
    failStream: (error: unknown) => void,
  ) => Eff<(() => void) | void, S>;
}): Stream<A, S> {
  const { chunked, register } = params;
  const { bufferSize, overflow } = params.options;

  // Stream.suspend gives every run its own buffer and its own cleanup.
  return Stream.suspend(() => {
    const buffer = new Deque<T>();
    let closed = false;
    let failure: { readonly error: unknown } | null = null;
    // The pull that is waiting for a value, if any.
    let waiter: ((effect: Eff<Step<A>, unknown>, onDiscard?: () => void) => void) | null = null;
    let cleanup: (() => void) | void;
    let cleaned = false;
    // Producers waiting for room in a full buffer share one promise.
    let room: { readonly promise: Promise<void>; readonly resolve: () => void } | null = null;

    const waitForRoom = (): Promise<void> => {
      if (room === null) {
        let resolve!: () => void;
        const promise = new Promise<void>((r) => (resolve = r));
        room = { promise, resolve };
      }
      return room.promise;
    };

    const wakeProducers = (): void => {
      if (room === null) return;
      const { resolve } = room;
      room = null;
      resolve();
    };

    const cleanupOnce = (): void => {
      if (cleaned) return;
      cleaned = true;
      // Nobody will read any more. Later emits are ignored, and producers
      // waiting for room are let go.
      closed = true;
      buffer.clear();
      wakeProducers();
      if (cleanup) cleanup();
    };

    const asChunk = (item: T): Chunk<A> =>
      chunked ? (item as unknown as Chunk<A>) : Chunk.single(item as unknown as A);

    const takeChunk = (): Chunk<A> =>
      chunked
        ? (buffer.shift() as unknown as Chunk<A>)
        : Chunk.fromArray(buffer.drain() as unknown as A[]);

    // Hand a chunk to the waiting pull. If that pull's fiber is interrupted
    // before it uses the chunk, the chunk comes back here (giveBack).
    const deliver = (chunk: Chunk<A>): void => {
      const w = waiter!;
      waiter = null;
      w(succeed(emit(chunk, next())), () => giveBack(chunk));
    };

    // Put a chunk back at the front, so no value is lost or reordered.
    const giveBack = (chunk: Chunk<A>): void => {
      if (waiter !== null) {
        deliver(chunk);
        return;
      }
      if (chunked) {
        buffer.insert(0, chunk as unknown as T);
        return;
      }
      const rest = buffer.drain();
      for (const value of chunk) buffer.push(value as unknown as T);
      for (const item of rest) buffer.push(item);
    };

    const pushEmit = (item: T): EmitResult => {
      if (closed) return;
      if (chunked && (item as unknown as Chunk<A>).isEmpty) return;
      if (waiter !== null) {
        deliver(asChunk(item));
        return;
      }
      if (buffer.length >= bufferSize) {
        if (overflow === "dropNewest") return;
        if (overflow === "dropOldest") buffer.shift();
      }
      buffer.push(item);
      if (overflow === "backpressure" && buffer.length >= bufferSize) return waitForRoom();
    };

    const pushClose = (): void => {
      if (closed) return;
      closed = true;
      if (waiter !== null && buffer.length === 0) {
        const w = waiter;
        waiter = null;
        cleanupOnce();
        w(succeed(DONE));
      }
    };

    const pushFail = (error: unknown): void => {
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
          (resume: (eff: any, onDiscard?: () => void) => void) => {
            if (buffer.length > 0) {
              const chunk = takeChunk();
              if (buffer.length < bufferSize) wakeProducers();
              resume(succeed(emit(chunk, next())), () => giveBack(chunk));
              return;
            }
            // Values buffered before a failure or a close are still
            // delivered first (above).
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
            waiter = (effect, onDiscard) => resume(effect, onDiscard);
            // The consumer was interrupted while waiting: stop the source.
            return () => {
              closed = true;
              buffer.clear();
              waiter = null;
              cleanupOnce();
            };
          },
          null,
        ) as any,
      );
    }

    const start = (register(pushEmit, pushClose, pushFail) as any).flatMap(
      (c: (() => void) | void) => {
        cleanup = c ?? undefined;
        // The stream may already have been cleaned up while register ran.
        if (cleaned && cleanup) cleanup();
        return next().step;
      },
    );
    return new Stream<A, S>(start, sync(cleanupOnce));
  });
}

function emit<A>(chunk: Chunk<A>, next: Stream<A, unknown>): Step<A> {
  return { _tag: "Emit", chunk, next };
}
