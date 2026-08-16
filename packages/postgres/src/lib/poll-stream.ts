// ---------------------------------------------------------------------------
// pollStream — shared poll-loop → Stream adapter for Postgres-backed sources
//
// Runs a Promise-returning batch fetch in a loop and flattens the batches
// into a Stream. Sleeps `intervalMs` only after an EMPTY batch — while the
// source has data, throughput is bounded by query latency, not the poll
// interval. Driver rejections become defects (the stream dies loudly),
// matching promin's Effect.promise semantics.
// ---------------------------------------------------------------------------

import { fromPromise, sleep, succeed } from "@perfect/core";
import { Stream } from "@perfect/core/stream";

export function pollStream<R>(poll: () => Promise<R[]>, intervalMs: number): Stream<R, never> {
  const batch = fromPromise(poll, (e) => e)
    .orDie()
    .flatMap((rows) => (rows.length > 0 ? succeed(rows) : sleep(intervalMs).map(() => rows)));
  return Stream.repeat(batch).flatMap((rows) => Stream.fromArray(rows));
}
