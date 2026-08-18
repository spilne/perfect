// ---------------------------------------------------------------------------
// pollStream — shared poll-loop → Stream adapter for Postgres-backed sources
//
// Runs a Promise-returning batch fetch in a loop and flattens the batches
// into a Stream. Sleeps `intervalMs` only after an EMPTY batch — while the
// source has data, throughput is bounded by query latency, not the poll
// interval. Driver rejections stay in the typed channel as PostgresError.
// ---------------------------------------------------------------------------

import { fromPromise, sleep, succeed } from "@perfect/core";
import type { Throws } from "@perfect/core";
import { Stream } from "@perfect/core/stream";
import { PostgresError, toPostgresError } from "./postgres-error";

export function pollStream<R>(
  poll: () => Promise<R[]>,
  intervalMs: number,
  operation: string,
): Stream<R, Throws<PostgresError>> {
  const batch = fromPromise(poll, (cause) => toPostgresError(operation, cause)).flatMap((rows) =>
    rows.length > 0 ? succeed(rows) : sleep(intervalMs).map(() => rows),
  );
  return Stream.repeat(batch).flatMap((rows) => Stream.fromArray(rows));
}
