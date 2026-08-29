// ---------------------------------------------------------------------------
// PgChangeStream<T> — LISTEN/NOTIFY-based change stream with poll fallback
//
// Implements Streamable<T> and Replayable<T> for real-time CDC:
//   - Primary: LISTEN on a Postgres channel for instant notifications
//   - Fallback: periodic poll to catch any missed events (at-least-once)
//   - Replayable: subscribe from a specific offset (timestamp or sequence)
//
// LISTEN/NOTIFY is lossy — if the consumer is down, notifications are lost.
// The poll-based fallback ensures at-least-once delivery by periodically
// checking for rows newer than the last seen timestamp.
//
// Ported from promin (Effect-TS StreamPipeline → perfect Stream): the merged
// LISTEN+poll stream is deduped globally by JSON key — promin's `dedupe()`
// (consecutive-only, reference equality) could not actually drop the
// poll-side copy of a notified object. Memory grows with distinct events;
// scope a subscription's lifetime accordingly.
// ---------------------------------------------------------------------------

import { async as asyncEff, fail, succeed, type Throws } from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import { sql } from "drizzle-orm";
import { JsonCodec } from "@spilne/perfect-core/connect";
import type {
  Streamable,
  Replayable,
  Offset,
  Codec,
  ConsumerGroup,
} from "@spilne/perfect-core/connect";
import type { DrizzleDb } from "./drizzle-db";
import { pollStream } from "./poll-stream";
import { PostgresError, toPostgresError } from "./postgres-error";
import type postgres from "postgres";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface PgChangeStreamConfig<T> {
  /** Drizzle database instance (for poll queries). */
  db: DrizzleDb;
  /**
   * Raw postgres-js client (for LISTEN/NOTIFY).
   * Required because Drizzle doesn't expose LISTEN.
   */
  sql: ReturnType<typeof postgres>;
  /** Postgres NOTIFY channel name. */
  channel: string;
  /** Table to poll for changes. Must have a timestamp column for ordering. */
  table: string;
  /** Column name used for ordering/filtering (e.g. "created_at", "updated_at"). */
  timestampColumn?: string;
  /** Column name for a monotonic sequence (e.g. "id"). Used for specific offsets. */
  sequenceColumn?: string;
  /** Payload column to read (e.g. "payload"). Default: entire row as JSON. */
  payloadColumn?: string;
  /** Codec for deserializing payloads. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Poll interval in ms for the fallback poller. Default: 5000. */
  pollIntervalMs?: number;
  /** Max rows per poll batch. Default: 100. */
  pollBatchSize?: number;
}

/** Map a connect Offset to the poll cursor timestamp. Pure — exported for tests. */
export function offsetToDate(offset: Offset): Date {
  switch (offset.type) {
    case "earliest":
      return new Date(0);
    case "latest":
      return new Date();
    case "timestamp":
      return new Date(offset.value);
    case "specific":
      // Interpret as ISO timestamp string
      return new Date(offset.value);
  }
}

// ---------------------------------------------------------------------------
// PgChangeStream
// ---------------------------------------------------------------------------

export class PgChangeStream<T>
  implements Streamable<T, Throws<PostgresError>>, Replayable<T, Throws<PostgresError>>
{
  readonly codec: Codec<T>;
  private readonly db: DrizzleDb;
  private readonly sqlClient: ReturnType<typeof postgres>;
  private readonly channel: string;
  private readonly table: string;
  private readonly timestampColumn: string;
  private readonly sequenceColumn: string;
  private readonly payloadColumn: string | undefined;
  private readonly pollIntervalMs: number;
  private readonly pollBatchSize: number;

  constructor(config: PgChangeStreamConfig<T>) {
    this.db = config.db;
    this.sqlClient = config.sql;
    this.channel = config.channel;
    this.table = config.table;
    this.timestampColumn = config.timestampColumn ?? "created_at";
    this.sequenceColumn = config.sequenceColumn ?? "id";
    this.payloadColumn = config.payloadColumn;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.pollIntervalMs = config.pollIntervalMs ?? 5000;
    this.pollBatchSize = config.pollBatchSize ?? 100;
  }

  // ---------------------------------------------------------------------------
  // Streamable<T> — LISTEN + poll merged stream
  // ---------------------------------------------------------------------------

  subscribe(_params?: { group?: ConsumerGroup }): Stream<T, Throws<PostgresError>> {
    return this.subscribeFrom({ offset: { type: "latest" } });
  }

  // ---------------------------------------------------------------------------
  // Replayable<T> — subscribe from offset
  // ---------------------------------------------------------------------------

  subscribeFrom(params: {
    offset: Offset;
    group?: ConsumerGroup;
  }): Stream<T, Throws<PostgresError>> {
    const listenStream = this.createListenStream();
    const pollStream = this.createPollStream(params.offset);

    // Merge both sources — LISTEN for low latency, poll for reliability
    return listenStream.merge(pollStream).dedupe((v) => JSON.stringify(v));
  }

  // ---------------------------------------------------------------------------
  // LISTEN stream — real-time notifications
  // ---------------------------------------------------------------------------

  private createListenStream(): Stream<T, Throws<PostgresError>> {
    const codec = this.codec;
    const sqlClient = this.sqlClient;
    const channel = this.channel;

    return Stream.async<T, Throws<PostgresError>>((emit) =>
      asyncEff<() => void, PostgresError>((resume) => {
        let canceled = false;
        let listener: { unlisten(): Promise<void> } | undefined;
        const close = () => {
          if (listener) void listener.unlisten().catch(() => {});
        };

        void sqlClient
          .listen(channel, (payload: string) => {
            try {
              const parsed = JSON.parse(payload);
              emit(codec.decode(parsed));
            } catch {
              // Skip malformed payloads
            }
          })
          .then(
            (activeListener) => {
              listener = activeListener;
              if (canceled) close();
              else resume(succeed(close));
            },
            (cause) => {
              if (!canceled) resume(fail(toPostgresError("changeStream.listen", cause)));
            },
          );

        return () => {
          canceled = true;
          close();
        };
      }),
    );
  }

  // ---------------------------------------------------------------------------
  // Poll stream — periodic catch-up for at-least-once delivery
  // ---------------------------------------------------------------------------

  private createPollStream(offset: Offset): Stream<T, Throws<PostgresError>> {
    let cursor = offsetToDate(offset);

    return pollStream(
      async () => {
        const rows = await this.pollSince(cursor);
        if (rows.length > 0) {
          // Advance cursor to latest row's timestamp
          const lastRow = rows[rows.length - 1]!;
          cursor = new Date(lastRow.ts.getTime() + 1);
        }
        return rows.map((r) => r.value);
      },
      this.pollIntervalMs,
      "changeStream.poll",
    );
  }

  private async pollSince(since: Date): Promise<{ value: T; ts: Date }[]> {
    const tsCol = this.timestampColumn;
    const seqCol = this.sequenceColumn;
    const table = this.table;
    const limit = this.pollBatchSize;
    const payloadExpr = this.payloadColumn ? `${this.payloadColumn}` : `row_to_json(t)`;

    const rows = (await this.db.execute(
      sql.raw(`
        SELECT ${payloadExpr} as payload, "${tsCol}" as ts
        FROM "${table}" t
        WHERE "${tsCol}" >= '${since.toISOString()}'
        ORDER BY "${seqCol}" ASC
        LIMIT ${limit}
      `),
    )) as any[];

    return rows.map((r: any) => ({
      value: this.codec.decode(r.payload),
      ts: r.ts instanceof Date ? r.ts : new Date(r.ts),
    }));
  }

  // ---------------------------------------------------------------------------
  // Publish — NOTIFY helper for producers
  // ---------------------------------------------------------------------------

  /**
   * Send a NOTIFY on the configured channel.
   * Call this after INSERT/UPDATE to push real-time events.
   */
  async notify(value: T): Promise<void> {
    const payload = JSON.stringify(this.codec.encode(value));
    await this.sqlClient.notify(this.channel, payload);
  }

  // ---------------------------------------------------------------------------
  // Trigger helpers — install/remove Postgres trigger for auto-NOTIFY
  // ---------------------------------------------------------------------------

  /**
   * Install a trigger on the table that auto-NOTIFYs on INSERT.
   * The trigger sends the payload column (or row JSON) as the notification payload.
   */
  async installTrigger(): Promise<void> {
    const fnName = `notify_${this.channel}`;
    const triggerName = `trg_notify_${this.channel}`;
    const payloadExpr = this.payloadColumn
      ? `NEW."${this.payloadColumn}"::text`
      : `row_to_json(NEW)::text`;

    await this.db.execute(
      sql.raw(`
      CREATE OR REPLACE FUNCTION ${fnName}() RETURNS trigger AS $$
      BEGIN
        PERFORM pg_notify('${this.channel}', ${payloadExpr});
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `),
    );

    await this.db.execute(
      sql.raw(`
      DROP TRIGGER IF EXISTS ${triggerName} ON "${this.table}";
      CREATE TRIGGER ${triggerName}
        AFTER INSERT ON "${this.table}"
        FOR EACH ROW EXECUTE FUNCTION ${fnName}();
    `),
    );
  }

  /** Remove the auto-NOTIFY trigger from the table. */
  async removeTrigger(): Promise<void> {
    const fnName = `notify_${this.channel}`;
    const triggerName = `trg_notify_${this.channel}`;

    await this.db.execute(
      sql.raw(`
      DROP TRIGGER IF EXISTS ${triggerName} ON "${this.table}";
      DROP FUNCTION IF EXISTS ${fnName}();
    `),
    );
  }
}
