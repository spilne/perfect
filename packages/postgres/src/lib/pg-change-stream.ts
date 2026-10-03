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
// The same event usually arrives twice (once from LISTEN, once from the
// poll), so the merged stream drops values it has seen recently. "Recently"
// is a bounded window (`dedupeWindow`, default 10 000 events), so memory
// stays flat on a long-running subscription.
//
// The poll keeps a (timestamp, sequence) cursor and asks for rows strictly
// after it, ordered the same way. That way rows that share a timestamp, or
// whose sequence order differs from their timestamp order, are not skipped.
// A row committed late with an older timestamp than rows already read can
// still be missed; use a commit-ordered column for the timestamp if that
// matters.
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
import { type DrizzleDb, execRaw } from "./drizzle-db.js";
import { pollStream } from "./poll-stream.js";
import { PostgresError, toPostgresError } from "./postgres-error.js";
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
  /** How many recent events to remember for dropping duplicates. Default: 10 000. */
  dedupeWindow?: number;
}

// Channel names end up inside a trigger function body, where we can't use
// query parameters, so we only accept plain identifier names.
const CHANNEL_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

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
  private readonly dedupeWindow: number;

  constructor(config: PgChangeStreamConfig<T>) {
    if (!CHANNEL_NAME.test(config.channel)) {
      throw new RangeError(
        `PgChangeStream: channel must be a plain name (letters, digits, _), got ${JSON.stringify(config.channel)}`,
      );
    }
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
    this.dedupeWindow = config.dedupeWindow ?? 10_000;
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
    const window = this.dedupeWindow;
    return Stream.suspend(() => {
      const recent = new RecentKeys(window);
      return listenStream.merge(pollStream).filter((v) => recent.add(JSON.stringify(v)));
    });
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
              // A subscriber interrupted before it receives `close` unlistens.
              else resume(succeed(close), close);
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
    return Stream.suspend(() => {
      // Where the next poll starts. `ts` is kept as the text Postgres gave
      // us: a JS Date would drop the microseconds, and the poll would keep
      // re-reading its own last row.
      let cursor: PollCursor = { ts: offsetToDate(offset).toISOString(), seq: null };

      return pollStream(
        async () => {
          const rows = await this.pollAfter(cursor);
          const last = rows.at(-1);
          if (last !== undefined) cursor = { ts: last.ts, seq: last.seq };
          return rows.map((r) => r.value);
        },
        this.pollIntervalMs,
        "changeStream.poll",
      );
    });
  }

  private async pollAfter(cursor: PollCursor): Promise<{ value: T; ts: string; seq: unknown }[]> {
    const ts = sql.identifier(this.timestampColumn);
    const seq = sql.identifier(this.sequenceColumn);
    const table = sql.identifier(this.table);
    const payload = this.payloadColumn
      ? sql`t.${sql.identifier(this.payloadColumn)}`
      : sql`row_to_json(t)`;
    // First poll: everything from the offset's time. After that: strictly
    // after the last row we read, in (timestamp, sequence) order.
    const after =
      cursor.seq === null
        ? sql`t.${ts} >= ${cursor.ts}::timestamptz`
        : sql`(t.${ts}, t.${seq}) > (${cursor.ts}::timestamptz, ${cursor.seq})`;

    const rows = await execRaw(
      this.db,
      sql`
        SELECT ${payload} AS payload, t.${ts}::text AS ts, t.${seq} AS seq
        FROM ${table} t
        WHERE ${after}
        ORDER BY t.${ts} ASC, t.${seq} ASC
        LIMIT ${this.pollBatchSize}
      `,
    );

    return rows.map((r) => ({
      value: this.codec.decode(r.payload),
      ts: String(r.ts),
      seq: r.seq,
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
    const fn = sql.identifier(`notify_${this.channel}`);
    const trigger = sql.identifier(`trg_notify_${this.channel}`);
    const table = sql.identifier(this.table);
    const payload = this.payloadColumn
      ? sql`NEW.${sql.identifier(this.payloadColumn)}::text`
      : sql`row_to_json(NEW)::text`;
    // The channel name was checked in the constructor, so it is safe to put
    // inside the function body as a literal.
    const channel = sql.raw(`'${this.channel}'`);

    await this.db.execute(sql`
      CREATE OR REPLACE FUNCTION ${fn}() RETURNS trigger AS $$
      BEGIN
        PERFORM pg_notify(${channel}, ${payload});
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);
    await this.db.execute(sql`DROP TRIGGER IF EXISTS ${trigger} ON ${table}`);
    await this.db.execute(sql`
      CREATE TRIGGER ${trigger}
        AFTER INSERT ON ${table}
        FOR EACH ROW EXECUTE FUNCTION ${fn}()
    `);
  }

  /** Remove the auto-NOTIFY trigger from the table. */
  async removeTrigger(): Promise<void> {
    const fn = sql.identifier(`notify_${this.channel}`);
    const trigger = sql.identifier(`trg_notify_${this.channel}`);
    await this.db.execute(sql`DROP TRIGGER IF EXISTS ${trigger} ON ${sql.identifier(this.table)}`);
    await this.db.execute(sql`DROP FUNCTION IF EXISTS ${fn}()`);
  }
}

interface PollCursor {
  /** Timestamp of the last row read, as Postgres text (full precision). */
  readonly ts: string;
  /** Sequence of the last row read; null before the first row. */
  readonly seq: unknown;
}

/**
 * Remembers the last `size` keys. add() returns true for a key it hasn't
 * seen (or has forgotten), false for a duplicate.
 */
class RecentKeys {
  private readonly seen = new Set<string>();

  constructor(private readonly size: number) {}

  add(key: string): boolean {
    if (this.seen.has(key)) return false;
    this.seen.add(key);
    // A Set remembers insertion order, so the first key is the oldest.
    if (this.seen.size > this.size) {
      const oldest = this.seen.values().next().value as string;
      this.seen.delete(oldest);
    }
    return true;
  }
}
