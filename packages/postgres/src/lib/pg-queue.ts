// ---------------------------------------------------------------------------
// PgQueue<T> — SKIP LOCKED-based queue (no extension required)
//
// Implements Streamable + Sinkable + Acknowledgeable using plain Postgres:
//   - Enqueue: INSERT into queue table
//   - Dequeue: SELECT ... FOR UPDATE SKIP LOCKED, then mark the rows
//     'processing' and hide them for the visibility timeout (VT)
//   - Ack: DELETE (or UPDATE status = 'completed')
//   - Nack: make the message visible again right away
//   - A consumer that dies without ack or nack: once the VT runs out the
//     message is picked up again
//   - After `maxAttempts` deliveries a message is marked 'dead' instead of
//     being retried forever. `requeueDead()` puts dead messages back.
//
// Every delivery gets a random token (locked_by). ack and nack only touch
// the row if the token still matches, so a consumer whose VT ran out (and
// whose message went to someone else) can't ack or nack the new delivery.
//
// All values are sent as query parameters and the table name is always a
// quoted identifier, so queue names with capitals or dashes work and can't
// inject SQL.
//
// Works with any Postgres 9.5+ — no pgmq extension needed.
// Ported from promin (Effect-TS StreamPipeline → perfect Stream).
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import { fail, fromPromise, succeed, suspend, type Eff, type Throws } from "@spilne/perfect-core";
import { JsonCodec } from "@spilne/perfect-core/connect";
import type {
  Streamable,
  Sinkable,
  Acknowledgeable,
  Envelope,
  Codec,
  ConsumerGroup,
} from "@spilne/perfect-core/connect";
import type { Stream } from "@spilne/perfect-core/stream";
import { type DrizzleDb, execRaw } from "./drizzle-db.js";
import { createQueueTable } from "./pg-queue-schema.js";
import { ensureTable as ensureTableFromSchema } from "./schema-utils.js";
import { pollStream } from "./poll-stream.js";
import { PostgresError, toPostgresError } from "./postgres-error.js";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface PgQueueConfig<T> {
  /** Drizzle database instance. */
  db: DrizzleDb;
  /** Queue name (used as table suffix: pgq_{name}). */
  queue: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Default visibility timeout in seconds. Default: 30. */
  defaultVtSeconds?: number;
  /** Default batch size for reads. Default: 10. */
  defaultBatchSize?: number;
  /** Client-side poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Max delivery attempts before message is dead-lettered. Default: 3. */
  maxAttempts?: number;
  /** Whether to archive (keep) or delete completed messages. Default: "delete". */
  ackMode?: "delete" | "archive";
}

// ---------------------------------------------------------------------------
// PgQueue
// ---------------------------------------------------------------------------

/**
 * A typed message queue backed by plain Postgres tables + `SELECT FOR UPDATE SKIP LOCKED`.
 *
 * No pgmq extension required — works with any Postgres 9.5+.
 * Implements `Streamable<T>`, `Sinkable<T>`, and `Acknowledgeable<T>`.
 *
 * @example
 * ```ts
 * import { PgQueue } from "@spilne/perfect-postgres";
 *
 * const queue = await PgQueue.create<{ userId: string }>(db, "jobs");
 *
 * // Publish
 * await queue.publish({ userId: "u_42" });
 *
 * // Consume with manual ack
 * await run(
 *   queue
 *     .subscribeAck()
 *     .forEach((envelope) =>
 *       fromPromise(async () => {
 *         await processUser(envelope.value);
 *         await envelope.ack();
 *       }, (cause) => cause).orDie(),
 *     )
 *     .orDie(),
 * );
 *
 * // Or auto-consume (pop — read + delete in one step)
 * await run(queue.subscribe().take(10).toArray().orDie());
 * ```
 */
export class PgQueue<T>
  implements
    Streamable<T, Throws<PostgresError>>,
    Sinkable<T, Throws<PostgresError>>,
    Acknowledgeable<T, Throws<PostgresError>>
{
  readonly codec: Codec<T>;
  readonly queue: string;
  private readonly db: DrizzleDb;
  private readonly tableName: string;
  private readonly defaultVtSeconds: number;
  private readonly defaultBatchSize: number;
  private readonly pollIntervalMs: number;
  private readonly ackMode: "delete" | "archive";
  private readonly maxAttempts: number;
  // The quoted table name, ready to put in a query.
  private readonly table: ReturnType<typeof sql.identifier>;

  private constructor(config: PgQueueConfig<T>) {
    this.db = config.db;
    this.queue = config.queue;
    this.tableName = `pgq_${config.queue}`;
    this.table = sql.identifier(this.tableName);
    this.maxAttempts = config.maxAttempts ?? 3;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.defaultVtSeconds = config.defaultVtSeconds ?? 30;
    this.defaultBatchSize = config.defaultBatchSize ?? 10;
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.ackMode = config.ackMode ?? "delete";
  }

  /**
   * Create a PgQueue — creates the underlying table if it doesn't exist.
   */
  static async create<T>(
    db: DrizzleDb,
    queue: string,
    config?: Omit<PgQueueConfig<T>, "db" | "queue">,
  ): Promise<PgQueue<T>> {
    const q = new PgQueue<T>({ db, queue, ...config });
    await q.ensureTable();
    return q;
  }

  /** Wrap an existing queue table (assumes it exists). */
  static wrap<T>(config: PgQueueConfig<T>): PgQueue<T> {
    return new PgQueue(config);
  }

  /**
   * Get the Drizzle schema for a queue table.
   * Use this to include queue tables in your migration pipeline.
   *
   * @example
   * ```ts
   * // In your drizzle schema file:
   * export const ordersQueue = PgQueue.schema("orders");
   *
   * // Then run: bun drizzle-kit generate
   * ```
   */
  static schema(queueName: string) {
    return createQueueTable(queueName);
  }

  // ---------------------------------------------------------------------------
  // Table management
  // ---------------------------------------------------------------------------

  private async ensureTable(): Promise<void> {
    await ensureTableFromSchema(this.db, createQueueTable(this.queue));
  }

  // ---------------------------------------------------------------------------
  // Sinkable<T> — publish
  // ---------------------------------------------------------------------------

  publish(
    value: T,
    params?: { delay?: number; headers?: Record<string, string> },
  ): Eff<void, Throws<PostgresError>> {
    return fromPromise(
      async () => {
        const payload = JSON.stringify(this.codec.encode(value));
        const headers = params?.headers ? JSON.stringify(params.headers) : null;
        const delaySeconds = params?.delay ?? 0;
        await this.db.execute(sql`
          INSERT INTO ${this.table} (payload, headers, visible_at)
          VALUES (${payload}::jsonb, ${headers}::jsonb, NOW() + make_interval(secs => ${delaySeconds}))
        `);
      },
      (cause) => toPostgresError("queue.publish", cause),
    );
  }

  // ---------------------------------------------------------------------------
  // Streamable<T> — subscribe with auto-pop
  // ---------------------------------------------------------------------------

  subscribe(_params?: { group?: ConsumerGroup }): Stream<T, Throws<PostgresError>> {
    return pollStream(
      () => this.pop(this.defaultBatchSize),
      this.pollIntervalMs,
      "queue.pop",
    ).evalMap((row) => this.decode(row.payload));
  }

  // Decoding is synchronous, so we don't need a promise for it. A codec
  // that throws fails the stream with a PostgresError.
  private decode(payload: unknown): Eff<T, Throws<PostgresError>> {
    return suspend(() => {
      try {
        return succeed(this.codec.decode(payload));
      } catch (cause) {
        return fail(toPostgresError("queue.decode", cause));
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Acknowledgeable<T> — subscribe with manual ack/nack
  // ---------------------------------------------------------------------------

  subscribeAck(params?: {
    group?: ConsumerGroup;
    vtSeconds?: number;
  }): Stream<Envelope<T, Throws<PostgresError>>, Throws<PostgresError>> {
    const vt = params?.vtSeconds ?? this.defaultVtSeconds;

    return pollStream(
      () => this.dequeue(this.defaultBatchSize, vt),
      this.pollIntervalMs,
      "queue.dequeue",
    ).evalMap((row) =>
      this.decode(row.payload).map((value): Envelope<T, Throws<PostgresError>> => ({
        value,
        ack: () =>
          fromPromise(
            () => this.ack(row.id, row.lockToken),
            (cause) => toPostgresError("queue.ack", cause),
          ),
        nack: () =>
          fromPromise(
            () => this.nack(row.id, row.lockToken),
            (cause) => toPostgresError("queue.nack", cause),
          ),
        metadata: {
          msgId: row.id,
          attemptCount: row.attemptCount,
          createdAt: row.createdAt,
          headers: row.headers,
        },
      })),
    );
  }

  // ---------------------------------------------------------------------------
  // Core operations
  // ---------------------------------------------------------------------------

  /**
   * Claim up to `limit` messages for `vtSeconds` (SKIP LOCKED, so parallel
   * consumers never get the same message).
   *
   * A message can be claimed when it is visible and has attempts left:
   *   - 'pending': new, or nacked
   *   - 'processing' whose VT ran out: its consumer died or got stuck
   * In the same statement, messages that ran out of attempts are marked
   * 'dead', so they stop coming back.
   */
  private async dequeue(
    limit: number,
    vtSeconds: number,
  ): Promise<
    {
      id: number;
      payload: unknown;
      attemptCount: number;
      createdAt: Date;
      headers: unknown;
      lockToken: string;
    }[]
  > {
    const lockToken = crypto.randomUUID();
    const rows = await execRaw(
      this.db,
      sql`
        WITH out_of_attempts AS (
          UPDATE ${this.table}
          SET status = 'dead', locked_by = NULL
          WHERE status IN ('pending', 'processing')
            AND visible_at <= NOW()
            AND attempt_count >= ${this.maxAttempts}
        ),
        claimable AS (
          SELECT id FROM ${this.table}
          WHERE status IN ('pending', 'processing')
            AND visible_at <= NOW()
            AND attempt_count < ${this.maxAttempts}
          ORDER BY created_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        UPDATE ${this.table} AS q
        SET status = 'processing',
            visible_at = NOW() + make_interval(secs => ${vtSeconds}),
            attempt_count = q.attempt_count + 1,
            locked_by = ${lockToken}
        FROM claimable
        WHERE q.id = claimable.id
        RETURNING q.id, q.payload, q.attempt_count, q.created_at, q.headers
      `,
    );
    return rows.map((r: any) => ({
      id: Number(r.id),
      payload: r.payload,
      attemptCount: r.attempt_count,
      createdAt: r.created_at instanceof Date ? r.created_at : new Date(r.created_at),
      headers: r.headers,
      lockToken,
    }));
  }

  /** Pop (read + delete) — for auto-ack consumers. */
  private async pop(limit: number): Promise<{ id: number; payload: unknown }[]> {
    const rows = await execRaw(
      this.db,
      sql`
        DELETE FROM ${this.table}
        WHERE id IN (
          SELECT id FROM ${this.table}
          WHERE status IN ('pending', 'processing')
            AND visible_at <= NOW()
            AND attempt_count < ${this.maxAttempts}
          ORDER BY created_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, payload
      `,
    );
    return rows.map((r: any) => ({ id: Number(r.id), payload: r.payload }));
  }

  /**
   * Acknowledge — delete or mark completed. Only if this delivery still
   * holds the message (see the note at the top of the file).
   */
  private async ack(msgId: number, lockToken: string): Promise<void> {
    if (this.ackMode === "delete") {
      await this.db.execute(
        sql`DELETE FROM ${this.table} WHERE id = ${msgId} AND locked_by = ${lockToken}`,
      );
    } else {
      await this.db.execute(sql`
        UPDATE ${this.table}
        SET status = 'completed', completed_at = NOW(), locked_by = NULL
        WHERE id = ${msgId} AND locked_by = ${lockToken}
      `);
    }
  }

  /**
   * Nack — make the message visible again right away, or mark it 'dead' if
   * it has used all its attempts. Only if this delivery still holds it.
   */
  private async nack(msgId: number, lockToken: string): Promise<void> {
    await this.db.execute(sql`
      UPDATE ${this.table}
      SET status = CASE WHEN attempt_count >= ${this.maxAttempts} THEN 'dead' ELSE 'pending' END,
          visible_at = NOW(),
          locked_by = NULL
      WHERE id = ${msgId} AND locked_by = ${lockToken}
    `);
  }

  // ---------------------------------------------------------------------------
  // Queue management
  // ---------------------------------------------------------------------------

  /** Get queue metrics. */
  async metrics(): Promise<{
    pending: number;
    processing: number;
    completed: number;
    dead: number;
    total: number;
  }> {
    const [row] = await execRaw(
      this.db,
      sql`
        SELECT
          COUNT(*) FILTER (WHERE status = 'pending') as pending,
          COUNT(*) FILTER (WHERE status = 'processing') as processing,
          COUNT(*) FILTER (WHERE status = 'completed') as completed,
          COUNT(*) FILTER (WHERE status = 'dead') as dead,
          COUNT(*) as total
        FROM ${this.table}
      `,
    );
    return {
      pending: Number(row?.pending ?? 0),
      processing: Number(row?.processing ?? 0),
      completed: Number(row?.completed ?? 0),
      dead: Number(row?.dead ?? 0),
      total: Number(row?.total ?? 0),
    };
  }

  /** Purge all messages from the queue. */
  async purge(): Promise<number> {
    const rows = await execRaw(this.db, sql`DELETE FROM ${this.table} RETURNING id`);
    return rows.length;
  }

  /** Drop the queue table entirely. */
  async drop(): Promise<void> {
    await this.db.execute(sql`DROP TABLE IF EXISTS ${this.table}`);
  }

  /**
   * Put 'dead' messages (the ones that used all their attempts) back in the
   * queue with a fresh attempt count. Returns how many were requeued.
   *
   * Messages whose consumer died are redelivered automatically once their
   * visibility timeout runs out; this is only for dead ones.
   */
  async requeueDead(): Promise<number> {
    const rows = await execRaw(
      this.db,
      sql`
        UPDATE ${this.table}
        SET status = 'pending', visible_at = NOW(), attempt_count = 0, locked_by = NULL
        WHERE status = 'dead'
        RETURNING id
      `,
    );
    return rows.length;
  }
}
