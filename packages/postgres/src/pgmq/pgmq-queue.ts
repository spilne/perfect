// ---------------------------------------------------------------------------
// PgmqQueue<T> — high-level typed queue implementing core connect contracts
// Streamable + Sinkable + Acknowledgeable
//
// Ported from promin (Effect-TS StreamPipeline → perfect Stream): the
// spaced-schedule poll loops became pollStream (sleep only after an empty
// batch); database and schema failures remain in the Stream effect channel.
// ---------------------------------------------------------------------------

import { fromPromise, type Eff, type Throws } from "@spilne/perfect-core";
import { Stream, type SchemaParser } from "@spilne/perfect-core/stream";
import { JsonCodec } from "@spilne/perfect-core/connect";
import type {
  Streamable,
  Sinkable,
  Acknowledgeable,
  Codec,
  ConsumerGroup,
  TransactionalEnvelope,
  TransactionalSinkable,
} from "@spilne/perfect-core/connect";
import type { DrizzleDb } from "../lib/drizzle-db";
import { pollStream } from "../lib/poll-stream";
import { PostgresError, toPostgresError } from "../lib/postgres-error";
import type { ReadMode, AckMode } from "./types";
import * as pgmq from "./pgmq";

// ---------------------------------------------------------------------------
// Schema errors
// ---------------------------------------------------------------------------

/**
 * What to do when a read message fails schema validation.
 *
 * - `"throw"` (default) — let the error propagate; the consumer stream
 *   fails. Safest default for initial rollout: loud failures beat silent
 *   bad data. The poison message is NOT deleted — operator must intervene.
 * - `"skip"` — log (TODO once we have a hook), delete the bad message
 *   from the queue so it doesn't re-appear, and continue. Use when bad
 *   messages are acceptable losses (e.g. observability pipelines).
 * - `"dlq"` — publish the raw message to `{queue}_dlq` for inspection,
 *   then delete the original. The DLQ is auto-created on first use.
 */
export type PgmqOnSchemaError = "throw" | "skip" | "dlq";

/**
 * Thrown from the subscribe stream when `onSchemaError: "throw"` and a
 * message fails validation. Carries the original payload + the schema's
 * error so the operator can reproduce locally.
 */
export class PgmqSchemaValidationError extends Error {
  readonly _tag = "PgmqSchemaValidationError";
  constructor(
    readonly queueName: string,
    readonly msgId: number,
    readonly raw: unknown,
    readonly schemaError: unknown,
  ) {
    super(
      `PgmqQueue "${queueName}": message ${msgId} failed schema validation. ` +
        `Set onSchemaError to "skip" or "dlq" to handle invalid messages without ` +
        `killing the consumer.`,
    );
  }
}

export type PgmqQueueError = PostgresError | PgmqSchemaValidationError;

function toPgmqQueueError(operation: string, cause: unknown): PgmqQueueError {
  return cause instanceof PgmqSchemaValidationError ? cause : toPostgresError(operation, cause);
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface PgmqQueueConfig<T> {
  /** Drizzle database instance. */
  db: DrizzleDb;
  /** Queue name. */
  queue: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Default visibility timeout in seconds. Default: 30. */
  defaultVt?: number;
  /** Default read batch size. Default: 10. */
  defaultQty?: number;
  /** Default poll interval in ms (client-side). Default: 1000. */
  defaultPollIntervalMs?: number;
  /** Default ack mode. Default: "delete". */
  defaultAckMode?: AckMode;
  /** Enable per-group FIFO defaults and create the recommended FIFO index. */
  fifo?: boolean;
  /**
   * Optional runtime schema validator for consumed messages. Zod, Valibot,
   * and ArkType all satisfy `SchemaParser<T>` out of the box. Applied AFTER
   * codec decode; catches silent producer-side drift (right JSON shape
   * after JSON.parse, wrong fields after validation).
   *
   * Skip if `T` is self-describing enough for you (e.g. you own both
   * producer + consumer and trust the types). Required if you're reading
   * messages published by another service.
   */
  schema?: SchemaParser<T>;
  /**
   * How to handle messages that fail `schema.safeParse`. Ignored when
   * `schema` is unset. Default: `"throw"` — loud failures are the right
   * default for schema drift; silent drops hide bugs.
   */
  onSchemaError?: PgmqOnSchemaError;
}

export interface PgmqEnvelope<T> extends TransactionalEnvelope<
  T,
  Throws<PgmqQueueError>,
  DrizzleDb
> {
  extendVisibility(vtSeconds: number): Eff<void, Throws<PgmqQueueError>>;
}

// ---------------------------------------------------------------------------
// PgmqQueue
// ---------------------------------------------------------------------------

export class PgmqQueue<T>
  implements
    Streamable<T, Throws<PgmqQueueError>>,
    Sinkable<T, Throws<PgmqQueueError>>,
    TransactionalSinkable<T, Throws<PgmqQueueError>, DrizzleDb>,
    Acknowledgeable<T, Throws<PgmqQueueError>>
{
  readonly codec: Codec<T>;
  readonly transactionDomain: object;
  private readonly db: DrizzleDb;
  readonly queue: string;
  private readonly defaultVt: number;
  private readonly defaultQty: number;
  private readonly defaultPollIntervalMs: number;
  private readonly defaultAckMode: AckMode;
  private readonly fifo: boolean;
  private readonly schema?: SchemaParser<T>;
  private readonly onSchemaError: PgmqOnSchemaError;
  /** Lazy-init flag — DLQ queue is created the first time a message is routed there. */
  private dlqEnsured = false;

  private constructor(config: PgmqQueueConfig<T>) {
    this.db = config.db;
    this.transactionDomain = config.db;
    this.queue = config.queue;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.defaultVt = config.defaultVt ?? 30;
    this.defaultQty = config.defaultQty ?? 10;
    this.defaultPollIntervalMs = config.defaultPollIntervalMs ?? 1000;
    this.defaultAckMode = config.defaultAckMode ?? "delete";
    this.fifo = config.fifo ?? false;
    this.schema = config.schema;
    this.onSchemaError = config.onSchemaError ?? "throw";
  }

  /** Name of the auto-managed dead-letter queue for this PgmqQueue. */
  get dlqName(): string {
    return `${this.queue}_dlq`;
  }

  /**
   * Decode + validate a message. Returns a result carrying the validated
   * value on success or a tagged failure that the caller dispatches via
   * `onSchemaError`.
   */
  private decodeAndValidate(raw: unknown): { ok: true; value: T } | { ok: false; error: unknown } {
    const decoded = this.codec.decode(raw);
    if (!this.schema) return { ok: true, value: decoded };
    const result = this.schema.safeParse(decoded);
    if (result.success) return { ok: true, value: result.data };
    return { ok: false, error: result.error };
  }

  /**
   * Route a failed message per `onSchemaError`. Throws when the policy is
   * `"throw"`. Performs all side effects (DLQ publish, original delete)
   * before returning.
   */
  private async handleSchemaError(params: {
    msgId: number;
    rawMessage: unknown;
    error: unknown;
    // When true (read/manual-ack path), we still own the lock — the
    // caller needs us to delete the original so it doesn't retry. When
    // false (pop/auto-ack path), the message is already gone.
    deleteOriginal: boolean;
  }): Promise<void> {
    const { msgId, rawMessage, error, deleteOriginal } = params;
    if (this.onSchemaError === "throw") {
      throw new PgmqSchemaValidationError(this.queue, msgId, rawMessage, error);
    }
    if (this.onSchemaError === "dlq") {
      if (!this.dlqEnsured) {
        await pgmq.createQueue(this.db, this.dlqName);
        this.dlqEnsured = true;
      }
      await pgmq.send(this.db, this.dlqName, { data: rawMessage });
    }
    if (deleteOriginal) {
      await pgmq.deleteMessage(this.db, this.queue, msgId);
    }
  }

  /**
   * Create a PgmqQueue and ensure the underlying pgmq queue exists.
   *
   * @example
   * ```ts
   * const jobQueue = await PgmqQueue.create<{ userId: string }>(db, "onboard-jobs");
   * ```
   */
  static async create<T>(
    db: DrizzleDb,
    queue: string,
    config?: Omit<PgmqQueueConfig<T>, "db" | "queue">,
  ): Promise<PgmqQueue<T>> {
    await pgmq.createQueue(db, queue);
    if (config?.fifo) await pgmq.createFifoIndex(db, queue);
    return new PgmqQueue({ db, queue, ...config });
  }

  /**
   * Wrap an existing pgmq queue (assumes it already exists).
   */
  static wrap<T>(config: PgmqQueueConfig<T>): PgmqQueue<T> {
    return new PgmqQueue(config);
  }

  // ---------------------------------------------------------------------------
  // Sinkable<T> — publish messages
  // ---------------------------------------------------------------------------

  publish(
    value: T,
    params?: { delay?: number; headers?: Record<string, string>; group?: string },
  ): Eff<void, Throws<PgmqQueueError>> {
    return fromPromise(
      async () => {
        const headers =
          params?.group === undefined
            ? params?.headers
            : { ...params.headers, "x-pgmq-group": params.group };
        await pgmq.send(this.db, this.queue, {
          data: this.codec.encode(value),
          delay: params?.delay,
          headers,
        });
      },
      (cause) => toPgmqQueueError("pgmq.publish", cause),
    );
  }

  async publishInTransaction(transaction: DrizzleDb, value: T): Promise<void> {
    await pgmq.send(transaction, this.queue, { data: this.codec.encode(value) });
  }

  async publishBatch(
    values: T[],
    params?: { delay?: number; groupBy?: (value: T) => string },
  ): Promise<number[]> {
    return pgmq.sendBatch(
      this.db,
      this.queue,
      values.map((value) => ({
        data: this.codec.encode(value),
        delay: params?.delay,
        headers: params?.groupBy ? { "x-pgmq-group": params.groupBy(value) } : undefined,
      })),
    );
  }

  // ---------------------------------------------------------------------------
  // Streamable<T> — subscribe to messages (auto-ack via pop)
  // ---------------------------------------------------------------------------

  subscribe(_params?: { group?: ConsumerGroup }): Stream<T, Throws<PgmqQueueError>> {
    return pollStream(
      () => pgmq.pop<unknown>(this.db, this.queue, this.defaultQty),
      this.defaultPollIntervalMs,
      "pgmq.pop",
    ).flatMap((record) =>
      // Per-record effect that emits 0 values on schema failure (after
      // routing per onSchemaError) and 1 value on success.
      Stream.fromEffect(
        fromPromise(
          async () => {
            const res = this.decodeAndValidate(record.message);
            if (res.ok) return [res.value];
            // pop() already removed the message — no delete needed.
            await this.handleSchemaError({
              msgId: record.msgId,
              rawMessage: record.message,
              error: res.error,
              deleteOriginal: false,
            });
            return [];
          },
          (cause) => toPgmqQueueError("pgmq.decode", cause),
        ),
      ).flatMap((values) => Stream.fromArray(values)),
    );
  }

  // ---------------------------------------------------------------------------
  // Acknowledgeable<T> — subscribe with manual ack/nack
  // ---------------------------------------------------------------------------

  subscribeAck(params?: {
    group?: ConsumerGroup;
    readMode?: ReadMode;
    pollIntervalMs?: number;
    ackMode?: AckMode;
  }): Stream<PgmqEnvelope<T>, Throws<PgmqQueueError>> {
    const db = this.db;
    const queue = this.queue;
    const pollMs = params?.pollIntervalMs ?? this.defaultPollIntervalMs;
    const ackMode = params?.ackMode ?? this.defaultAckMode;
    const readMode: ReadMode =
      params?.readMode ??
      (this.fifo
        ? { _tag: "grouped-head", vt: this.defaultVt, qty: this.defaultQty }
        : { _tag: "standard", vt: this.defaultVt, qty: this.defaultQty });

    const ack = async (target: DrizzleDb, msgId: number): Promise<void> => {
      if (ackMode === "archive") {
        await pgmq.archive(target, queue, msgId);
      } else {
        await pgmq.deleteMessage(target, queue, msgId);
      }
    };

    return pollStream(() => pgmq.read<unknown>(db, queue, readMode), pollMs, "pgmq.read").flatMap(
      (record) =>
        Stream.fromEffect(
          fromPromise(
            async () => {
              const res = this.decodeAndValidate(record.message);
              if (res.ok) {
                const envelope: PgmqEnvelope<T> = {
                  value: res.value,
                  transactionDomain: this.transactionDomain,
                  ack: () =>
                    fromPromise(
                      () => ack(db, record.msgId),
                      (cause) => toPgmqQueueError("pgmq.ack", cause),
                    ),
                  ackInTransaction: (transaction) => ack(transaction, record.msgId),
                  nack: () =>
                    fromPromise(
                      () => pgmq.setVt(db, queue, record.msgId, 1).then(() => {}),
                      (cause) => toPgmqQueueError("pgmq.nack", cause),
                    ),
                  extendVisibility: (vtSeconds) =>
                    fromPromise(
                      () => pgmq.setVt(db, queue, record.msgId, vtSeconds).then(() => {}),
                      (cause) => toPgmqQueueError("pgmq.extendVisibility", cause),
                    ),
                  metadata: {
                    topic: queue,
                    partition: 0,
                    offset: String(record.msgId),
                    msgId: record.msgId,
                    readCt: record.readCt,
                    enqueuedAt: record.enqueuedAt,
                    headers: record.headers,
                  },
                };
                return [envelope];
              }
              // read() kept the message in the queue (locked via vt) — we
              // delete it ourselves for skip/dlq so it doesn't retry after
              // vt expires. For "throw", leave the lock to expire naturally
              // (the error surfaces again on retry, which is the point).
              await this.handleSchemaError({
                msgId: record.msgId,
                rawMessage: record.message,
                error: res.error,
                deleteOriginal: true,
              });
              return [];
            },
            (cause) => toPgmqQueueError("pgmq.decode", cause),
          ),
        ).flatMap((envelopes) => Stream.fromArray(envelopes)),
    );
  }

  // ---------------------------------------------------------------------------
  // Queue management
  // ---------------------------------------------------------------------------

  async purge(): Promise<number> {
    return pgmq.purgeQueue(this.db, this.queue);
  }

  async drop(): Promise<boolean> {
    return pgmq.dropQueue(this.db, this.queue);
  }

  async metrics(): Promise<{
    queueName: string;
    queueLength: number;
    newestMsgAgeSec: number | null;
    oldestMsgAgeSec: number | null;
    totalMessages: number;
  }> {
    return pgmq.metrics(this.db, this.queue);
  }

  async enableNotify(throttleIntervalMs?: number): Promise<void> {
    await pgmq.enableNotify(this.db, this.queue, throttleIntervalMs);
  }

  async disableNotify(): Promise<void> {
    await pgmq.disableNotify(this.db, this.queue);
  }
}
