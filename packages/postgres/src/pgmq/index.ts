// Drizzle DB type (re-export for convenience)
export type { DrizzleDb } from "../lib/drizzle-db.js";
export { PostgresError } from "../lib/postgres-error.js";

// Types
export {
  type PgmqMessage,
  fifoMessage,
  type PgmqRecord,
  type ReadMode,
  ReadMode as ReadModes,
  type AckMode,
} from "./types.js";

// High-level typed queue (Streamable + Sinkable + Acknowledgeable)
export {
  PgmqQueue,
  type PgmqQueueConfig,
  type PgmqQueueError,
  type PgmqEnvelope,
  type PgmqOnSchemaError,
  PgmqSchemaValidationError,
} from "./pgmq-queue.js";

// Low-level SQL functions
export {
  createQueue,
  createUnloggedQueue,
  createPartitionedQueue,
  dropQueue,
  listQueues,
  send,
  sendBatch,
  read,
  pop,
  deleteMessage,
  deleteBatch,
  archive,
  archiveBatch,
  purgeQueue,
  setVt,
  metrics,
  enableNotify,
  disableNotify,
  createFifoIndex,
} from "./pgmq.js";
