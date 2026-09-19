// @spilne/perfect-postgres — Postgres coordination backends for @spilne/perfect-core.
//
// Queues (pgmq + plain SKIP LOCKED), LISTEN/NOTIFY change streams, and
// distributed implementations of core's coordination contracts
// (RateLimiter, Throttle, Singleflight, Ref, StateBackend, LeaderElection).
// The pgmq module also ships as the `@spilne/perfect-postgres/pgmq` subpath.

// Database plumbing
export type { DrizzleDb } from "./lib/drizzle-db.js";
export { createPostgresDb, execRaw } from "./lib/drizzle-db.js";
export { ensureTable } from "./lib/schema-utils.js";
export { PostgresError } from "./lib/postgres-error.js";

// Queues
export { PgQueue, type PgQueueConfig } from "./lib/pg-queue.js";
export { createQueueTable, type QueueTable } from "./lib/pg-queue-schema.js";

// Change streams (LISTEN/NOTIFY CDC)
export { PgChangeStream, type PgChangeStreamConfig, offsetToDate } from "./lib/pg-change-stream.js";

// Coordination primitives
export {
  PgRateLimiter,
  type PgRateLimiterConfig,
  slidingWindowDecision,
} from "./lib/pg-rate-limiter.js";
export { PgThrottle, type PgThrottleConfig } from "./lib/pg-throttle.js";
export { PgSingleflight, type PgSingleflightConfig } from "./lib/pg-singleflight.js";
export { PgRef, type PgRefConfig } from "./lib/pg-ref.js";
export {
  PgLeaderElection,
  type PgLeaderElectionConfig,
  hashToInt32,
} from "./lib/pg-leader-election.js";

// Durable state
export { PgStateBackend, type PgStateBackendConfig } from "./lib/pg-state-backend.js";
export {
  PgPartitionedStateBackend,
  type PgPartitionedStateBackendConfig,
} from "./lib/pg-partitioned-state-backend.js";
export { createTopologyStateTable, topologyState } from "./lib/pg-state-schema.js";
