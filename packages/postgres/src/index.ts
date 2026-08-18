// @perfect/postgres — Postgres coordination backends for @perfect/core.
//
// Queues (pgmq + plain SKIP LOCKED), LISTEN/NOTIFY change streams, and
// distributed implementations of core's coordination contracts
// (RateLimiter, Throttle, Singleflight, Ref, StateBackend, LeaderElection).
// The pgmq module also ships as the `@perfect/postgres/pgmq` subpath.

// Database plumbing
export type { DrizzleDb } from "./lib/drizzle-db";
export { createPostgresDb, execRaw } from "./lib/drizzle-db";
export { ensureTable } from "./lib/schema-utils";
export { PostgresError } from "./lib/postgres-error";

// Queues
export { PgQueue, type PgQueueConfig } from "./lib/pg-queue";
export { createQueueTable, type QueueTable } from "./lib/pg-queue-schema";

// Change streams (LISTEN/NOTIFY CDC)
export { PgChangeStream, type PgChangeStreamConfig, offsetToDate } from "./lib/pg-change-stream";

// Coordination primitives
export {
  PgRateLimiter,
  type PgRateLimiterConfig,
  slidingWindowDecision,
} from "./lib/pg-rate-limiter";
export { PgThrottle, type PgThrottleConfig } from "./lib/pg-throttle";
export { PgSingleflight, type PgSingleflightConfig } from "./lib/pg-singleflight";
export { PgRef, type PgRefConfig } from "./lib/pg-ref";
export {
  PgLeaderElection,
  type PgLeaderElectionConfig,
  hashToInt32,
} from "./lib/pg-leader-election";

// Durable state
export { PgStateBackend, type PgStateBackendConfig } from "./lib/pg-state-backend";
export { createTopologyStateTable, topologyState } from "./lib/pg-state-schema";
