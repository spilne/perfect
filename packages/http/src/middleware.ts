// Middleware hooks for observability — tracing, metrics, logging.
//
// Each hook is a PLAIN SYNC FUNCTION returning void. Never an Eff.
// Reasons: (1) instrumentation should never block the async path;
// (2) exceptions inside hooks become unhandled defects (let them crash —
// defensive wrappers should live inside the user's hook if needed).

import type { HttpClientError } from "./errors";

/** Lightweight context passed to every middleware callback. */
export interface HttpRequestContext {
  readonly method: string;
  readonly url: string;
  /** Optional label for metrics/logs — avoids high-cardinality raw URLs. */
  readonly tag?: string;
}

/** Sync observability hooks. Exceptions thrown here crash as defects. */
export interface HttpMiddleware {
  /** Fires just before the request is sent. */
  onRequest?: (context: HttpRequestContext) => void;
  /** Fires on successful response (after parsing + validation). */
  onResponse?: (context: HttpRequestContext & { durationMs: number }) => void;
  /** Fires when the request fails at any stage. */
  onError?: (
    context: HttpRequestContext & { durationMs: number },
    error: HttpClientError,
  ) => void;
}
