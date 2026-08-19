// Outcome tracking for retry logic.
//
// `RetryAttempt<T, E>` is a tagged union that captures:
// - success values (`{ _tag: "success" }`)
// - typed failures (`{ _tag: "error" }`)
// - defects (`{ _tag: "thrown" }`)
//
// This is intentionally transport- and domain-agnostic so it can support
// higher-level retry helpers across packages.

export type RetryAttempt<T, E = unknown> =
  | { readonly _tag: "success"; readonly value: T }
  | { readonly _tag: "error"; readonly error: E }
  | { readonly _tag: "thrown"; readonly error: unknown };

export const RetryAttempt = {
  success: <T>(value: T): RetryAttempt<T, never> => ({ _tag: "success", value }),
  error: <T = never, E = unknown>(error: E): RetryAttempt<T, E> => ({ _tag: "error", error }),
  thrown: <T = never>(error: unknown): RetryAttempt<T, never> => ({ _tag: "thrown", error }),
  isSuccess: <T, E>(r: RetryAttempt<T, E>): r is { _tag: "success"; value: T } => r._tag === "success",
  isError: <T, E>(r: RetryAttempt<T, E>): r is { _tag: "error"; error: E } => r._tag === "error",
  isThrown: <T, E>(r: RetryAttempt<T, E>): r is { _tag: "thrown"; error: unknown } => r._tag === "thrown",
} as const;
