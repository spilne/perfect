import { TaggedError } from "@spilne/perfect-core";

export class RedisError extends TaggedError("RedisError")<{
  readonly operation: string;
  readonly cause: unknown;
}>() {}

export function toRedisError(operation: string, cause: unknown): RedisError {
  return cause instanceof RedisError ? cause : new RedisError({ operation, cause });
}
