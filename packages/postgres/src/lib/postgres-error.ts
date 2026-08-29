import { TaggedError } from "@spilne/perfect-core";

export class PostgresError extends TaggedError("PostgresError")<{
  readonly operation: string;
  readonly cause: unknown;
}>() {}

export function toPostgresError(operation: string, cause: unknown): PostgresError {
  return cause instanceof PostgresError ? cause : new PostgresError({ operation, cause });
}
