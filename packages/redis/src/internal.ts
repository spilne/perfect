import { async, fail, fromPromise, succeed } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { Eff, Throws } from "@spilne/perfect-core";
import { closeRedisClient, type RedisClient } from "./redis-client.js";
import { RedisError, toRedisError } from "./redis-error.js";

export function redisEff<A>(
  operation: string,
  thunk: () => Promise<A>,
): Eff<A, Throws<RedisError>> {
  return fromPromise(thunk, (cause) => toRedisError(operation, cause));
}

// ── Connections for blocking commands ──────────────────────────────
//
// A blocking command (BRPOP) ties up its connection until it returns, so it
// can't share the main client. Opening a new connection for every wait was
// expensive: an idle RedisQueue consumer polls every 100 ms and opened and
// closed about ten connections a second.
//
// Instead, each client keeps a few idle blocking connections. A wait
// borrows one and gives it back when the command finishes. A connection
// that sits idle for IDLE_CLOSE_MS is closed, so a program that is done
// with Redis can still exit.

const MAX_IDLE_CONNECTIONS = 4;
const IDLE_CLOSE_MS = 1_000;

interface IdleConnection {
  readonly client: RedisClient;
  readonly closeTimer: ReturnType<typeof setTimeout>;
}

const idleConnections = new WeakMap<RedisClient, IdleConnection[]>();

async function borrowConnection(redis: RedisClient): Promise<RedisClient> {
  const idle = idleConnections.get(redis)?.pop();
  if (idle !== undefined) {
    clearTimeout(idle.closeTimer);
    return idle.client;
  }
  return redis.duplicate();
}

function returnConnection(redis: RedisClient, client: RedisClient): void {
  let idle = idleConnections.get(redis);
  if (idle === undefined) {
    idle = [];
    idleConnections.set(redis, idle);
  }
  if (idle.length >= MAX_IDLE_CONNECTIONS) {
    closeRedisClient(client);
    return;
  }
  const list = idle;
  const entry: IdleConnection = {
    client,
    closeTimer: setTimeout(() => {
      const index = list.indexOf(entry);
      if (index !== -1) list.splice(index, 1);
      closeRedisClient(client);
    }, IDLE_CLOSE_MS),
  };
  list.push(entry);
}

// Runs a blocking command on a connection of its own. `giveBack` puts back
// what the command took (a list item, a wake-up token) when nobody receives
// it: the waiter was interrupted as the command completed, before it ran.
export function redisBlocking<A>(
  redis: RedisClient,
  operation: string,
  run: (client: RedisClient) => Promise<A>,
  options: { readonly giveBack?: (value: A) => void } = {},
): Eff<A, Throws<RedisError>> {
  const { giveBack } = options;
  return async<A, RedisError>((resume) => {
    let client: RedisClient | null = null;
    let canceled = false;

    void borrowConnection(redis).then(
      async (borrowed) => {
        client = borrowed;
        let reusable = false;
        try {
          const value = await run(borrowed);
          // The command finished normally, so the connection is free again.
          reusable = !canceled;
          if (canceled) giveBack?.(value);
          else resume(succeed(value), giveBack && (() => giveBack(value)));
        } catch (cause) {
          if (!canceled) resume(fail(toRedisError(operation, cause)));
        } finally {
          if (reusable) returnConnection(redis, borrowed);
          else closeRedisClient(borrowed);
        }
      },
      (cause) => {
        if (!canceled) resume(fail(toRedisError(operation, cause)));
      },
    );

    // Interrupted while the command is still blocked: the connection is busy
    // with a command nobody waits for, so close it rather than reuse it.
    return () => {
      canceled = true;
      if (client) closeRedisClient(client);
    };
  });
}

export function encode<T>(codec: Codec<T>, value: T): string {
  return JSON.stringify(codec.encode(value));
}

export function decode<T>(codec: Codec<T>, value: string): T {
  return codec.decode(JSON.parse(value));
}

export function numberResult(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number(value);
  throw new TypeError(`Expected numeric Redis result, received ${String(value)}`);
}

export function redisKeyFamily(key: string): string {
  return /\{[^{}]+\}/.test(key) ? key : `{${key}}`;
}
