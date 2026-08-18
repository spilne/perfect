export interface RedisClient {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ...args: Array<string | number>): Promise<unknown>;
  del(...keys: string[]): Promise<number>;
  exists(key: string): Promise<number | boolean>;
  keys(pattern: string): Promise<string[]>;
  scan(cursor: string | number, ...args: Array<string | number>): Promise<[string, string[]]>;

  lpush(key: string, ...values: string[]): Promise<number>;
  rpush(key: string, ...values: string[]): Promise<number>;
  brpop(key: string, timeoutSeconds: number): Promise<[string, string] | null>;
  llen(key: string): Promise<number>;
  lrange(key: string, start: number, stop: number): Promise<string[]>;
  pexpire(key: string, milliseconds: number): Promise<number>;

  hset(key: string, ...args: Array<string | Record<string, string>>): Promise<number>;
  hget(key: string, field: string): Promise<string | null>;
  hgetall(key: string): Promise<Record<string, string>>;

  xadd(key: string, ...args: Array<string | number>): Promise<string | null>;
  xgroup(...args: Array<string | number>): Promise<unknown>;
  xreadgroup(...args: Array<string | number>): Promise<unknown>;
  xack(key: string, group: string, ...ids: string[]): Promise<number>;
  xpending(key: string, group: string, ...args: Array<string | number>): Promise<unknown>;
  xclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    ...ids: string[]
  ): Promise<unknown>;
  xinfo(...args: Array<string | number>): Promise<unknown>;

  publish(channel: string, message: string): Promise<number>;
  pubsub(...args: string[]): Promise<unknown>;
  subscribe(...channels: string[]): Promise<unknown>;
  unsubscribe(...channels: string[]): Promise<unknown>;
  psubscribe(...patterns: string[]): Promise<unknown>;
  punsubscribe(...patterns: string[]): Promise<unknown>;
  on(event: string, listener: (...args: any[]) => void): unknown;
  off?(event: string, listener: (...args: any[]) => void): unknown;
  removeListener?(event: string, listener: (...args: any[]) => void): unknown;

  eval(script: string, numKeys: number, ...args: Array<string | number>): Promise<unknown>;
  duplicate(): RedisClient | Promise<RedisClient>;
  disconnect?(): void;
  close?(): void;
}

export function closeRedisClient(client: RedisClient): void {
  if (client.disconnect) client.disconnect();
  else client.close?.();
}
