import {
  JsonCodec,
  LeaseEpoch,
  StateCheckpointId,
  TopologyInstanceId,
  type Codec,
  type PartitionCommitResult,
  type PartitionStateCommit,
  type PartitionStateSnapshot,
  type PartitionedStateBackend,
  type StatePartitionLease,
  type StatePartitionScope,
  type SourceRecordId,
} from "@perfect/core/connect";
import { decode, encode, redisKeyFamily } from "./internal";
import type { RedisClient } from "./redis-client";

export interface RedisPartitionedStateBackendConfig<V = unknown> {
  redis: RedisClient;
  key?: string;
  codec?: Codec<V>;
  processedRetentionMs?: number;
}

export class RedisPartitionedStateBackend<V = unknown> implements PartitionedStateBackend<V> {
  private readonly redis: RedisClient;
  private readonly key: string;
  private readonly codec: Codec<V>;
  private readonly processedRetentionMs?: number;

  constructor(config: RedisPartitionedStateBackendConfig<V>) {
    this.redis = config.redis;
    this.key = redisKeyFamily(config.key ?? "perfect-partition-state");
    this.codec = config.codec ?? (JsonCodec as Codec<V>);
    this.processedRetentionMs = config.processedRetentionMs;
    validateRetention(config.processedRetentionMs);
  }

  async acquire(params: {
    scope: StatePartitionScope;
    ownerId: TopologyInstanceId;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    validateLeaseMs(params.leaseMs);
    const result = resultArray(
      await this.redis.eval(
        ACQUIRE_SCRIPT,
        1,
        this.metaKey(params.scope),
        params.ownerId,
        params.leaseMs,
      ),
    );
    return result.length === 0
      ? undefined
      : {
          scope: params.scope,
          ownerId: TopologyInstanceId(String(result[0])),
          epoch: LeaseEpoch(Number(result[1])),
          expiresAt: Number(result[2]),
        };
  }

  async renew(params: {
    lease: StatePartitionLease;
    leaseMs: number;
  }): Promise<StatePartitionLease | undefined> {
    validateLeaseMs(params.leaseMs);
    const result = resultArray(
      await this.redis.eval(
        RENEW_SCRIPT,
        1,
        this.metaKey(params.lease.scope),
        params.lease.ownerId,
        params.lease.epoch,
        params.leaseMs,
      ),
    );
    return result.length === 0
      ? undefined
      : {
          scope: params.lease.scope,
          ownerId: TopologyInstanceId(String(result[0])),
          epoch: LeaseEpoch(Number(result[1])),
          expiresAt: Number(result[2]),
        };
  }

  async load(lease: StatePartitionLease): Promise<PartitionStateSnapshot<V> | undefined> {
    const result = resultArray(
      await this.redis.eval(
        LOAD_SCRIPT,
        2,
        this.metaKey(lease.scope),
        this.stateKey(lease.scope),
        lease.ownerId,
        lease.epoch,
      ),
    );
    if (result.length === 0) return undefined;
    const values = new Map<string, V>();
    for (let index = 2; index < result.length; index += 2) {
      values.set(String(result[index]), decode(this.codec, String(result[index + 1])));
    }
    const sourceOffset = String(result[0]);
    const checkpointId = String(result[1]);
    return {
      values,
      ...(sourceOffset === "" ? {} : { sourceOffset }),
      ...(checkpointId === "" ? {} : { checkpointId: StateCheckpointId(checkpointId) }),
    };
  }

  async isProcessed(params: {
    lease: StatePartitionLease;
    sourceId: SourceRecordId;
  }): Promise<boolean> {
    return (
      Number(
        await this.redis.eval(
          IS_PROCESSED_SCRIPT,
          2,
          this.metaKey(params.lease.scope),
          this.processedKey(params.lease.scope),
          params.lease.ownerId,
          params.lease.epoch,
          params.sourceId,
        ),
      ) === 1
    );
  }

  async commit(commit: PartitionStateCommit<V>): Promise<PartitionCommitResult> {
    const args: Array<string | number> = [
      commit.lease.ownerId,
      commit.lease.epoch,
      commit.sourceId ?? "",
      commit.sourceOffset ?? "",
      commit.checkpointId ?? "",
      this.processedRetentionMs ?? "",
      commit.mutations.length,
    ];
    for (const mutation of commit.mutations) {
      if (mutation.type === "put")
        args.push("put", mutation.key, encode(this.codec, mutation.value));
      else args.push("delete", mutation.key, "");
    }
    const result = String(
      await this.redis.eval(
        COMMIT_SCRIPT,
        3,
        this.metaKey(commit.lease.scope),
        this.stateKey(commit.lease.scope),
        this.processedKey(commit.lease.scope),
        ...args,
      ),
    );
    if (result === "committed" || result === "duplicate" || result === "fenced") return result;
    throw new TypeError(`Unexpected Redis partition commit result: ${result}`);
  }

  async release(lease: StatePartitionLease): Promise<boolean> {
    return (
      Number(
        await this.redis.eval(
          RELEASE_SCRIPT,
          1,
          this.metaKey(lease.scope),
          lease.ownerId,
          lease.epoch,
        ),
      ) === 1
    );
  }

  private metaKey(scope: StatePartitionScope): string {
    return `${this.scopeKey(scope)}:meta`;
  }

  private stateKey(scope: StatePartitionScope): string {
    return `${this.scopeKey(scope)}:state`;
  }

  private processedKey(scope: StatePartitionScope): string {
    return `${this.scopeKey(scope)}:processed`;
  }

  private scopeKey(scope: StatePartitionScope): string {
    return `${this.key}:topology:${encodeURIComponent(scope.topologyId)}:stage:${encodeURIComponent(scope.stageId)}:partition:${scope.partition}`;
  }
}

function resultArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function validateLeaseMs(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1) {
    throw new RangeError(`leaseMs must be a positive safe integer, got ${leaseMs}`);
  }
}

function validateRetention(retentionMs: number | undefined): void {
  if (retentionMs !== undefined && (!Number.isSafeInteger(retentionMs) || retentionMs < 1)) {
    throw new RangeError(
      `processedRetentionMs must be a positive safe integer, got ${retentionMs}`,
    );
  }
}

const ACQUIRE_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local owner = redis.call('HGET', KEYS[1], 'owner')
local epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch') or '0')
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires') or '0')
if owner == ARGV[1] and expires > now then
  expires = now + tonumber(ARGV[2])
elseif owner and expires > now then
  return {}
else
  owner = ARGV[1]
  epoch = epoch + 1
  expires = now + tonumber(ARGV[2])
end
redis.call('HSET', KEYS[1], 'owner', owner, 'epoch', epoch, 'expires', expires)
return { owner, tostring(epoch), tostring(expires) }
`;

const RENEW_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local owner = redis.call('HGET', KEYS[1], 'owner')
local epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch') or '-1')
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires') or '0')
if owner ~= ARGV[1] or epoch ~= tonumber(ARGV[2]) or expires <= now then return {} end
expires = now + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'expires', expires)
return { owner, tostring(epoch), tostring(expires) }
`;

const LOAD_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1]
  or tonumber(redis.call('HGET', KEYS[1], 'epoch') or '-1') ~= tonumber(ARGV[2])
  or tonumber(redis.call('HGET', KEYS[1], 'expires') or '0') <= now then
  return {}
end
local result = {
  redis.call('HGET', KEYS[1], 'offset') or '',
  redis.call('HGET', KEYS[1], 'checkpoint') or ''
}
local state = redis.call('HGETALL', KEYS[2])
for index = 1, #state do table.insert(result, state[index]) end
return result
`;

const IS_PROCESSED_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1]
  or tonumber(redis.call('HGET', KEYS[1], 'epoch') or '-1') ~= tonumber(ARGV[2])
  or tonumber(redis.call('HGET', KEYS[1], 'expires') or '0') <= now then
  return 0
end
return redis.call('ZSCORE', KEYS[2], ARGV[3]) and 1 or 0
`;

const COMMIT_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1]
  or tonumber(redis.call('HGET', KEYS[1], 'epoch') or '-1') ~= tonumber(ARGV[2])
  or tonumber(redis.call('HGET', KEYS[1], 'expires') or '0') <= now then
  return 'fenced'
end
if ARGV[3] ~= '' and redis.call('ZSCORE', KEYS[3], ARGV[3]) then return 'duplicate' end
local count = tonumber(ARGV[7])
local cursor = 8
for index = 1, count do
  if ARGV[cursor] == 'put' then
    redis.call('HSET', KEYS[2], ARGV[cursor + 1], ARGV[cursor + 2])
  else
    redis.call('HDEL', KEYS[2], ARGV[cursor + 1])
  end
  cursor = cursor + 3
end
if ARGV[3] ~= '' then redis.call('ZADD', KEYS[3], now, ARGV[3]) end
if ARGV[4] ~= '' then redis.call('HSET', KEYS[1], 'offset', ARGV[4]) end
if ARGV[5] ~= '' then redis.call('HSET', KEYS[1], 'checkpoint', ARGV[5]) end
if ARGV[6] ~= '' then redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now - tonumber(ARGV[6])) end
return 'committed'
`;

const RELEASE_SCRIPT = `
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1]
  or tonumber(redis.call('HGET', KEYS[1], 'epoch') or '-1') ~= tonumber(ARGV[2]) then
  return 0
end
redis.call('HDEL', KEYS[1], 'owner')
redis.call('HSET', KEYS[1], 'expires', 0)
return 1
`;
