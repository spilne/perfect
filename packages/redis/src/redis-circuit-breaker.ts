import { fail, succeed } from "@perfect/core";
import type {
  CircuitBreaker,
  CircuitBreakerOptions,
  CircuitOpen,
  CircuitState,
  Eff,
  Throws,
} from "@perfect/core";
import { redisEff } from "./internal";
import type { RedisClient } from "./redis-client";
import { RedisError } from "./redis-error";

const INSPECT_SCRIPT = `
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local failures = tonumber(redis.call('HGET', KEYS[1], 'failures') or '0')
local openedAt = tonumber(redis.call('HGET', KEYS[1], 'openedAt') or '0')
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local timeout = tonumber(ARGV[1])
local claim = tonumber(ARGV[2])

if state == 'open' and now - openedAt >= timeout then
  state = 'half-open'
  version = version + 1
  redis.call('HSET', KEYS[1], 'state', state, 'version', version, 'probe', 0)
end

local allowed = 1
if state == 'open' then
  allowed = 0
elseif state == 'half-open' and claim == 1 then
  local probe = tonumber(redis.call('HGET', KEYS[1], 'probe') or '0')
  if probe == 1 then
    allowed = 0
  else
    redis.call('HSET', KEYS[1], 'probe', 1)
  end
end

return { allowed, state, failures, openedAt, now, version }
`;

const SUCCESS_SCRIPT = `
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
if version ~= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'state', 'closed', 'failures', 0, 'openedAt', 0, 'probe', 0, 'version', version + 1)
return 1
`;

const FAILURE_SCRIPT = `
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
if version ~= tonumber(ARGV[1]) then return 0 end
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local failures = tonumber(redis.call('HGET', KEYS[1], 'failures') or '0')
local threshold = tonumber(ARGV[2])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

if state == 'half-open' then
  redis.call('HSET', KEYS[1], 'state', 'open', 'openedAt', now, 'probe', 0, 'version', version + 1)
  return 1
end

failures = failures + 1
if failures >= threshold then
  redis.call('HSET', KEYS[1], 'state', 'open', 'failures', failures, 'openedAt', now, 'probe', 0, 'version', version + 1)
else
  redis.call('HSET', KEYS[1], 'failures', failures)
end
return 1
`;

const RELEASE_PROBE_SCRIPT = `
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
if version == tonumber(ARGV[1]) and state == 'half-open' then
  redis.call('HSET', KEYS[1], 'probe', 0)
end
return 1
`;

const RESET_SCRIPT = `
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
redis.call('HSET', KEYS[1], 'state', 'closed', 'failures', 0, 'openedAt', 0, 'probe', 0, 'version', version + 1)
return 1
`;

interface Inspection {
  readonly allowed: boolean;
  readonly state: CircuitState;
  readonly failures: number;
  readonly openedAt: number;
  readonly now: number;
  readonly version: number;
}

type Outcome<A, E> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly error: E };

export interface RedisCircuitBreakerConfig<E> extends CircuitBreakerOptions<E> {
  redis: RedisClient;
  key: string;
}

export class RedisCircuitBreaker<E = unknown> implements CircuitBreaker<E, Throws<RedisError>> {
  private constructor(
    private readonly redis: RedisClient,
    private readonly key: string,
    private readonly options: CircuitBreakerOptions<E>,
  ) {}

  static make<E = unknown>(config: RedisCircuitBreakerConfig<E>): RedisCircuitBreaker<E> {
    if (!Number.isInteger(config.failureThreshold) || config.failureThreshold < 1) {
      throw new Error("RedisCircuitBreaker.make: failureThreshold must be a positive integer");
    }
    if (config.resetTimeoutMs < 0) {
      throw new Error("RedisCircuitBreaker.make: resetTimeoutMs must be non-negative");
    }
    return new RedisCircuitBreaker(config.redis, config.key, config);
  }

  private inspect(claim: boolean, operation: string): Eff<Inspection, Throws<RedisError>> {
    return redisEff(operation, async () => {
      const result = await this.redis.eval(
        INSPECT_SCRIPT,
        1,
        this.key,
        this.options.resetTimeoutMs,
        claim ? 1 : 0,
      );
      if (!Array.isArray(result) || result.length < 6) {
        throw new TypeError("Redis circuit breaker returned an invalid result");
      }
      const state = String(result[1]);
      if (state !== "closed" && state !== "open" && state !== "half-open") {
        throw new TypeError(`Redis circuit breaker returned invalid state: ${state}`);
      }
      return {
        allowed: Number(result[0]) === 1,
        state,
        failures: Number(result[2]),
        openedAt: Number(result[3]),
        now: Number(result[4]),
        version: Number(result[5]),
      };
    });
  }

  get state(): Eff<CircuitState, Throws<RedisError>> {
    return this.inspect(false, "circuitBreaker.state").map((inspection) => inspection.state);
  }

  get failures(): Eff<number, Throws<RedisError>> {
    return this.inspect(false, "circuitBreaker.failures").map((inspection) => inspection.failures);
  }

  protect<A, S>(
    eff: Eff<A, S | Throws<E>>,
  ): Eff<A, S | Throws<RedisError> | Throws<E | CircuitOpen>> {
    return this.inspect(true, "circuitBreaker.protect").flatMap(
      (inspection): Eff<A, S | Throws<RedisError> | Throws<E | CircuitOpen>> => {
        if (!inspection.allowed) {
          const openedAt = inspection.openedAt || inspection.now;
          return fail<CircuitOpen>({
            _tag: "CircuitOpen",
            openedAt,
            resetAtMs: openedAt + this.options.resetTimeoutMs,
          });
        }

        const outcome = (eff as any)
          .map((value: A): Outcome<A, E> => ({ ok: true, value }))
          .catch((error: E) => succeed({ ok: false, error } as Outcome<A, E>)) as Eff<
          Outcome<A, E>,
          S
        >;

        return outcome.flatMap((result) => {
          if (result.ok) {
            return redisEff("circuitBreaker.success", async () => {
              await this.redis.eval(SUCCESS_SCRIPT, 1, this.key, inspection.version);
              return result.value;
            });
          }
          if (this.options.isFailure && !this.options.isFailure(result.error)) {
            return redisEff("circuitBreaker.releaseProbe", async () => {
              await this.redis.eval(RELEASE_PROBE_SCRIPT, 1, this.key, inspection.version);
            }).flatMap(() => fail(result.error));
          }
          return redisEff("circuitBreaker.failure", async () => {
            await this.redis.eval(
              FAILURE_SCRIPT,
              1,
              this.key,
              inspection.version,
              this.options.failureThreshold,
            );
          }).flatMap(() => fail(result.error));
        });
      },
    ) as Eff<A, S | Throws<RedisError> | Throws<E | CircuitOpen>>;
  }

  reset(): Eff<void, Throws<RedisError>> {
    return redisEff("circuitBreaker.reset", async () => {
      await this.redis.eval(RESET_SCRIPT, 1, this.key);
    });
  }
}
