import { type Eff, Suspend, Op } from "./eff"
import { service, type ServiceTag } from "./service"

// ── Random service ─────────────────────────────────────────────────

export interface Random {
  /** Float in [0, 1). */
  readonly next: () => Eff<number, never>
  /** Integer in [0, max). */
  readonly nextInt: (max: number) => Eff<number, never>
  /** Integer in [min, max). */
  readonly nextRange: (min: number, max: number) => Eff<number, never>
  readonly nextBool: () => Eff<boolean, never>
  readonly nextBytes: (n: number) => Eff<Uint8Array, never>
  /** Returns a NEW shuffled array; does not mutate the input. */
  readonly shuffle: <A>(arr: ReadonlyArray<A>) => Eff<A[], never>
  /** Pick one item uniformly at random; throws (sync error) on empty arrays. */
  readonly choice: <A>(arr: ReadonlyArray<A>) => Eff<A, never>
}

export const Random: ServiceTag<Random> = service<Random>("Random")

// Helper for building Random impls — eff-wraps a sync function so users
// don't have to write the Suspend(Op.Sync, ...) themselves.
function effSync<A>(f: () => A): Eff<A, never> {
  return new Suspend(Op.Sync, f, null) as any
}

// ── Real random: Math.random based ─────────────────────────────────

export class RealRandom implements Random {
  next(): Eff<number, never> {
    return effSync(() => Math.random())
  }
  nextInt(max: number): Eff<number, never> {
    return effSync(() => Math.floor(Math.random() * max))
  }
  nextRange(min: number, max: number): Eff<number, never> {
    return effSync(() => min + Math.floor(Math.random() * (max - min)))
  }
  nextBool(): Eff<boolean, never> {
    return effSync(() => Math.random() < 0.5)
  }
  nextBytes(n: number): Eff<Uint8Array, never> {
    return effSync(() => {
      const bytes = new Uint8Array(n)
      for (let i = 0; i < n; i++) bytes[i] = Math.floor(Math.random() * 256)
      return bytes
    })
  }
  shuffle<A>(arr: ReadonlyArray<A>): Eff<A[], never> {
    return effSync(() => {
      const out = arr.slice()
      for (let i = out.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1))
        ;[out[i], out[j]] = [out[j]!, out[i]!]
      }
      return out
    })
  }
  choice<A>(arr: ReadonlyArray<A>): Eff<A, never> {
    return effSync(() => {
      if (arr.length === 0) throw new Error("Random.choice: empty array")
      return arr[Math.floor(Math.random() * arr.length)]!
    })
  }
}

export const realRandom: Random = new RealRandom()

// ── Test random: seeded, deterministic ─────────────────────────────

/**
 * Deterministic Random implementation backed by an xorshift32 PRNG.
 * Construct with a seed for reproducibility; default seed is 1.
 *
 * Tests can also feed an explicit queue of next floats via setNextValues —
 * useful for exact assertions on randomized branches:
 *
 *   const r = new TestRandom()
 *   r.setNextValues([0.1, 0.9])  // first two next() calls return these
 *   ...
 */
export class TestRandom implements Random {
  private state: number
  private queue: number[] = []

  constructor(seed = 1) {
    // xorshift32 needs a non-zero state
    this.state = (seed | 0) || 1
  }

  /** Override the next N float values. Consumed before the PRNG is touched. */
  setNextValues(values: number[]): void {
    for (const v of values) {
      if (v < 0 || v >= 1) throw new Error(`TestRandom value must be in [0, 1), got ${v}`)
    }
    this.queue.push(...values)
  }

  /** Reset the PRNG to a specific seed. Clears any queued values. */
  reseed(seed: number): void {
    this.state = (seed | 0) || 1
    this.queue.length = 0
  }

  /** Number of queued forced values still pending. */
  get queuedCount(): number {
    return this.queue.length
  }

  // xorshift32 — fast, 32-bit period. Good enough for tests.
  private rawFloat(): number {
    if (this.queue.length > 0) return this.queue.shift()!
    let x = this.state | 0
    x ^= x << 13
    x ^= x >>> 17
    x ^= x << 5
    this.state = x | 0
    // map signed int32 to [0, 1)
    return ((x >>> 0) % 0xffffffff) / 0xffffffff
  }

  next(): Eff<number, never> {
    return effSync(() => this.rawFloat())
  }
  nextInt(max: number): Eff<number, never> {
    return effSync(() => Math.floor(this.rawFloat() * max))
  }
  nextRange(min: number, max: number): Eff<number, never> {
    return effSync(() => min + Math.floor(this.rawFloat() * (max - min)))
  }
  nextBool(): Eff<boolean, never> {
    return effSync(() => this.rawFloat() < 0.5)
  }
  nextBytes(n: number): Eff<Uint8Array, never> {
    return effSync(() => {
      const bytes = new Uint8Array(n)
      for (let i = 0; i < n; i++) bytes[i] = Math.floor(this.rawFloat() * 256)
      return bytes
    })
  }
  shuffle<A>(arr: ReadonlyArray<A>): Eff<A[], never> {
    return effSync(() => {
      const out = arr.slice()
      for (let i = out.length - 1; i > 0; i--) {
        const j = Math.floor(this.rawFloat() * (i + 1))
        ;[out[i], out[j]] = [out[j]!, out[i]!]
      }
      return out
    })
  }
  choice<A>(arr: ReadonlyArray<A>): Eff<A, never> {
    return effSync(() => {
      if (arr.length === 0) throw new Error("Random.choice: empty array")
      return arr[Math.floor(this.rawFloat() * arr.length)]!
    })
  }
}
