// Ref<A> — atomic mutable reference, effect-typed.
//
// The `Ref<A>` interface is the contract; this module ships an in-process
// implementation. Distributed implementations (Redis, etcd, etc.) live in
// downstream packages — they implement the same interface.

import { type Eff } from "./eff";
import { sync } from "./constructors";

export interface Ref<A> {
  /** Read the current value. */
  readonly get: Eff<A, never>;
  /** Replace the value. */
  set(value: A): Eff<void, never>;
  /** Apply a transformation to the current value. */
  update(f: (a: A) => A): Eff<void, never>;
  /** Atomic compute-and-return — returns the second tuple element, sets the first. */
  modify<B>(f: (a: A) => [B, A]): Eff<B, never>;
  /** Replace and return the previous value. */
  getAndSet(value: A): Eff<A, never>;
  /** Update and return the previous value. */
  getAndUpdate(f: (a: A) => A): Eff<A, never>;
  /** Update and return the new value. */
  updateAndGet(f: (a: A) => A): Eff<A, never>;
}

class InProcessRef<A> implements Ref<A> {
  private value: A;

  constructor(initial: A) {
    this.value = initial;
  }

  get get(): Eff<A, never> {
    return sync(() => this.value);
  }

  set(value: A): Eff<void, never> {
    return sync(() => {
      this.value = value;
    });
  }

  update(f: (a: A) => A): Eff<void, never> {
    return sync(() => {
      this.value = f(this.value);
    });
  }

  modify<B>(f: (a: A) => [B, A]): Eff<B, never> {
    return sync(() => {
      const [b, a] = f(this.value);
      this.value = a;
      return b;
    });
  }

  getAndSet(value: A): Eff<A, never> {
    return sync(() => {
      const old = this.value;
      this.value = value;
      return old;
    });
  }

  getAndUpdate(f: (a: A) => A): Eff<A, never> {
    return sync(() => {
      const old = this.value;
      this.value = f(old);
      return old;
    });
  }

  updateAndGet(f: (a: A) => A): Eff<A, never> {
    return sync(() => {
      this.value = f(this.value);
      return this.value;
    });
  }
}

export const Ref = {
  make<A>(initial: A): Eff<Ref<A>, never> {
    return sync(() => new InProcessRef(initial));
  },
} as const;
