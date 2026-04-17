import { type Eff, Suspend, Op } from "./eff";
import { succeed, sync } from "./constructors";

export class Ref<A> {
  private value: A;

  private constructor(initial: A) {
    this.value = initial;
  }

  static make<A>(initial: A): Eff<Ref<A>, never> {
    return sync(() => new Ref(initial));
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
