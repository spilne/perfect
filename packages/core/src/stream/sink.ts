import type { Eff } from "../eff";
import { succeed } from "../constructors";
import type { Stream } from "./stream";

export class Sink<A, B, S = never> {
  // Polymorphic in the input's effect union, like Pipe: a sink THREADS the
  // upstream S through its result instead of erasing it.
  constructor(readonly run: <S2>(input: Stream<A, S2>) => Eff<B, S | S2>) {}

  contramap<C>(f: (c: C) => A): Sink<C, B, S> {
    return new Sink((input) => input.map(f).runSink(this));
  }

  map<C>(f: (b: B) => C): Sink<A, C, S> {
    return new Sink((input) => this.run(input).map(f));
  }

  flatMap<C, S2>(f: (b: B) => Eff<C, S2>): Sink<A, C, S | S2> {
    return new Sink((input) => this.run(input).flatMap(f));
  }

  static fold<A, B>(zero: B, f: (acc: B, a: A) => B): Sink<A, B, never> {
    return new Sink((input) => input.fold(zero, f));
  }

  static foldEffect<A, B, S>(zero: B, f: (acc: B, a: A) => Eff<B, S>): Sink<A, B, S> {
    return new Sink((input) => {
      let acc = zero;
      return input
        .forEach((a) =>
          f(acc, a).map((next) => {
            acc = next;
          }),
        )
        .map(() => acc);
    });
  }

  static collectN<A>(n: number): Sink<A, A[], never> {
    return new Sink((input) => input.take(n).toArray());
  }

  static collectAll<A>(): Sink<A, A[], never> {
    return new Sink((input) => input.toArray());
  }

  static drain<A>(): Sink<A, void, never> {
    return new Sink((input) => input.drain());
  }

  static forEach<A, S>(f: (a: A) => Eff<void, S>): Sink<A, void, S> {
    return new Sink((input) => input.forEach(f));
  }

  static forEachWhile<A, S>(f: (a: A) => Eff<boolean, S>): Sink<A, void, S> {
    return new Sink((input) => {
      let stopped = false;
      return input.forEach((a) => {
        if (stopped) return succeed(undefined);
        return f(a).map((keepGoing) => {
          stopped = !keepGoing;
        });
      });
    });
  }

  static drainWith<A, B, S>(eff: Eff<B, S>): Sink<A, B, S> {
    return new Sink((input) => input.drain().flatMap(() => eff));
  }

  static fromEffect<A, B, S>(eff: Eff<B, S>): Sink<A, B, S> {
    return new Sink(() => eff);
  }

  static head<A>(): Sink<A, A | undefined, never> {
    return new Sink((input) => input.head());
  }

  static last<A>(): Sink<A, A | undefined, never> {
    return new Sink((input) => input.last());
  }

  static count<A>(): Sink<A, number, never> {
    return new Sink((input) => input.count());
  }
}

export const fold = Sink.fold;
export const foldEffect = Sink.foldEffect;
export const collectAll = Sink.collectAll;
export const collectN = Sink.collectN;
export const drain = Sink.drain;
export const forEach = Sink.forEach;
export const forEachWhile = Sink.forEachWhile;
export const drainWith = Sink.drainWith;
export const fromEffect = Sink.fromEffect;
export const head = Sink.head;
export const last = Sink.last;
export const count = Sink.count;
