import type { Eff } from "../eff";
import type { Stream } from "./stream";

export class Sink<A, B, S = never> {
  constructor(readonly run: (input: Stream<A, any>) => Eff<B, S>) {}

  static fold<A, B>(zero: B, f: (acc: B, a: A) => B): Sink<A, B, never> {
    return new Sink((input) => input.fold(zero, f));
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
export const collectAll = Sink.collectAll;
export const drain = Sink.drain;
export const forEach = Sink.forEach;
export const head = Sink.head;
export const last = Sink.last;
export const count = Sink.count;
