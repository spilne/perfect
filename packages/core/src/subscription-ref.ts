// SubscriptionRef<A> — a Ref<A> that also exposes a Stream<A> of changes.
//
// Useful for:
//   - Config hot-reload (subscribe to live config changes)
//   - UI state (reactive cells)
//   - Event-sourced state machines
//
// `changes` emits the current value first, then every set/update.
//
// Built atop Ref<A> + PubSub<A>. Eff-typed contract; in-process by default.

import { type Eff, type Throws } from "./eff";
import { type Ref, Ref as RefNS } from "./ref";
import { type PubSub, PubSub as PubSubNS } from "./pubsub";
import { Stream } from "./stream";
import { type QueueClosed } from "./queue";

export interface SubscriptionRef<A> {
  /** Read the current value. */
  readonly get: Eff<A, never>;
  /** Set a new value — notifies all subscribers. */
  set(value: A): Eff<void, never>;
  /** Update with a function — notifies all subscribers. */
  update(f: (a: A) => A): Eff<void, never>;
  /**
   * Stream of changes. Emits the current value as the first element,
   * then every subsequent set/update.
   */
  readonly changes: Eff<Stream<A, Throws<QueueClosed>>, never>;
}

class InProcessSubscriptionRef<A> implements SubscriptionRef<A> {
  constructor(
    private readonly ref: Ref<A>,
    private readonly pubsub: PubSub<A>,
  ) {}

  get get(): Eff<A, never> {
    return this.ref.get;
  }

  set(value: A): Eff<void, never> {
    return (this.ref.set(value) as any).flatMap(() =>
      (this.pubsub.publish(value) as any).map(() => undefined),
    ) as Eff<void, never>;
  }

  update(f: (a: A) => A): Eff<void, never> {
    return (this.ref.updateAndGet(f) as any).flatMap((next: A) =>
      (this.pubsub.publish(next) as any).map(() => undefined),
    ) as Eff<void, never>;
  }

  get changes(): Eff<Stream<A, Throws<QueueClosed>>, never> {
    return (this.ref.get as any).flatMap((current: A) =>
      (this.pubsub.subscribe as any).map(
        (subscribed: Stream<A, Throws<QueueClosed>>) =>
          Stream.of(current).concat(subscribed) as Stream<A, Throws<QueueClosed>>,
      ),
    );
  }
}

export const SubscriptionRef = {
  make<A>(initial: A): Eff<SubscriptionRef<A>, never> {
    return RefNS.make(initial).flatMap((ref) =>
      PubSubNS.unbounded<A>().map(
        (pubsub) => new InProcessSubscriptionRef(ref, pubsub) as SubscriptionRef<A>,
      ),
    );
  },
} as const;
