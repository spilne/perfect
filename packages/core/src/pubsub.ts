// PubSub<T> — broadcast channel. Every subscriber sees every published
// message, with per-subscriber backpressure.
//
// Design (matches promin's contract; built atop our Queue+Stream):
//   - Each subscribe() allocates a dedicated Queue
//   - publish(value) writes the value to ALL subscriber queues
//   - When a subscriber Stream ends/cancels, its queue is closed and
//     removed from the broadcast set
//   - shutdown() closes all subscriber queues — pending takers receive
//     the close signal, the streams terminate
//
// Slow-consumer policy (v1): "block" — if any subscriber queue is full,
// publish blocks until that subscriber drains. Future: configurable
// drop-oldest / disconnect modes.
//
// Eff-typed contract; in-process by default. Distributed PubSub
// implementations (Redis Pub/Sub, NATS, etc.) implement the same interface.

import { type Eff, type Throws } from "./eff";
import { succeed, sync, suspend } from "./constructors";
import { type Queue, Queue as QueueNS, QueueClosed } from "./queue";
import { Stream } from "./stream";

export interface PubSub<T, S = never> {
  /** Publish a value to all current subscribers. Blocks if any subscriber queue is full. */
  publish(value: T): Eff<boolean, S>;
  /** Open a new subscription. Returns a Stream that ends when shutdown() is called. */
  readonly subscribe: Eff<Stream<T, S | Throws<QueueClosed>>, S>;
  /** Close all subscriber queues — every subscribe stream terminates. */
  shutdown(): Eff<void, S>;
  /** Number of active subscribers. */
  readonly subscriberCount: Eff<number, S>;
}

class InProcessPubSub<T> implements PubSub<T> {
  private subscribers = new Set<Queue<T>>();
  private _shutdown = false;

  constructor(private readonly capacity: number) {}

  publish(value: T): Eff<boolean, never> {
    return suspend(() => {
      if (this._shutdown || this.subscribers.size === 0) return succeed(false);
      const offers = Array.from(this.subscribers).map((q) =>
        q.offer(value).catch(() => succeed(false)),
      );
      return offers.reduce<Eff<boolean, never>>(
        (acc, offer) => acc.flatMap(() => offer),
        succeed(true),
      );
    });
  }

  get subscribe(): Eff<Stream<T, Throws<QueueClosed>>, never> {
    return QueueNS.bounded<T>(this.capacity).flatMap((q) =>
      sync(() => {
        this.subscribers.add(q);
        // When the subscriber stream terminates (drained, failed, or
        // abandoned early), leave the broadcast set and close the queue so
        // blocked publishers wake up instead of waiting on a dead consumer.
        const cleanup = sync(() => {
          this.subscribers.delete(q);
        }).flatMap(() => q.close());
        return (Stream.fromQueue(q) as Stream<T, Throws<QueueClosed>>).onFinalize(cleanup);
      }),
    );
  }

  shutdown(): Eff<void, never> {
    return sync(() => {
      this._shutdown = true;
      return Array.from(this.subscribers);
    }).flatMap((subs) => {
      const closeAll = subs.reduce<Eff<void, never>>(
        (acc, q) => (acc as any).flatMap(() => q.close()),
        succeed(undefined) as any,
      );
      return (closeAll as any).flatMap(() =>
        sync(() => {
          this.subscribers.clear();
        }),
      );
    }) as Eff<void, never>;
  }

  get subscriberCount(): Eff<number, never> {
    return sync(() => this.subscribers.size);
  }
}

export const PubSub = {
  /** Bounded — each subscriber gets a queue of `capacity`. Publishers block on slow consumers. */
  bounded<T>(capacity: number): Eff<PubSub<T>, never> {
    if (capacity < 1) throw new Error("PubSub.bounded: capacity must be >= 1");
    return sync(() => new InProcessPubSub<T>(capacity));
  },
  /** Unbounded — subscribers buffer everything. Beware slow-consumer memory growth. */
  unbounded<T>(): Eff<PubSub<T>, never> {
    return sync(() => new InProcessPubSub<T>(Infinity));
  },
} as const;
