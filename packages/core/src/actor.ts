// Actors: state that one fiber owns, changed only by messages, one at a
// time, so it needs no locks.
//
//   const counter = yield* Actor.make({
//     initial: 0,
//     receive: (count, message: CounterMessage) =>
//       message.type === "add"
//         ? succeed(count + message.n)
//         : message.reply(count).map(() => count),
//   });
//   yield* counter.tell({ type: "add", n: 2 });
//   const now = yield* counter.ask((reply) => ({ type: "get", reply }));
//
// `tell` drops a message in the mailbox. `ask` sends one that carries a
// `reply` and waits for the answer. If handling a message fails with a typed
// error, the state stays as it was, an `ask` for that message fails with the
// error, and the actor goes on with the next message. A defect (a bug) stops
// the actor. An actor stops when the scope that made it ends.
//
// `Entities` runs one actor per key, made on its first message and stopped
// after it has been idle for a while.

import { type Eff, type ErrorsOf, type Throws } from "./eff.js";
import {
  acquireRelease,
  awaitFiber,
  fail,
  forkDaemon,
  succeed,
  suspend,
  sync,
} from "./constructors.js";
import { Cause } from "./cause.js";
import { Deferred } from "./deferred.js";
import { Queue } from "./queue.js";
import { TaggedError } from "./tagged-error.js";
import { currentClock } from "./clock.js";
import type { Fiber } from "./fiber.js";

/** The actor is stopped: its scope ended, it was stopped, or a defect stopped it. */
export class ActorStopped extends TaggedError("ActorStopped")<{
  readonly message: string;
  readonly cause?: unknown;
}>() {}

/** How an actor starts and what it does with a message. */
export interface Behavior<State, Message, S = never> {
  readonly initial: State;
  /** The state after handling `message`. */
  receive(state: State, message: Message): Eff<State, S>;
}

/** Answer an `ask`. Calling it more than once has no effect. */
export type Reply<R> = (value: R) => Eff<void, never>;

export interface ActorRef<Message, E = never> {
  /** Put a message in the mailbox; waits while a bounded mailbox is full. */
  tell(message: Message): Eff<void, Throws<ActorStopped>>;
  /**
   * Send a message carrying a `reply`, and wait for the answer. Fails with
   * the error the message's handling failed with, if it did.
   */
  ask<R>(message: (reply: Reply<R>) => Message): Eff<R, Throws<E | ActorStopped>>;
  /** Stop after the message being handled; later messages fail with ActorStopped. */
  readonly stop: Eff<void, never>;
}

export interface ActorOptions {
  /** Messages the mailbox holds before `tell` waits. Default: no limit. */
  readonly mailboxSize?: number;
}

/** A message in the mailbox, with what to do if handling it fails. */
interface Letter<Message, E> {
  readonly message: Message;
  readonly failed?: (error: E | ActorStopped) => Eff<unknown, never>;
}

function make<State, Message, S>(
  behavior: Behavior<State, Message, S>,
  options: ActorOptions = {},
): Eff<ActorRef<Message, ErrorsOf<S>>, Exclude<S, Throws<unknown>>> {
  type E = ErrorsOf<S>;
  return (
    options.mailboxSize === undefined
      ? Queue.unbounded<Letter<Message, E>>()
      : Queue.bounded<Letter<Message, E>>(options.mailboxSize)
  ).flatMap((mailbox) => {
    let state = behavior.initial;
    let stopped: ActorStopped | undefined;

    const stop = (reason: ActorStopped): Eff<void, never> =>
      suspend(() => {
        if (stopped) return succeed(undefined);
        stopped = reason;
        // Whoever is still waiting for an answer gets the reason.
        return mailbox
          .takeAll()
          .flatMap((letters) =>
            letters.reduce<Eff<void, never>>(
              (rest, letter) =>
                rest.flatMap(() =>
                  letter.failed ? letter.failed(reason).map(() => undefined) : succeed(undefined),
                ),
              succeed(undefined),
            ),
          )
          .flatMap(() => mailbox.shutdown());
      });

    const handle = (letter: Letter<Message, E>): Eff<void, Exclude<S, Throws<unknown>>> =>
      behavior
        .receive(state, letter.message)
        .exit()
        .flatMap((exit) => {
          if (exit._tag === "Success") {
            state = exit.value;
            return succeed(undefined);
          }
          const failure = Cause.firstFail(exit.cause);
          if (failure !== null) {
            // A typed failure: the state stays, the asker hears about it.
            // `.exit()` drops the error type; a typed failure of `receive` is an `E`.
            const error = failure.value as E;
            return letter.failed ? letter.failed(error).map(() => undefined) : succeed(undefined);
          }
          // A defect or an interrupt stops the actor.
          const reason = new ActorStopped({
            message: `the actor stopped: ${Cause.pretty(exit.cause)}`,
            cause: exit.cause,
          });
          return (letter.failed ? letter.failed(reason) : succeed(undefined)).flatMap(() =>
            stop(reason),
          );
        });

    const loop: Eff<void, Exclude<S, Throws<unknown>>> = suspend(() =>
      stopped
        ? succeed(undefined)
        : mailbox
            .take()
            .catch(() => succeed(undefined))
            .flatMap((letter) =>
              letter === undefined ? succeed(undefined) : handle(letter).flatMap(() => loop),
            ),
    );

    const stoppedError = () => stopped ?? new ActorStopped({ message: "the actor is stopped" });

    const post = (letter: Letter<Message, E>): Eff<void, Throws<ActorStopped>> =>
      suspend(() =>
        stopped
          ? fail(stoppedError())
          : mailbox
              .offer(letter)
              .catch(() => fail(stoppedError()))
              .map(() => undefined),
      );

    const ref: ActorRef<Message, E> = {
      tell: (message) => post({ message }),
      ask: <R>(message: (reply: Reply<R>) => Message) =>
        Deferred.make<R, E | ActorStopped>().flatMap((answer) =>
          post({
            message: message((value) => answer.succeed(value).map(() => undefined)),
            failed: (error) => answer.fail(error),
          }).flatMap(() => answer.await),
        ),
      stop: stop(new ActorStopped({ message: "the actor was stopped" })),
    };

    return acquireRelease(forkDaemon(loop), (fiber) =>
      ref.stop.flatMap(() => sync(() => fiber.interrupt())).flatMap(() => awaitFiber(fiber)),
    ).map(() => ref);
  });
}

export const Actor = {
  /** Start an actor; it stops when the surrounding scope ends. */
  make,
} as const;

export interface EntitiesOptions<Key extends string, State, Message, E> extends ActorOptions {
  /**
   * The behavior of the entity with this key. Entities are made when their
   * first message comes, so a behavior can't need services: provide them
   * inside `receive`.
   */
  readonly behavior: (key: Key) => Behavior<State, Message, Throws<E>>;
  /** Stop an entity after this long (Clock time) without messages. Default: never. */
  readonly idleMs?: number;
}

export interface Entities<Key extends string, Message, E = never> {
  tell(key: Key, message: Message): Eff<void, Throws<ActorStopped>>;
  ask<R>(key: Key, message: (reply: Reply<R>) => Message): Eff<R, Throws<E | ActorStopped>>;
  /** Entities running now. */
  readonly count: Eff<number, never>;
}

interface Running<Message, E> {
  readonly ref: ActorRef<Message, E>;
  readonly fiber: Fiber<void, unknown>;
  lastUsed: number;
}

/**
 * One actor per key: each key's messages are handled one at a time, and
 * different keys at the same time. An entity is made on its first message,
 * and stopped (losing its state) after `idleMs` without messages.
 */
function entities<Key extends string, State, Message, E = never>(
  options: EntitiesOptions<Key, State, Message, E>,
): Eff<Entities<Key, Message, E>, never> {
  const running = new Map<Key, Running<Message, E>>();

  return currentClock.flatMap((clock) => {
    // Each entity lives on a fiber of its own, holding its actor's scope
    // open until it is stopped.
    const start = (key: Key): Eff<Running<Message, E>, never> =>
      Deferred.make<ActorRef<Message, E>>().flatMap((started) =>
        Deferred.make<void>().flatMap((done) =>
          forkDaemon(
            make(options.behavior(key), options).flatMap((ref) =>
              started.succeed(ref).flatMap(() => done.await),
            ),
          ).flatMap((fiber) =>
            started.await.map((ref) => {
              const entity: Running<Message, E> = {
                ref: {
                  ...ref,
                  stop: ref.stop.flatMap(() => done.succeed(undefined)).map(() => undefined),
                },
                fiber,
                lastUsed: clock.now(),
              };
              running.set(key, entity);
              return entity;
            }),
          ),
        ),
      );

    const entity = (key: Key): Eff<ActorRef<Message, E>, never> =>
      suspend(() => {
        const found = running.get(key);
        if (found) {
          found.lastUsed = clock.now();
          return succeed(found.ref);
        }
        return start(key).map((started) => started.ref);
      });

    const sweep: Eff<void, never> = suspend(() => {
      const idleMs = options.idleMs;
      if (idleMs === undefined) return succeed(undefined);
      const now = clock.now();
      const idle = [...running].filter(([, e]) => now - e.lastUsed >= idleMs);
      for (const [key] of idle) running.delete(key);
      return idle.reduce<Eff<void, never>>(
        (rest, [, e]) => rest.flatMap(() => e.ref.stop),
        succeed(undefined),
      );
    });

    const sweeper: Eff<void, never> =
      options.idleMs === undefined
        ? succeed(undefined)
        : clock
            .sleep(Math.max(1, options.idleMs / 2))
            .flatMap(() => sweep)
            .flatMap(() => sweeper);

    const result: Entities<Key, Message, E> = {
      tell: (key, message) => entity(key).flatMap((ref) => ref.tell(message)),
      ask: (key, message) => entity(key).flatMap((ref) => ref.ask(message)),
      count: sync(() => running.size),
    };

    return acquireRelease(forkDaemon(sweeper), (fiber) =>
      sync(() => fiber.interrupt())
        .flatMap(() => awaitFiber(fiber))
        .flatMap(() => {
          const all = [...running.values()];
          running.clear();
          return all.reduce<Eff<void, never>>(
            (rest, e) => rest.flatMap(() => e.ref.stop),
            succeed(undefined),
          );
        }),
    ).map(() => result);
  });
}

export const Entities = {
  /** Start a set of keyed entities; they stop when the surrounding scope ends. */
  make: entities,
} as const;
