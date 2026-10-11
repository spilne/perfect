// A connection that reconnects by itself. Give it the effect that opens a
// connection; when the connection breaks or the other side closes it, the
// next send (or the message stream) opens a new one, with retries and,
// optionally, a circuit breaker.
//
//   const connection = yield* Resilient.connect(Tcp.connect(address, { codec }), {
//     retry: RetryPolicy.exponential({ initial: 100 }).withMaxDelay(5_000).withMaxRetries(20),
//   });
//
// What it can't do is know whether a message sent just as the connection
// broke arrived. Such a message may be lost, or sent again on the new
// connection; protocols that care should acknowledge messages or make them
// safe to repeat.

import {
  CircuitBreaker,
  Deferred,
  RetryPolicy,
  acquireRelease,
  fail,
  forkDaemon,
  retry,
  scoped,
  succeed,
  suspend,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import type { Connection } from "./connection.js";
import { ConnectError, ConnectionError, type NetError } from "./errors.js";

export interface ResilientOptions {
  /**
   * How to retry opening a connection. Default: from 100 ms, doubling up to
   * 5 s between tries, at most 10 tries in a row.
   */
  readonly retry?: RetryPolicy;
  /**
   * Stop trying for a while after the server failed several times in a row,
   * so callers fail fast instead of each waiting through the retries.
   */
  readonly breaker?: CircuitBreaker<ConnectError>;
}

const defaultRetry = RetryPolicy.exponential({ initial: 100 })
  .withMaxDelay(5_000)
  .withMaxRetries(10);

/** A connection kept open by a background fiber until `release` is called. */
interface Held<M> {
  readonly connection: Connection<M>;
  readonly release: Eff<void, never>;
}

/**
 * Open a connection on a background fiber that keeps its scope open until
 * we let go. Opening scoped connections in the caller's fiber would close
 * them when that caller's scope ends, which for a reconnect inside `send`
 * could be right away.
 */
function hold<M>(
  open: Eff<Connection<M>, Throws<ConnectError>>,
): Eff<Held<M>, Throws<ConnectError>> {
  return Deferred.make<Connection<M>, ConnectError>().flatMap((opened) =>
    Deferred.make<void>().flatMap((letGo) =>
      forkDaemon(
        scoped(
          open
            .flatMap((connection) => opened.succeed(connection).flatMap(() => letGo.await))
            .catch((error) => opened.fail(error).map(() => undefined)),
        ),
      ).flatMap(() =>
        opened.await.map((connection) => ({
          connection,
          release: letGo.succeed(undefined).map(() => undefined),
        })),
      ),
    ),
  ) as Eff<Held<M>, Throws<ConnectError>>;
}

export const Resilient = {
  /**
   * A connection that reopens itself with `open` when it breaks. It is
   * closed (and stops reconnecting) when the surrounding scope ends.
   */
  connect<M>(
    open: Eff<Connection<M>, Throws<ConnectError>>,
    options: ResilientOptions = {},
  ): Eff<Connection<M>, Throws<ConnectError>> {
    const policy = options.retry ?? defaultRetry;
    const breaker = options.breaker;
    let current: Held<M> | undefined;
    // A reconnect in progress, shared by everyone who needs the connection.
    let reconnecting: Eff<Held<M>, Throws<ConnectError>> | undefined;
    let closed = false;
    let remote = "";

    const openOnce: Eff<Held<M>, Throws<ConnectError>> = breaker
      ? (breaker
          .protect(hold(open))
          .catchTag("CircuitOpen", () =>
            fail(new ConnectError({ address: remote, message: "the circuit breaker is open" })),
          ) as Eff<Held<M>, Throws<ConnectError>>)
      : hold(open);

    const reconnect: Eff<Held<M>, Throws<ConnectError>> = suspend(() => {
      if (reconnecting) return reconnecting;
      const attempt = retry(openOnce, policy).map((held) => {
        current = held;
        remote = held.connection.remote;
        return held;
      });
      // Every caller waits for the same attempt.
      reconnecting = Deferred.make<Held<M>, ConnectError>().flatMap((result) =>
        forkDaemon(
          attempt
            .flatMap((held) => result.succeed(held))
            .catch((error) => result.fail(error))
            .ensuring(sync(() => (reconnecting = undefined))),
        ).flatMap(() => result.await),
      ) as Eff<Held<M>, Throws<ConnectError>>;
      return reconnecting;
    });

    const connection: Eff<Connection<M>, Throws<ConnectError>> = suspend(() =>
      closed
        ? fail(new ConnectError({ address: remote, message: "the connection is closed" }))
        : current
          ? succeed(current.connection)
          : reconnect.map((held) => held.connection),
    );

    // Forget a broken connection (once), so the next use opens a new one.
    const drop = (broken: Connection<M>): Eff<void, never> =>
      suspend(() => {
        if (current?.connection !== broken) return succeed(undefined);
        const held = current;
        current = undefined;
        return held.release;
      });

    const asNetError = (error: ConnectError): NetError =>
      new ConnectionError({ address: error.address, message: error.message, cause: error });

    const send = (message: M): Eff<void, Throws<NetError>> =>
      connection
        .catch((error) => fail(asNetError(error)))
        .flatMap((c) =>
          c.send(message).catchTag("ConnectionError", () =>
            // The connection broke: send once more on a new one.
            drop(c).flatMap(() =>
              connection
                .catch((error) => fail(asNetError(error)))
                .flatMap((fresh) => fresh.send(message)),
            ),
          ),
        ) as Eff<void, Throws<NetError>>;

    // The messages of one connection after another, until closed. A broken
    // or closed connection is replaced; a message that can't be decoded still
    // fails the stream.
    const messagesFrom = (): Stream<M, Throws<NetError>> =>
      closed
        ? Stream.empty()
        : (Stream.fromEffect(connection.catch((error) => fail(asNetError(error)))).flatMap((c) =>
            c.messages
              .catch((error): Stream<M, Throws<NetError>> =>
                error._tag === "ConnectionError" ? Stream.empty() : Stream.fail(error),
              )
              .concat(Stream.suspend(() => Stream.fromEffect(drop(c)).flatMap(messagesFrom))),
          ) as Stream<M, Throws<NetError>>);

    const close: Eff<void, never> = suspend(() => {
      closed = true;
      const held = current;
      current = undefined;
      return held ? held.connection.close.flatMap(() => held.release) : succeed(undefined);
    });

    const resilient: Connection<M> = {
      get remote() {
        return remote;
      },
      send,
      messages: Stream.suspend(messagesFrom),
      close,
    };

    // Connect once now, so a bad address fails right away.
    return acquireRelease(
      reconnect.map(() => resilient),
      (c) => c.close,
    );
  },
} as const;
