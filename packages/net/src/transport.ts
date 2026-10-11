// Opening connections and listening for them. `Tcp` uses real sockets (TCP,
// Unix domain sockets, TLS) through node:net and node:tls, so it runs on Node
// and Bun. Code that should also run against the network simulator in tests
// uses the `Transport` service instead of `Tcp` directly.

import * as net from "node:net";
import * as tls from "node:tls";
import type { Duplex } from "node:stream";
import {
  Log,
  acquireRelease,
  async,
  awaitFiber,
  fail,
  forkDaemon,
  fork,
  service,
  succeed,
  sync,
  type Eff,
  type ServiceTag,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import { connectionOver, type Connection, type ConnectionOptions } from "./connection.js";
import { ConnectError, ListenError } from "./errors.js";
import { Inbox } from "./inbox.js";

/** Where to connect or listen: a host and port, or a Unix socket path. */
export type Address = { readonly host: string; readonly port: number } | { readonly path: string };

/** "host:port" or the socket path, for messages and errors. */
export function formatAddress(address: Address): string {
  return "path" in address ? address.path : `${address.host}:${address.port}`;
}

/** Options for `connect`. */
export interface ConnectOptions<M> extends ConnectionOptions<M> {
  /**
   * Use TLS. `true` checks the server's certificate against the system's
   * trusted CAs; pass options for a custom CA, client certificates (mutual
   * TLS) or SNI.
   */
  readonly tls?: true | tls.ConnectionOptions;
  /** Give up connecting after this many ms. Default: 10 000. */
  readonly timeoutMs?: number;
}

/** Options for `listen`. */
export interface ListenOptions<M, S> extends ConnectionOptions<M> {
  /**
   * Called for every connection, on its own fiber, with the listener's
   * services. The connection is closed when it returns. If it fails, the
   * failure is logged and only that connection is closed.
   */
  readonly handle: (connection: Connection<M>) => Eff<unknown, S>;
  /** Serve TLS with this certificate and key (and options). */
  readonly tls?: tls.TlsOptions;
}

/** A running listener. */
export interface Listener {
  /** Where it listens; for port 0, the port the system picked. */
  readonly address: Address;
  /** Stop accepting, close the open connections and stop their handlers. */
  readonly close: Eff<void, never>;
}

/** Opens and accepts connections. `Tcp` in production, a simulated network in tests. */
export interface Transport {
  /** Connect. The connection is closed when the surrounding scope ends. */
  connect<M>(
    address: Address,
    options: ConnectOptions<M>,
  ): Eff<Connection<M>, Throws<ConnectError>>;
  /**
   * Listen. The listener is closed when the surrounding scope ends. The
   * handlers run with the services of the code that called `listen`, so the
   * services they need are needed here.
   */
  listen<M, S>(
    address: Address,
    options: ListenOptions<M, S>,
  ): Eff<Listener, Throws<ListenError> | Exclude<S, Throws<unknown>>>;
}

export const Transport: ServiceTag<Transport, "Transport"> = service<Transport>()("Transport");

// ── Real sockets ────────────────────────────────────────────────────

function openSocket<M>(
  address: Address,
  options: ConnectOptions<M>,
): Eff<Connection<M>, Throws<ConnectError>> {
  const where = formatAddress(address);
  return async<Connection<M>, ConnectError>((resume) => {
    const target =
      "path" in address ? { path: address.path } : { host: address.host, port: address.port };
    const useTls = options.tls !== undefined;
    const socket: net.Socket = useTls
      ? tls.connect({
          ...target,
          ...(options.tls === true ? {} : options.tls),
          // SNI defaults to the host we connect to.
          servername:
            (options.tls !== true && options.tls?.servername) ||
            ("host" in address && net.isIP(address.host) === 0 ? address.host : undefined),
        })
      : net.createConnection(target);
    const ready = useTls ? "secureConnect" : "connect";

    const timer = setTimeout(() => {
      socket.destroy();
      resume(
        fail(new ConnectError({ address: where, message: `timed out connecting to ${where}` })),
      );
    }, options.timeoutMs ?? 10_000);
    const failed = (error: Error) => {
      clearTimeout(timer);
      resume(fail(new ConnectError({ address: where, message: error.message, cause: error })));
    };
    socket.once("error", failed);
    socket.once(ready, () => {
      clearTimeout(timer);
      socket.off("error", failed);
      socket.setNoDelay?.(true);
      resume(succeed(connectionOver(socket, where, options)));
    });
    // Interrupted while connecting.
    return () => {
      clearTimeout(timer);
      socket.destroy();
    };
  });
}

/**
 * Accept connections from `server` and run `handle` for each, on fibers
 * that inherit the listener's services. Shared by the real and simulated
 * transports.
 */
export function serveConnections<M, S>(params: {
  readonly address: Address;
  /** Start accepting; call `accept` with each new socket. Returns how to stop. */
  readonly start: (
    accept: (socket: Duplex, remote: string) => void,
  ) => Eff<{ stop: () => void; address: Address }, Throws<ListenError>>;
  readonly options: ListenOptions<M, S>;
}): Eff<Listener, Throws<ListenError> | Exclude<S, Throws<unknown>>> {
  const { options } = params;
  const accepted = new Inbox<Connection<M>, never>(
    Infinity,
    () => {},
    () => {},
  );

  const serveOne = (connection: Connection<M>): Eff<unknown, Exclude<S, Throws<unknown>>> =>
    options
      .handle(connection)
      .exit()
      .flatMap((exit) =>
        exit._tag === "Failure"
          ? Log.warn("connection handler failed", {
              remote: connection.remote,
              cause: String(exit.cause),
            })
          : succeed(undefined),
      )
      .ensuring(connection.close);

  // Each accepted connection gets its own fiber, a child of this loop, so
  // stopping the loop stops every handler too.
  const acceptLoop = Stream.unfoldEffect(undefined, () =>
    accepted.take.map((batch) => (batch === undefined ? null : ([batch, undefined] as const))),
  ).forEach((batch) =>
    batch
      .reduce<Eff<unknown, Exclude<S, Throws<unknown>>>>(
        (previous, connection) => previous.flatMap(() => fork(serveOne(connection))),
        succeed(undefined),
      )
      .map(() => undefined),
  );

  const open = params
    .start((socket, remote) => accepted.push(connectionOver(socket, remote, options)))
    .flatMap((started) =>
      forkDaemon(acceptLoop).map((loop) => {
        let closed = false;
        const close: Eff<void, never> = sync(() => {
          if (closed) return false;
          closed = true;
          started.stop();
          accepted.end();
          return true;
        }).flatMap((first) =>
          first
            ? sync(() => loop.interrupt()).flatMap(() => awaitFiber(loop).map(() => undefined))
            : succeed(undefined),
        );
        return { address: started.address, close } satisfies Listener;
      }),
    );

  return acquireRelease(open, (listener) => listener.close);
}

function listenOnSockets<M, S>(
  address: Address,
  options: ListenOptions<M, S>,
): Eff<Listener, Throws<ListenError> | Exclude<S, Throws<unknown>>> {
  const where = formatAddress(address);
  return serveConnections({
    address,
    options,
    start: (accept) =>
      async<{ stop: () => void; address: Address }, ListenError>((resume) => {
        const onSocket = (socket: net.Socket) => {
          socket.setNoDelay?.(true);
          const remote =
            socket.remoteAddress === undefined
              ? where
              : `${socket.remoteAddress}:${socket.remotePort}`;
          accept(socket, remote);
        };
        const server = options.tls
          ? tls.createServer(options.tls, onSocket)
          : net.createServer(onSocket);
        const sockets = new Set<net.Socket>();
        server.on(options.tls ? "secureConnection" : "connection", (socket: net.Socket) => {
          sockets.add(socket);
          socket.once("close", () => sockets.delete(socket));
        });
        server.once("error", (error) =>
          resume(fail(new ListenError({ address: where, message: error.message, cause: error }))),
        );
        const target =
          "path" in address ? { path: address.path } : { host: address.host, port: address.port };
        server.listen(target, () => {
          const bound = server.address();
          const actual: Address =
            "path" in address || bound === null || typeof bound === "string"
              ? address
              : { host: address.host, port: bound.port };
          resume(
            succeed({
              address: actual,
              stop: () => {
                server.close();
                for (const socket of sockets) socket.destroy();
              },
            }),
          );
        });
        return () => server.close();
      }),
  });
}

/** Real TCP, Unix-socket and TLS connections. */
export const Tcp: Transport = {
  connect: (address, options) => acquireRelease(openSocket(address, options), (c) => c.close),
  listen: listenOnSockets,
};
