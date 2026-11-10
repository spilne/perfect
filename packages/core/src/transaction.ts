// Transactions as effects, one resource per transaction.
//
//   const pg = PgTransactor.make({ db });
//
//   pg.transact(function* () {
//     yield* payments.insert(row);           // needs Tx<"pg">
//     yield* queue.publishIn(pg, message);   // needs Tx<"pg">
//   });
//   // Tx<"pg"> is gone from the type, Commits<"pg"> is left in its place.
//
// A store that must run inside a transaction says so with `Tx<K>` in its
// effect type, so forgetting `transact` does not compile. Everything inside
// commits together; a typed failure, a defect or an interrupt rolls it all
// back.
//
// Nothing here can commit two resources together, so mixing them is refused:
// `transact` does not compile when the effect needs another resource's
// transaction, or contains one (that is what the `Commits<K>` mark is for).
// Effects that are not transactional, like logging or an HTTP call, may run
// inside a transaction, but they are not undone by a rollback.
//
// Nesting the same transactor joins the outer transaction (no savepoints).

import type { Commits, Eff, InferEffects, Needs } from "./eff.js";
import { die, failCause, succeed, sync, uninterruptibleMask } from "./constructors.js";
import { FiberLocal } from "./fiber-local.js";
import { eff as fromGenerator } from "./syntax/generator.js";
import { provide, service } from "./service.js";
import { InProcessSemaphore } from "./semaphore.js";

type TxName<K extends string> = `spilne/tx/${K}`;

/** Needs an open transaction on resource `K`, whose handle is `H`. */
export type Tx<K extends string, H = unknown> = Needs<H, TxName<K>>;

/** Resource names in S, other than K, that have a transaction needed or committed. */
export type ForeignTx<S, K extends string> =
  S extends Needs<unknown, TxName<infer Other>>
    ? Other extends K
      ? never
      : Other
    : S extends Commits<infer Other>
      ? Other extends K
        ? never
        : Other
      : never;

export type NoForeignTx<S, K extends string> = [ForeignTx<S, K>] extends [never]
  ? unknown
  : {
      "Cannot use another resource's transaction inside this one": ForeignTx<S, K>;
      "Transaction resource": K;
    };

/** S after `transact`: the transaction is provided, and the commit is recorded. */
export type Transacted<S, K extends string> = Exclude<S, Needs<unknown, TxName<K>>> | Commits<K>;

/** How a backend opens, commits and rolls back one transaction. */
export interface TxDriver<H, SE = never> {
  readonly begin: Eff<H, SE>;
  commit(handle: H): Eff<void, SE>;
  /** The transaction already failed, so a rollback error is not reported. */
  rollback(handle: H): Eff<void, never>;
  /**
   * Let an interrupt (a timeout, say) stop `begin` while it waits. Only for
   * a `begin` that gives back what it took when interrupted, like waiting for
   * a lock. Otherwise `begin` runs to the end, so an opened transaction is
   * never lost.
   */
  readonly interruptibleBegin?: boolean;
}

export interface Transactor<K extends string, H, SE = never> {
  readonly kind: K;
  /** For stores: run `f` with the open transaction's handle. */
  withHandle<A, S>(f: (handle: H) => Eff<A, S>): Eff<A, S | Tx<K, H>>;
  // One signature, not overloads: with overloads TypeScript reports the last
  // one's error and hides the readable "another resource" message.
  transact<T extends TxBody>(
    body: T & NoForeignTx<BodyEffects<T>, K>,
  ): Eff<BodyValue<T>, Transacted<BodyEffects<T>, K> | SE>;
}

/** What `transact` takes: an effect, or a generator function like `eff` takes. */
export type TxBody = Eff<unknown, unknown> | (() => Generator<Eff<unknown, unknown>, unknown, unknown>);

type BodyEffects<T> =
  T extends Eff<unknown, infer S>
    ? S
    : T extends () => Generator<infer Y, unknown, unknown>
      ? InferEffects<Y>
      : never;

type BodyValue<T> =
  T extends Eff<infer A, unknown> ? A : T extends () => Generator<unknown, infer A, unknown> ? A : never;

/** Opening another transactor's transaction inside an open one. */
export class MixedTransactionDefect extends Error {
  readonly open: string;
  readonly requested: string;
  constructor(params: { open: string; requested: string }) {
    super(
      params.open === params.requested
        ? `Cannot open another "${params.requested}" transactor's transaction inside an open "${params.open}" transaction`
        : `Cannot open a "${params.requested}" transaction inside an open "${params.open}" transaction`,
    );
    this.name = "MixedTransactionDefect";
    this.open = params.open;
    this.requested = params.requested;
  }
}

/** Using a transaction's handle after it committed or rolled back. */
export class TransactionClosedDefect extends Error {
  readonly kind: string;
  constructor(kind: string) {
    super(`The "${kind}" transaction has already ended; its handle cannot be used`);
    this.name = "TransactionClosedDefect";
    this.kind = kind;
  }
}

/** Two store calls running at the same time on one transaction. */
export class TransactionBusyDefect extends Error {
  readonly kind: string;
  constructor(kind: string) {
    super(
      `The "${kind}" transaction is already running an operation; store calls on one transaction cannot run in parallel`,
    );
    this.name = "TransactionBusyDefect";
    this.kind = kind;
  }
}

// What is provided for an open transaction. A Postgres transaction is one
// connection and a store call can be several queries, so calls must not
// overlap, and nothing may use the handle once the transaction has ended.
interface OpenTransaction<H = unknown> {
  readonly handle: H;
  /** The transactor that opened it; two transactors of one kind share a tag name. */
  readonly owner: object;
  readonly kind: string;
  closed: boolean;
  busy: boolean;
}

/**
 * The transaction open on this fiber, if any. Forks inherit it, so a fiber
 * that outlives the transaction sees it `closed` and may open new ones.
 */
const openTransaction = FiberLocal.make<OpenTransaction | undefined>(undefined, {
  name: "openTransaction",
});

/**
 * The transaction whose handle this fiber is already using, so a store call
 * made inside another store's `withHandle` is not mistaken for a parallel one.
 * Forks made inside `withHandle` inherit it too; don't run store calls in
 * parallel from there.
 */
const usingHandle = FiberLocal.make<OpenTransaction | undefined>(undefined, {
  name: "usingTransactionHandle",
});

function make<const K extends string, H, SE = never>(params: {
  kind: K;
  driver: TxDriver<H, SE>;
}): Transactor<K, H, SE> {
  const { kind, driver } = params;
  const openTag = service<OpenTransaction<H>>()(`spilne/tx/${kind}` as TxName<K>);
  const owner = {};

  const transactEff = (body: Eff<unknown, unknown>): Eff<unknown, unknown> =>
    openTransaction.get.flatMap((open): Eff<unknown, unknown> => {
      if (open !== undefined && !open.closed) {
        if (open.owner === owner) return body;
        return die(new MixedTransactionDefect({ open: open.kind, requested: kind }));
      }
      return uninterruptibleMask((restore) =>
        (driver.interruptibleBegin ? restore(driver.begin) : driver.begin).flatMap((handle) => {
          const state: OpenTransaction<H> = { handle, owner, kind, closed: false, busy: false };
          const close = sync(() => {
            state.closed = true;
          });
          return restore(openTransaction.locally(state, provide(body, openTag, state)))
            .catchAllCause((cause) =>
              close.flatMap(() => driver.rollback(handle)).flatMap(() => failCause(cause)),
            )
            .flatMap((value) =>
              close.flatMap(() => driver.commit(handle)).map(() => value),
            );
        }),
      );
    });

  const withHandle = <A, S>(f: (handle: H) => Eff<A, S>): Eff<A, S | Tx<K, H>> =>
    openTag.get.flatMap((state) =>
      usingHandle.get.flatMap((using): Eff<A, S> => {
        if (state.owner !== owner) {
          return die(new MixedTransactionDefect({ open: state.kind, requested: kind }));
        }
        if (state.closed) return die(new TransactionClosedDefect(kind));
        if (using === state) return f(state.handle);
        if (state.busy) return die(new TransactionBusyDefect(kind));
        state.busy = true;
        return usingHandle.locally(state, f(state.handle)).ensuring(
          sync(() => {
            state.busy = false;
          }),
        );
      }),
    ) as Eff<A, S | Tx<K, H>>;

  const transact = (body: TxBody) =>
    transactEff(typeof body === "function" ? fromGenerator(body) : body);

  return {
    kind,
    withHandle,
    transact: transact as Transactor<K, H, SE>["transact"],
  };
}

// ── In-memory backend ──────────────────────────────────────────────
// For tests and single-process use: the same all-or-nothing behaviour as a
// database, without one. Transactions run one at a time.

/** Writes made in the open transaction, applied on commit. `deleted` marks a delete. */
export interface MemoryTxHandle {
  readonly writes: Map<string, { readonly value: unknown } | "deleted">;
}

export interface MemoryTable<V, K extends string = "memory"> {
  get(key: string): Eff<V | undefined, Tx<K, MemoryTxHandle>>;
  set(key: string, value: V): Eff<void, Tx<K, MemoryTxHandle>>;
  delete(key: string): Eff<void, Tx<K, MemoryTxHandle>>;
  /** Committed contents only, read outside any transaction. */
  snapshot(): ReadonlyMap<string, V>;
}

function memory<const K extends string = "memory">(
  params: { kind?: K } = {},
): {
  transactor: Transactor<K, MemoryTxHandle>;
  table<V>(name: string): MemoryTable<V, K>;
} {
  const committed = new Map<string, unknown>();
  const lock = new InProcessSemaphore(1);

  const transactor = make<K, MemoryTxHandle>({
    kind: (params.kind ?? "memory") as K,
    driver: {
      begin: lock.acquire().map((): MemoryTxHandle => ({ writes: new Map() })),
      commit: (handle) =>
        sync(() => {
          for (const [key, write] of handle.writes) {
            if (write === "deleted") committed.delete(key);
            else committed.set(key, write.value);
          }
        }).flatMap(() => lock.release()),
      rollback: () => lock.release(),
      // Waiting for the lock gives the permit back if interrupted.
      interruptibleBegin: true,
    },
  });

  const table = <V>(name: string): MemoryTable<V, K> => {
    const keyOf = (key: string) => `${name}/${key}`;
    const prefix = `${name}/`;
    return {
      get: (key) =>
        transactor.withHandle((handle) =>
          sync(() => {
            const write = handle.writes.get(keyOf(key));
            if (write === undefined) return committed.get(keyOf(key)) as V | undefined;
            return write === "deleted" ? undefined : (write.value as V);
          }),
        ),
      set: (key, value) =>
        transactor.withHandle((handle) =>
          sync(() => {
            handle.writes.set(keyOf(key), { value });
          }),
        ),
      delete: (key) =>
        transactor.withHandle((handle) =>
          sync(() => {
            handle.writes.set(keyOf(key), "deleted");
          }),
        ),
      snapshot: () => {
        const rows = new Map<string, V>();
        for (const [key, value] of committed) {
          if (key.startsWith(prefix)) rows.set(key.slice(prefix.length), value as V);
        }
        return rows;
      },
    };
  };

  return { transactor, table };
}

export const Transactor = { make, memory };
