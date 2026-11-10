// PgTransactor — Postgres transactions as effects (see core's transaction.ts).
//
//   const pg = PgTransactor.make({ db });
//   pg.transact(function* () {
//     yield* pg.withHandle(({ db }) => fromPromise(() => db.insert(payments).values(row), ...));
//     yield* queue.publishIn(pg, message);
//   });
//
// A second database gets its own kind, so the two can never be mixed in one
// transaction: `PgTransactor.make({ db: billingDb, kind: "billing" })`.

import { Transactor, fromPromise, succeed, type Throws, type TxDriver } from "@spilne/perfect-core";
import type { DrizzleDb } from "./drizzle-db.js";
import { type PostgresError, toPostgresError } from "./postgres-error.js";

/** An open Postgres transaction: run Drizzle queries on `db` to run them inside it. */
export interface PgTx {
  readonly db: DrizzleDb;
}

// How an open transaction is finished. Drizzle only offers callback-style
// `db.transaction(async (tx) => ...)`, so the callback is kept waiting on
// `finish` until commit or rollback decides how it ends.
interface Control {
  readonly finish: PromiseWithResolvers<void>;
  readonly done: Promise<unknown>;
}

const rollbackSignal = Symbol("pg-transactor/rollback");

function driver(db: DrizzleDb): TxDriver<PgTx, Throws<PostgresError>> {
  const controls = new WeakMap<PgTx, Control>();

  return {
    begin: fromPromise(
      async (): Promise<PgTx> => {
        const ready = Promise.withResolvers<DrizzleDb>();
        const finish = Promise.withResolvers<void>();
        const done = db.transaction(async (tx) => {
          ready.resolve(tx as unknown as DrizzleDb);
          await finish.promise;
        });
        // If BEGIN itself fails, the callback never runs.
        done.catch((cause: unknown) => ready.reject(cause));
        const handle: PgTx = { db: await ready.promise };
        controls.set(handle, { finish, done });
        return handle;
      },
      (cause) => toPostgresError("pg.begin", cause),
    ),

    commit: (handle) =>
      fromPromise(
        async () => {
          const control = controls.get(handle)!;
          control.finish.resolve();
          await control.done;
        },
        (cause) => toPostgresError("pg.commit", cause),
      ),

    rollback: (handle) =>
      fromPromise(
        async () => {
          const control = controls.get(handle)!;
          control.finish.reject(rollbackSignal);
          await control.done.catch(() => undefined);
        },
        (cause) => cause,
      ).orElse(() => succeed(undefined)),
  };
}

export const PgTransactor = {
  make<const K extends string = "pg">(params: {
    db: DrizzleDb;
    kind?: K;
  }): Transactor<K, PgTx, Throws<PostgresError>> {
    return Transactor.make({ kind: (params.kind ?? "pg") as K, driver: driver(params.db) });
  },
};

