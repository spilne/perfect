// ---------------------------------------------------------------------------
// PgRef<T> — Postgres-backed atomic reference (JSON value stored as TEXT)
//
// Implements @spilne/perfect-core's Eff-typed Ref<A> contract. Atomicity comes
// from a SELECT ... FOR UPDATE transaction per mutation. Adapted from
// promin's Promise-typed AtomicRef (getAsync/setAsync/updateAsync):
// perfect's Ref adds modify/getAndSet/getAndUpdate/updateAndGet, all
// derived here from one transactional modify. Driver failures remain typed
// as PostgresError in the effect union.
// ---------------------------------------------------------------------------

import { fromPromise } from "@spilne/perfect-core";
import type { Eff, Ref, Throws } from "@spilne/perfect-core";
import { sql } from "drizzle-orm";
import { type DrizzleDb, execRaw } from "./drizzle-db";
import { PostgresError, toPostgresError } from "./postgres-error";

export interface PgRefConfig<T> {
  db: DrizzleDb;
  name: string;
  initial: T;
  table?: string;
}

export class PgRef<T> implements Ref<T, Throws<PostgresError>> {
  private readonly db: DrizzleDb;
  private readonly name: string;
  private readonly table: string;
  private setupPromise: Promise<void> | null = null;

  constructor(config: PgRefConfig<T>) {
    this.db = config.db;
    this.name = config.name;
    this.table = config.table ?? "perfect_ref";
    this.setupPromise = this._setup(config.initial);
    this.setupPromise.catch(() => {});
  }

  static async make<T>(config: PgRefConfig<T>): Promise<PgRef<T>> {
    const ref = new PgRef(config);
    await ref._ensureReady();
    return ref;
  }

  private async _setup(initial: T): Promise<void> {
    await this.db.execute(
      sql.raw(`
        CREATE TABLE IF NOT EXISTS ${this.table} (
          name TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
      `),
    );
    // Insert initial value only if row doesn't exist
    await execRaw(
      this.db,
      sql`INSERT INTO ${sql.raw(this.table)} (name, value) VALUES (${this.name}, ${JSON.stringify(initial)}) ON CONFLICT (name) DO NOTHING`,
    );
  }

  private async _ensureReady(): Promise<void> {
    if (this.setupPromise) {
      await this.setupPromise;
      this.setupPromise = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Promise-facing driver operations
  // ---------------------------------------------------------------------------

  private async getAsync(): Promise<T> {
    await this._ensureReady();
    const rows = await execRaw(
      this.db,
      sql`SELECT value FROM ${sql.raw(this.table)} WHERE name = ${this.name}`,
    );
    if (rows.length === 0) throw new Error(`PgRef: row not found for name "${this.name}"`);
    return JSON.parse(rows[0].value as string) as T;
  }

  private async setAsync(value: T): Promise<void> {
    await this._ensureReady();
    await execRaw(
      this.db,
      sql`UPDATE ${sql.raw(this.table)} SET value = ${JSON.stringify(value)} WHERE name = ${this.name}`,
    );
  }

  /** Transactional read-modify-write; returns the first tuple element. */
  private async modifyAsync<B>(f: (a: T) => [B, T]): Promise<B> {
    await this._ensureReady();
    const table = this.table;
    const name = this.name;

    let result: B;
    await this.db.transaction(async (tx) => {
      const db = tx as DrizzleDb;
      const rows = await execRaw(
        db,
        sql`SELECT value FROM ${sql.raw(table)} WHERE name = ${name} FOR UPDATE`,
      );
      if (rows.length === 0) throw new Error(`PgRef: row not found for name "${name}"`);
      const current = JSON.parse(rows[0].value as string) as T;
      const [b, next] = f(current);
      result = b;
      await execRaw(
        db,
        sql`UPDATE ${sql.raw(table)} SET value = ${JSON.stringify(next)} WHERE name = ${name}`,
      );
    });
    return result!;
  }

  // ---------------------------------------------------------------------------
  // Ref<T> (Eff-typed contract)
  // ---------------------------------------------------------------------------

  get get(): Eff<T, Throws<PostgresError>> {
    return fromPromise(
      () => this.getAsync(),
      (e) => toPostgresError("ref.get", e),
    );
  }

  set(value: T): Eff<void, Throws<PostgresError>> {
    return fromPromise(
      () => this.setAsync(value),
      (e) => toPostgresError("ref.set", e),
    );
  }

  update(f: (a: T) => T): Eff<void, Throws<PostgresError>> {
    return fromPromise(
      () => this.modifyAsync<void>((a) => [undefined, f(a)]),
      (e) => toPostgresError("ref.update", e),
    );
  }

  modify<B>(f: (a: T) => [B, T]): Eff<B, Throws<PostgresError>> {
    return fromPromise(
      () => this.modifyAsync(f),
      (e) => toPostgresError("ref.modify", e),
    );
  }

  getAndSet(value: T): Eff<T, Throws<PostgresError>> {
    return fromPromise(
      () => this.modifyAsync<T>((a) => [a, value]),
      (e) => toPostgresError("ref.getAndSet", e),
    );
  }

  getAndUpdate(f: (a: T) => T): Eff<T, Throws<PostgresError>> {
    return fromPromise(
      () => this.modifyAsync<T>((a) => [a, f(a)]),
      (e) => toPostgresError("ref.getAndUpdate", e),
    );
  }

  updateAndGet(f: (a: T) => T): Eff<T, Throws<PostgresError>> {
    return fromPromise(
      () =>
        this.modifyAsync<T>((a) => {
          const next = f(a);
          return [next, next];
        }),
      (e) => toPostgresError("ref.updateAndGet", e),
    );
  }
}
