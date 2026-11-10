import { describe, expect, test } from "bun:test";
import {
  Cause,
  type Eff,
  MixedTransactionDefect,
  TransactionBusyDefect,
  TransactionClosedDefect,
  all,
  forkDaemon,
  type Throws,
  awaitFiber,
  type TxDriver,
  Transactor,
  die,
  eff,
  fail,
  fork,
  interrupt,
  run,
  sleep,
  succeed,
  sync,
  timeoutOption,
} from "../src/index.js";

interface FakeHandle {
  readonly id: number;
}

type CommitFailed = { readonly _tag: "CommitFailed" };

// A driver that writes down what happened to each transaction, in order.
function recordingDriver(options: { failCommit?: boolean; slowCommit?: boolean } = {}) {
  const log: string[] = [];
  let opened = 0;
  const driver: TxDriver<FakeHandle, Throws<CommitFailed>> = {
    begin: sync(() => {
      opened += 1;
      log.push(`begin:${opened}`);
      return { id: opened };
    }),
    commit: (handle) => {
      if (options.failCommit) return fail<CommitFailed>({ _tag: "CommitFailed" });
      return (options.slowCommit ? sleep(20) : succeed(undefined)).map(() => {
        log.push(`commit:${handle.id}`);
      });
    },
    rollback: (handle) =>
      sync(() => {
        log.push(`rollback:${handle.id}`);
      }),
  };
  return { log, driver };
}

describe("Transactor lifecycle", () => {
  test("commits when the effect succeeds", async () => {
    const { log, driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    const value = await run(tx.transact(succeed(42)).catchTag("CommitFailed", () => succeed(0)));

    expect(value).toBe(42);
    expect(log).toEqual(["begin:1", "commit:1"]);
  });

  test("rolls back on a typed failure and re-fails with it", async () => {
    const { log, driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    const result = await run(
      tx
        .transact(fail({ _tag: "Declined" as const }))
        .catchTag("Declined", (e) => succeed(`caught ${e._tag}`))
        .catchTag("CommitFailed", () => succeed("commit failed")),
    );

    expect(result).toBe("caught Declined");
    expect(log).toEqual(["begin:1", "rollback:1"]);
  });

  test("rolls back on a defect", async () => {
    const { log, driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    const exit = await run(tx.transact(die(new Error("boom"))).exit());

    expect(exit._tag).toBe("Failure");
    expect(exit._tag === "Failure" && Cause.pretty(exit.cause)).toContain("boom");
    expect(log).toEqual(["begin:1", "rollback:1"]);
  });

  test("rolls back when interrupted", async () => {
    const { log, driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    await run(
      fork(tx.transact(sleep(1_000_000)).exit()).flatMap((fiber) =>
        sleep(5)
          .flatMap(() => interrupt(fiber))
          .flatMap(() => awaitFiber(fiber)),
      ),
    );

    expect(log).toEqual(["begin:1", "rollback:1"]);
  });

  test("a failed commit fails the effect", async () => {
    const { log, driver } = recordingDriver({ failCommit: true });
    const tx = Transactor.make({ kind: "fake", driver });

    const result = await run(
      tx
        .transact(succeed(1))
        .map(() => "committed")
        .catchTag("CommitFailed", () => succeed("commit failed")),
    );

    expect(result).toBe("commit failed");
    expect(log).toEqual(["begin:1"]);
  });

  test("interrupt while commit is running still finishes the commit", async () => {
    const { log, driver } = recordingDriver({ slowCommit: true });
    const tx = Transactor.make({ kind: "fake", driver });

    await run(
      fork(tx.transact(succeed(1)).exit()).flatMap((fiber) =>
        sleep(5)
          .flatMap(() => interrupt(fiber))
          .flatMap(() => awaitFiber(fiber)),
      ),
    );

    expect(log).toEqual(["begin:1", "commit:1"]);
  });

  test("the same transactor nested joins the outer transaction", async () => {
    const { log, driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    const value = await run(
      tx
        .transact(tx.transact(succeed(1)).map((n) => n + 1))
        .catchTag("CommitFailed", () => succeed(0)),
    );

    expect(value).toBe(2);
    expect(log).toEqual(["begin:1", "commit:1"]);
  });

  test("withHandle sees the open transaction's handle", async () => {
    const { driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    const id = await run(
      tx
        .transact(tx.withHandle((handle) => succeed(handle.id)))
        .catchTag("CommitFailed", () => succeed(0)),
    );

    expect(id).toBe(1);
  });

  test("accepts a generator function", async () => {
    const { log, driver } = recordingDriver();
    const tx = Transactor.make({ kind: "fake", driver });

    const value = await run(
      tx
        .transact(function* () {
          const a = yield* succeed(2);
          const b = yield* tx.withHandle((handle) => succeed(handle.id));
          return a + b;
        })
        .catchTag("CommitFailed", () => succeed(0)),
    );

    expect(value).toBe(3);
    expect(log).toEqual(["begin:1", "commit:1"]);
    void eff;
  });
});

describe("Transactor guards", () => {
  const plainDriver = () => recordingDriver().driver;

  test("another resource opened inside dies with MixedTransactionDefect", async () => {
    const a = Transactor.make({ kind: "a", driver: plainDriver() });
    const b = Transactor.make({ kind: "b", driver: plainDriver() });
    const sneaky = b.transact(succeed(1)) as unknown as Eff<number, never>;

    const exit = await run(a.transact(sneaky).exit());

    expect(exit._tag).toBe("Failure");
    const defect = exit._tag === "Failure" ? Cause.defects(exit.cause)[0] : undefined;
    expect(defect).toBeInstanceOf(MixedTransactionDefect);
    expect((defect as MixedTransactionDefect).message).toBe(
      'Cannot open a "b" transaction inside an open "a" transaction',
    );
  });

  test("two transactors of the same kind do not join", async () => {
    const outer = recordingDriver();
    const inner = recordingDriver();
    const first = Transactor.make({ kind: "a", driver: outer.driver });
    const second = Transactor.make({ kind: "a", driver: inner.driver });

    const exit = await run(first.transact(second.transact(succeed(1))).exit());

    const defect = exit._tag === "Failure" ? Cause.defects(exit.cause)[0] : undefined;
    expect(defect).toBeInstanceOf(MixedTransactionDefect);
    expect((defect as MixedTransactionDefect).message).toContain('another "a" transactor');
    expect(inner.log).toEqual([]);
    expect(outer.log).toEqual(["begin:1", "rollback:1"]);
  });

  test("a handle used after the transaction ended dies", async () => {
    const tx = Transactor.make({ kind: "a", driver: plainDriver() });

    const exit = await run(
      tx
        .transact(forkDaemon(sleep(10).flatMap(() => tx.withHandle((h) => succeed(h.id)))))
        .orDie()
        .flatMap((fiber) => awaitFiber(fiber)),
    );

    expect(exit._tag).toBe("Failure");
    const defect = exit._tag === "Failure" ? Cause.defects(exit.cause)[0] : undefined;
    expect(defect).toBeInstanceOf(TransactionClosedDefect);
    expect((defect as TransactionClosedDefect).message).toBe(
      'The "a" transaction has already ended; its handle cannot be used',
    );
  });

  test("parallel withHandle calls on one transaction die", async () => {
    const tx = Transactor.make({ kind: "a", driver: plainDriver() });
    const slow = tx.withHandle(() => sleep(10));

    const exit = await run(tx.transact(all([slow, slow])).exit());

    const defect = exit._tag === "Failure" ? Cause.defects(exit.cause)[0] : undefined;
    expect(defect).toBeInstanceOf(TransactionBusyDefect);
    expect((defect as TransactionBusyDefect).message).toBe(
      'The "a" transaction is already running an operation; store calls on one transaction cannot run in parallel',
    );
  });

  test("sequential withHandle calls on one transaction are fine", async () => {
    const tx = Transactor.make({ kind: "a", driver: plainDriver() });
    const slow = tx.withHandle(() => sleep(1).map(() => 1));

    const exit = await run(tx.transact(slow.flatMap(() => slow)).exit());

    expect(exit._tag).toBe("Success");
  });
});

describe("Transactor.memory", () => {
  test("writes in two tables commit together", async () => {
    const { transactor, table } = Transactor.memory();
    const payments = table<string>("payments");
    const outbox = table<string>("outbox");

    await run(
      transactor.transact(function* () {
        yield* payments.set("p1", "paid");
        yield* outbox.set("m1", "p1 paid");
      }),
    );

    expect(payments.snapshot()).toEqual(new Map([["p1", "paid"]]));
    expect(outbox.snapshot()).toEqual(new Map([["m1", "p1 paid"]]));
  });

  test("nothing is visible before commit", async () => {
    const { transactor, table } = Transactor.memory();
    const payments = table<string>("payments");

    const seenInside = await run(
      transactor.transact(payments.set("p1", "paid").map(() => payments.snapshot().size)),
    );

    expect(seenInside).toBe(0);
    expect(payments.snapshot().size).toBe(1);
  });

  test("reads see the transaction's own writes, including deletes", async () => {
    const { transactor, table } = Transactor.memory();
    const payments = table<string>("payments");
    await run(transactor.transact(payments.set("p1", "draft")));

    const seen = await run(
      transactor.transact(function* () {
        yield* payments.set("p1", "paid");
        const afterSet = yield* payments.get("p1");
        yield* payments.delete("p1");
        const afterDelete = yield* payments.get("p1");
        return { afterSet, afterDelete };
      }),
    );

    expect(seen).toEqual({ afterSet: "paid", afterDelete: undefined });
    expect(payments.snapshot().size).toBe(0);
  });

  test("a failure rolls back every table", async () => {
    const { transactor, table } = Transactor.memory();
    const payments = table<string>("payments");
    const outbox = table<string>("outbox");

    await run(
      transactor
        .transact(function* () {
          yield* payments.set("p1", "paid");
          yield* outbox.set("m1", "p1 paid");
          return yield* fail({ _tag: "StripeDown" as const });
        })
        .catchTag("StripeDown", () => succeed(undefined)),
    );

    expect(payments.snapshot().size).toBe(0);
    expect(outbox.snapshot().size).toBe(0);
  });

  test("concurrent transactions run one at a time", async () => {
    const { transactor, table } = Transactor.memory();
    const counter = table<number>("counter");
    const increment = transactor.transact(function* () {
      const n = (yield* counter.get("n")) ?? 0;
      yield* sleep(5);
      yield* counter.set("n", n + 1);
    });

    await run(all([increment, increment]));

    expect(counter.snapshot().get("n")).toBe(2);
  });

  test("a custom kind is used in the types and at run time", () => {
    const { transactor } = Transactor.memory({ kind: "ledger" });
    expect(transactor.kind).toBe("ledger");
  });
});

describe("Transactor review fixes", () => {
  test("a store of another same-kind transactor cannot use the open transaction", async () => {
    const a = Transactor.memory();
    const b = Transactor.memory();
    const bRows = b.table<number>("rows");

    const exit = await run(a.transactor.transact(bRows.set("x", 1)).exit());

    const defect = exit._tag === "Failure" ? Cause.defects(exit.cause)[0] : undefined;
    expect(defect).toBeInstanceOf(MixedTransactionDefect);
    expect(bRows.snapshot().size).toBe(0);
  });

  test("a fiber that outlives a transaction can open new ones later", async () => {
    const a = Transactor.make({ kind: "a", driver: recordingDriver().driver });
    const b = Transactor.make({ kind: "b", driver: recordingDriver().driver });
    const later = sleep(10).flatMap(() =>
      all([
        b.transact(b.withHandle((h) => succeed(h.id))).orDie(),
        a.transact(a.withHandle((h) => succeed(h.id))).orDie(),
      ]),
    ) as unknown as Eff<unknown, never>; // the types refuse this conservatively; the runtime must allow it

    const exit = await run(
      a
        .transact(forkDaemon(later))
        .orDie()
        .flatMap((fiber) => awaitFiber(fiber)),
    );

    expect(exit._tag).toBe("Success");
  });

  test("a store call inside withHandle on the same fiber is not a parallel call", async () => {
    const { transactor, table } = Transactor.memory();
    const rows = table<number>("rows");

    const value = await run(
      transactor.transact(
        rows.set("x", 7).flatMap(() => transactor.withHandle(() => rows.get("x"))),
      ),
    );

    expect(value).toBe(7);
  });

  test("waiting to begin a memory transaction can be interrupted by a timeout", async () => {
    const { transactor } = Transactor.memory();
    const hold = transactor.transact(sleep(300));

    const started = Date.now();
    const result = await run(
      fork(hold).flatMap((holder) =>
        sleep(5)
          .flatMap(() => timeoutOption(transactor.transact(succeed("got in")), 20))
          .flatMap((got) => sync(() => ({ got, firedAfter: Date.now() - started })))
          .flatMap((outcome) => awaitFiber(holder).map(() => outcome)),
      ),
    );

    expect(result.got).toBeUndefined();
    expect(result.firedAfter).toBeLessThan(150);
  });

  test("the timed-out waiter gives its place back", async () => {
    const { transactor } = Transactor.memory();
    const value = await run(
      timeoutOption(transactor.transact(sleep(50)), 10).flatMap(() =>
        transactor.transact(succeed("next")),
      ),
    );
    expect(value).toBe("next");
  });
});
