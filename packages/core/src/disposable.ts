// Resources and TypeScript's `using` / `await using`, both ways.
//
//   // A scoped effect as something plain async code can `await using`:
//   await using db = await runDisposable(openDatabase);
//   await db.value.query(...);          // closed when the block ends
//
//   // An object with [Symbol.dispose] or [Symbol.asyncDispose] as a
//   // resource of the surrounding scope:
//   const file = yield* acquireDisposable(sync(() => openFileHandle()));

import { type Eff, type EffectCheck } from "./eff.js";
import { acquireRelease, scoped, sync, tryPromise } from "./constructors.js";
import { Cause } from "./cause.js";
import { runFiber, type RunOptions } from "./runtime.js";

/** A value with the scope it was acquired in still open. */
export interface DisposableResource<A> extends AsyncDisposable {
  readonly value: A;
}

/**
 * Run a scoped effect and keep its scope open: its resources are released
 * when the result is disposed, at the end of an `await using` block (or by
 * calling `[Symbol.asyncDispose]()`). Rejects, with nothing left open, if
 * the effect fails. Disposing rejects if a release fails.
 */
export function runDisposable<A, S>(
  eff: Eff<A, S> & EffectCheck<S>,
  options: RunOptions = {},
): Promise<DisposableResource<A>> {
  return new Promise((resolve, reject) => {
    let letGo!: () => void;
    const released = new Promise<void>((done) => (letGo = done));
    let acquired = false;

    const holding: Eff<void, unknown> = scoped(
      (eff as Eff<A, S>).flatMap((value) =>
        sync(() => {
          acquired = true;
          resolve({
            value,
            [Symbol.asyncDispose]: async () => {
              letGo();
              const exit = await fiber.await();
              // Already closed by an aborted signal: nothing went wrong.
              if (exit._tag === "Failure" && !Cause.isInterruptedOnly(exit.cause)) {
                throw Cause.squash(exit.cause);
              }
            },
          });
        }).flatMap(() =>
          tryPromise(
            () => released,
            () => undefined,
          ),
        ),
      ),
    );
    // `eff` passed EffectCheck, so all it can still do is fail.
    const fiber = runFiber(holding as Eff<void, never>, options.scheduler);

    const { signal } = options;
    const onAbort = () => fiber.interrupt();
    if (signal?.aborted) onAbort();
    else signal?.addEventListener("abort", onAbort, { once: true });
    void fiber.await().then((exit) => {
      signal?.removeEventListener("abort", onAbort);
      if (!acquired && exit._tag === "Failure") reject(Cause.squash(exit.cause));
    });
  });
}

/**
 * Acquire a disposable object as a resource of the surrounding scope: it is
 * disposed (with `[Symbol.asyncDispose]` if it has one, otherwise
 * `[Symbol.dispose]`) when the scope closes. A failing dispose is a defect.
 */
export function acquireDisposable<A extends Disposable | AsyncDisposable, S>(
  acquire: Eff<A, S>,
): Eff<A, S> {
  return acquireRelease(acquire, (resource) =>
    Symbol.asyncDispose in resource
      ? tryPromise(
          async () => (resource as AsyncDisposable)[Symbol.asyncDispose](),
          (error) => error,
        ).orDie()
      : sync(() => (resource as Disposable)[Symbol.dispose]()),
  );
}
