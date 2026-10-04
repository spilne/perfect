import { run, runFiber, type Eff, type Fiber, type Scheduler } from "@spilne/perfect-core";

// Some tests run an effect whose errors are left unhandled on purpose, to
// check how run() rejects. run() refuses such effects at the type level, so
// tests call this instead to say "on purpose" out loud. At runtime it is
// exactly run().
export function runUnchecked<A>(eff: Eff<A, unknown>, scheduler?: Scheduler): Promise<A> {
  return run(eff as Eff<A, never>, scheduler);
}

// The same for runFiber(), for tests that read the failure from the fiber.
export function runFiberUnchecked<A>(eff: Eff<A, unknown>, scheduler?: Scheduler): Fiber<A> {
  return runFiber(eff as Eff<A, never>, scheduler);
}
