import { type Eff, Suspend, Op } from "../eff";
import { suspend, succeed, interrupt, awaitFiber } from "../constructors";
import type { Fiber } from "../fiber";

// Finalizer for driver fibers: interrupt every fiber registered so far.
export function interruptAllEff(drivers: Fiber<any>[]): Eff<void, never> {
  return suspend(() => {
    const fs = drivers.splice(0);
    return fs.reduce<Eff<void, never>>(
      (acc, f) =>
        (acc as any)
          .flatMap(() => interrupt(f))
          .flatMap(() => awaitFiber(f))
          .map(() => undefined),
      succeed(undefined) as any,
    );
  }) as any;
}

export function combineFinalizers(
  first: Eff<void, unknown> | null,
  second: Eff<void, unknown> | null,
): Eff<void, unknown> | null {
  if (first === null) return second;
  if (second === null) return first;
  return new Suspend(Op.Ensuring, first, second) as any;
}
