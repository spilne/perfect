import { type Eff, type EffectCheck, Suspend } from "../eff.js";
import { run, runSync, runExit, runFiber } from "../runtime.js";
import type { Scheduler } from "../scheduler.js";
import type { Fiber } from "../fiber.js";
import type { Exit } from "../exit.js";

declare module "../eff.js" {
  interface Suspend {
    run<A, S>(this: Eff<A, S> & EffectCheck<S>, scheduler?: Scheduler): Promise<A>;
    runSync<A>(this: Eff<A, never>): A;
    runExit<A>(this: Eff<A, unknown>, scheduler?: Scheduler): Promise<Exit<unknown, A>>;
    runFiber<A, S>(this: Eff<A, S> & EffectCheck<S>, scheduler?: Scheduler): Fiber<A>;
  }
}

Suspend.prototype.run = function (this: any, scheduler?: Scheduler) {
  return run(this, scheduler);
};

Suspend.prototype.runSync = function (this: any) {
  return runSync(this);
};

Suspend.prototype.runExit = function (this: any, scheduler?: Scheduler) {
  return runExit(this, scheduler);
};

Suspend.prototype.runFiber = function (this: any, scheduler?: Scheduler) {
  return runFiber(this, scheduler);
};
