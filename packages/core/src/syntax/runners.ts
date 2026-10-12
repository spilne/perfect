import { type Eff, type EffectCheck, type ErrorsOf, Suspend } from "../eff.js";
import { run, runSync, runExit, runFiber, type RunOptions } from "../runtime.js";
import type { Scheduler } from "../scheduler.js";
import type { Fiber } from "../fiber.js";
import type { Exit } from "../exit.js";

declare module "../eff.js" {
  interface Suspend {
    run<A, S>(this: Eff<A, S> & EffectCheck<S>, options?: Scheduler | RunOptions): Promise<A>;
    runSync<A>(this: Eff<A, never>): A;
    runExit<A>(this: Eff<A, unknown>, options?: Scheduler | RunOptions): Promise<Exit<unknown, A>>;
    runFiber<A, S>(this: Eff<A, S> & EffectCheck<S>, scheduler?: Scheduler): Fiber<A, ErrorsOf<S>>;
  }
}

Suspend.prototype.run = function (options?: Scheduler | RunOptions) {
  return run(this, options);
};

Suspend.prototype.runSync = function () {
  return runSync(this);
};

Suspend.prototype.runExit = function (options?: Scheduler | RunOptions) {
  return runExit(this, options);
};

Suspend.prototype.runFiber = function (scheduler?: Scheduler) {
  return runFiber(this, scheduler);
};
