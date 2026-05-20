import { type Eff, Suspend } from "../eff";
import { run, runSync, runExit, runFiber } from "../runtime";
import type { Scheduler } from "../scheduler";
import type { Fiber } from "../fiber";
import type { Exit } from "../exit";

declare module "../eff" {
  interface Suspend {
    run<A>(this: Eff<A, any>, scheduler?: Scheduler): Promise<A>;
    runSync<A>(this: Eff<A, never>): A;
    runExit<A>(this: Eff<A, any>, scheduler?: Scheduler): Promise<Exit<unknown, A>>;
    runFiber<A>(this: Eff<A, any>, scheduler?: Scheduler): Fiber<A>;
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
