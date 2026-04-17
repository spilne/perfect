import { type Eff, type Throws, Suspend, Op } from "../eff";
import { type Fiber } from "../fiber";
import { type Exit } from "../exit";
import { sleep, timeoutFail, timeoutOption, onExit, race } from "../constructors";

declare module "../eff" {
  interface Suspend {
    fork<A, S>(this: Eff<A, S>): Eff<Fiber<A>, never>;
    forkDaemon<A, S>(this: Eff<A, S>): Eff<Fiber<A>, never>;
    ensuring<A, S>(this: Eff<A, S>, finalizer: Eff<void, never>): Eff<A, S>;
    onExit<A, S, S2>(
      this: Eff<A, S>,
      handler: (exit: Exit<unknown, A>) => Eff<void, S2>,
    ): Eff<A, S | S2>;
    uninterruptible<A, S>(this: Eff<A, S>): Eff<A, S>;
    interruptible<A, S>(this: Eff<A, S>): Eff<A, S>;
    timeoutFail<A, S, E>(this: Eff<A, S>, ms: number, onTimeout: () => E): Eff<A, S | Throws<E>>;
    timeoutOption<A, S>(this: Eff<A, S>, ms: number): Eff<A | undefined, S>;
    race<A, S1, B, S2>(this: Eff<A, S1>, that: Eff<B, S2>): Eff<A | B, S1 | S2>;
    delay<A, S>(this: Eff<A, S>, ms: number): Eff<A, S>;
    when<A, S>(this: Eff<A, S>, cond: () => boolean): Eff<A | undefined, S>;
    unless<A, S>(this: Eff<A, S>, cond: () => boolean): Eff<A | undefined, S>;
  }
}

Suspend.prototype.fork = function () {
  return new Suspend(Op.Fork, this, null) as any;
};

Suspend.prototype.forkDaemon = function () {
  return new Suspend(Op.ForkDaemon, this, null) as any;
};

Suspend.prototype.ensuring = function (finalizer: any) {
  return new Suspend(Op.Ensuring, this, finalizer) as any;
};

Suspend.prototype.onExit = function (handler: any) {
  return onExit(this as any, handler) as any;
};

Suspend.prototype.uninterruptible = function () {
  return new Suspend(Op.SetInterruptible, this, false) as any;
};

Suspend.prototype.interruptible = function () {
  return new Suspend(Op.SetInterruptible, this, true) as any;
};

Suspend.prototype.timeoutFail = function (ms: any, onTimeout: any) {
  return timeoutFail(this as any, ms, onTimeout) as any;
};

Suspend.prototype.timeoutOption = function (ms: any) {
  return timeoutOption(this as any, ms) as any;
};

Suspend.prototype.race = function (that: any) {
  return race([this as any, that]) as any;
};

Suspend.prototype.delay = function (ms: any) {
  return new Suspend(Op.FlatMap, sleep(ms), () => this) as any;
};

Suspend.prototype.when = function (cond: any) {
  return new Suspend(Op.Sync, () => cond(), null).flatMap((c: boolean) =>
    c ? (this as any) : new Suspend(Op.Succeed, undefined, null),
  ) as any;
};

Suspend.prototype.unless = function (cond: any) {
  return new Suspend(Op.Sync, () => cond(), null).flatMap((c: boolean) =>
    c ? new Suspend(Op.Succeed, undefined, null) : (this as any),
  ) as any;
};
