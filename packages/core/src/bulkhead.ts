// Bulkhead — limit how many calls to something run at once, and how many may
// wait for their turn. When the waiting line is full, a call fails right away
// with a typed `BulkheadFull` instead of piling up, so one slow dependency
// can't take all the memory and fibers with it.
//
//   const payments = Bulkhead.make({ maxConcurrent: 10, maxQueued: 50 });
//   payments.protect(chargeCard(order))   // Eff<Receipt, ... | Throws<BulkheadFull>>
//
// Calls get their turn in arrival order. `maxWaitMs` also fails a call that
// waited too long. A call interrupted while waiting leaves the line.

import { type Eff, type Throws } from "./eff.js";
import {
  async,
  ensuring,
  fail,
  onExit,
  race,
  sleep,
  succeed,
  suspend,
  sync,
} from "./constructors.js";
import { Waiter, WaiterList } from "./internal/waiter-list.js";

export interface BulkheadFull {
  readonly _tag: "BulkheadFull";
  /** Why the call was turned away. */
  readonly reason: "queue full" | "waited too long";
  readonly maxConcurrent: number;
  readonly maxQueued: number;
}

export interface BulkheadOptions {
  /** Calls that may run at the same time. */
  readonly maxConcurrent: number;
  /** Calls that may wait for a turn; more fail at once. Default: 0. */
  readonly maxQueued?: number;
  /** Fail a call that waited this long for its turn (Clock time). Default: no limit. */
  readonly maxWaitMs?: number;
}

export interface Bulkhead {
  /** Run `eff` when there is room; fail with BulkheadFull when there isn't. */
  protect<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<BulkheadFull>>;
  /** Calls running now. */
  readonly running: Eff<number, never>;
  /** Calls waiting for their turn now. */
  readonly queued: Eff<number, never>;
}

/** A call's place in line. */
class Turn extends Waiter {
  granted = false;
  onGrant: (() => void) | undefined;

  grant(): void {
    this.granted = true;
    this.onGrant?.();
  }
}

class InProcessBulkhead implements Bulkhead {
  private active = 0;
  private readonly line = new WaiterList<Turn>();
  private readonly maxQueued: number;

  constructor(private readonly options: BulkheadOptions) {
    if (!(options.maxConcurrent >= 1)) throw new RangeError("maxConcurrent must be at least 1");
    this.maxQueued = options.maxQueued ?? 0;
  }

  protect<A, S>(eff: Eff<A, S>): Eff<A, S | Throws<BulkheadFull>> {
    return suspend(() => {
      if (this.line.length === 0 && this.active < this.options.maxConcurrent) {
        return this.run(this.takeSlotNow(), eff);
      }
      if (this.line.length >= this.maxQueued) return fail(this.full("queue full"));
      // Join the line right now, not once the wait starts: a slot freed in
      // between must still come to this call.
      const turn = this.line.push(new Turn());
      const { maxWaitMs } = this.options;
      const wait =
        maxWaitMs === undefined
          ? this.waitFor(turn)
          : race([
              this.waitFor(turn),
              sleep(maxWaitMs).flatMap(() => fail(this.full("waited too long"))),
            ]);
      // Not getting the slot (too late, or interrupted) means leaving the
      // line, or passing on a slot that has arrived after all.
      const slot = onExit(wait, (exit) =>
        sync(() => {
          if (exit._tag === "Success") return;
          if (turn.granted) this.freeSlot();
          else this.line.remove(turn);
        }),
      );
      return this.run(slot, eff);
    }) as Eff<A, S | Throws<BulkheadFull>>;
  }

  get running(): Eff<number, never> {
    return sync(() => this.active);
  }

  get queued(): Eff<number, never> {
    return sync(() => this.line.length);
  }

  /** Run `eff` once `slot` is ours, and give the slot back when it ends. */
  private run<A, S, S2>(slot: Eff<void, S2>, eff: Eff<A, S>): Eff<A, S | S2> {
    return slot.flatMap(() =>
      ensuring(
        eff,
        sync(() => this.freeSlot()),
      ),
    ) as Eff<A, S | S2>;
  }

  // Slots are handed over through resume, so a fiber interrupted before it
  // starts running gives its slot back (see `async`).
  private takeSlotNow(): Eff<void, never> {
    return async<void>((resume) => {
      this.active += 1;
      resume(succeed(undefined) as never, () => this.freeSlot());
    }) as Eff<void, never>;
  }

  private waitFor(turn: Turn): Eff<void, never> {
    return async<void>((resume) => {
      if (turn.granted) {
        resume(succeed(undefined) as never);
        return;
      }
      turn.onGrant = () => resume(succeed(undefined) as never);
      return () => {
        turn.onGrant = undefined;
      };
    }) as Eff<void, never>;
  }

  /** Pass the slot on to the first call in line, or give it up. */
  private freeSlot(): void {
    const next = this.line.shift();
    if (next) next.grant();
    else this.active -= 1;
  }

  private full(reason: BulkheadFull["reason"]): BulkheadFull {
    return {
      _tag: "BulkheadFull",
      reason,
      maxConcurrent: this.options.maxConcurrent,
      maxQueued: this.maxQueued,
    };
  }
}

export const Bulkhead = {
  make(options: BulkheadOptions): Bulkhead {
    return new InProcessBulkhead(options);
  },
} as const;
