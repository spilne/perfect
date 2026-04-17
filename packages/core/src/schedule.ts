import { type Eff, type Throws, Suspend, Op } from "./eff";
import { succeed, fail, sync, sleep, suspend } from "./constructors";

// A Schedule<In, Out> decides whether to continue and what delay to use.
// Each step receives the input and returns either a delay + output, or done.
export interface Schedule<In = unknown, Out = unknown> {
  readonly step: (input: In, state: any) => ScheduleDecision<Out>;
  readonly initial: any;
}

export type ScheduleDecision<Out> =
  | { readonly _tag: "Continue"; readonly delay: number; readonly output: Out; readonly state: any }
  | { readonly _tag: "Done"; readonly output: Out };

// Counting schedule — forever-running, state counts attempts from 0, delay
// computed from the current state. Basis for spaced/exponential/linear/forever.
function counting(delay: (state: number) => number): Schedule<unknown, number> {
  return {
    initial: 0,
    step: (_input, state: number) => ({
      _tag: "Continue",
      delay: delay(state),
      output: state,
      state: state + 1,
    }),
  };
}

export const Schedule = {
  // ── Constructors ───────────────────────────────────────────────

  forever: counting(() => 0) satisfies Schedule<unknown, number>,

  recurs(n: number): Schedule<unknown, number> {
    return {
      initial: 0,
      step: (_input, state: number) => {
        if (state >= n) return { _tag: "Done", output: state };
        return { _tag: "Continue", delay: 0, output: state, state: state + 1 };
      },
    };
  },

  once: {
    initial: false,
    step: (_input: unknown, state: boolean) => {
      if (state) return { _tag: "Done" as const, output: undefined };
      return { _tag: "Continue" as const, delay: 0, output: undefined, state: true };
    },
  } satisfies Schedule<unknown, undefined>,

  spaced(ms: number): Schedule<unknown, number> {
    return counting(() => ms);
  },

  exponential(base: number, factor = 2): Schedule<unknown, number> {
    return counting((state) => base * Math.pow(factor, state));
  },

  fixed(ms: number): Schedule<unknown, number> {
    return Schedule.spaced(ms);
  },

  linear(base: number): Schedule<unknown, number> {
    return counting((state) => base * (state + 1));
  },

  // ── Combinators ────────────────────────────────────────────────

  intersect<In, Out1, Out2>(
    s1: Schedule<In, Out1>,
    s2: Schedule<In, Out2>,
  ): Schedule<In, [Out1, Out2]> {
    return {
      initial: [s1.initial, s2.initial],
      step: (input, [state1, state2]: [any, any]) => {
        const d1 = s1.step(input, state1);
        const d2 = s2.step(input, state2);
        if (d1._tag === "Done" || d2._tag === "Done") {
          return { _tag: "Done", output: [d1.output, d2.output] as [Out1, Out2] };
        }
        return {
          _tag: "Continue",
          delay: Math.max(d1.delay, d2.delay),
          output: [d1.output, d2.output] as [Out1, Out2],
          state: [d1.state, d2.state],
        };
      },
    };
  },

  union<In, Out1, Out2>(
    s1: Schedule<In, Out1>,
    s2: Schedule<In, Out2>,
  ): Schedule<In, [Out1, Out2]> {
    return {
      initial: [s1.initial, s2.initial],
      step: (input, [state1, state2]: [any, any]) => {
        const d1 = s1.step(input, state1);
        const d2 = s2.step(input, state2);
        if (d1._tag === "Done" && d2._tag === "Done") {
          return { _tag: "Done", output: [d1.output, d2.output] as [Out1, Out2] };
        }
        const delay1 = d1._tag === "Continue" ? d1.delay : Infinity;
        const delay2 = d2._tag === "Continue" ? d2.delay : Infinity;
        return {
          _tag: "Continue",
          delay: Math.min(delay1, delay2),
          output: [d1.output, d2.output] as [Out1, Out2],
          state: [
            d1._tag === "Continue" ? d1.state : state1,
            d2._tag === "Continue" ? d2.state : state2,
          ],
        };
      },
    };
  },

  jittered<In, Out>(schedule: Schedule<In, Out>, min = 0.8, max = 1.2): Schedule<In, Out> {
    return {
      initial: schedule.initial,
      step: (input, state) => {
        const decision = schedule.step(input, state);
        if (decision._tag === "Done") return decision;
        const jitter = min + Math.random() * (max - min);
        return { ...decision, delay: Math.round(decision.delay * jitter) };
      },
    };
  },

  map<In, Out, Out2>(schedule: Schedule<In, Out>, f: (out: Out) => Out2): Schedule<In, Out2> {
    return {
      initial: schedule.initial,
      step: (input, state) => {
        const decision = schedule.step(input, state);
        return { ...decision, output: f(decision.output) };
      },
    };
  },

  whileInput<In, Out>(
    schedule: Schedule<In, Out>,
    pred: (input: In) => boolean,
  ): Schedule<In, Out> {
    return {
      initial: schedule.initial,
      step: (input, state) => {
        if (!pred(input)) return { _tag: "Done", output: schedule.step(input, state).output };
        return schedule.step(input, state);
      },
    };
  },

  whileOutput<In, Out>(
    schedule: Schedule<In, Out>,
    pred: (output: Out) => boolean,
  ): Schedule<In, Out> {
    return {
      initial: schedule.initial,
      step: (input, state) => {
        const decision = schedule.step(input, state);
        if (!pred(decision.output)) return { _tag: "Done", output: decision.output };
        return decision;
      },
    };
  },

  maxDelay<In, Out>(schedule: Schedule<In, Out>, max: number): Schedule<In, Out> {
    return {
      initial: schedule.initial,
      step: (input, state) => {
        const decision = schedule.step(input, state);
        if (decision._tag === "Done") return decision;
        return { ...decision, delay: Math.min(decision.delay, max) };
      },
    };
  },

  // Fibonacci backoff — delays are base * fib(attempt).
  // Smoother than exponential in practice (1, 1, 2, 3, 5, 8, 13, ...).
  fibonacci(base: number): Schedule<unknown, number> {
    return {
      initial: [0, 1] as [number, number],
      step: (_input, state: [number, number]) => {
        const [prev, curr] = state;
        return {
          _tag: "Continue",
          delay: base * curr,
          output: curr,
          state: [curr, prev + curr] as [number, number],
        };
      },
    };
  },

  // Full jitter: each delay is random in [0, scheduled_delay].
  // Better anti-thundering-herd profile than equal jitter (±factor).
  // Reference: AWS Architecture Blog on retries with jitter.
  fullJitter<In, Out>(schedule: Schedule<In, Out>): Schedule<In, Out> {
    return {
      initial: schedule.initial,
      step: (input, state) => {
        const decision = schedule.step(input, state);
        if (decision._tag === "Done") return decision;
        return { ...decision, delay: Math.round(Math.random() * decision.delay) };
      },
    };
  },

  // Transform the schedule's output to be the cumulative delay so far.
  // Useful to compose with whileOutput: `whileOutput(cumulativeDelay(s), ms => ms < budget)`
  // gives you a time-bounded schedule.
  cumulativeDelay<In, Out>(schedule: Schedule<In, Out>): Schedule<In, number> {
    return {
      initial: { inner: schedule.initial, total: 0 },
      step: (input, state: { inner: any; total: number }) => {
        const decision = schedule.step(input, state.inner);
        if (decision._tag === "Done") return { _tag: "Done", output: state.total };
        const total = state.total + decision.delay;
        return {
          _tag: "Continue",
          delay: decision.delay,
          output: total,
          state: { inner: decision.state, total },
        };
      },
    };
  },
};

// ── retry / repeat using Schedule ──────────────────────────────────

export interface RetryDetails<In = unknown, Out = unknown> {
  readonly attempts: number;
  readonly upcomingDelayMs: number;
  readonly input: In;
  readonly output: Out;
  readonly givingUp: boolean;
}

export function retryWith<A, S, In>(
  eff: Eff<A, S>,
  schedule: Schedule<In, any>,
  opts: {
    toInput?: (cause: any) => In;
    onRetry?: (details: RetryDetails<In, any>) => Eff<void, any>;
  } = {},
): Eff<A, S> {
  const { toInput, onRetry } = opts;
  function loop(state: any, attempts: number): Eff<A, S> {
    return new Suspend(Op.CatchAll, eff, (cause: any) => {
      const input = toInput
        ? toInput(cause)
        : ((cause._tag === "Fail" ? cause.error : cause) as any);
      const decision = schedule.step(input, state);
      if (decision._tag === "Done") {
        if (onRetry) {
          const details: RetryDetails<In, any> = {
            attempts: attempts + 1,
            upcomingDelayMs: 0,
            input,
            output: decision.output,
            givingUp: true,
          };
          return (onRetry(details) as any).flatMap(() => new Suspend(Op.Fail, cause, null));
        }
        return new Suspend(Op.Fail, cause, null);
      }
      const next = () => {
        const wait: Eff<void, never> =
          decision.delay > 0 ? (sleep(decision.delay) as any) : (succeed(undefined) as any);
        return (wait as any).flatMap(() => loop(decision.state, attempts + 1));
      };
      if (onRetry) {
        const details: RetryDetails<In, any> = {
          attempts: attempts + 1,
          upcomingDelayMs: decision.delay,
          input,
          output: decision.output,
          givingUp: false,
        };
        return (onRetry(details) as any).flatMap(next);
      }
      return next();
    }) as any;
  }
  return loop(schedule.initial, 0);
}

export function repeat<A, S>(eff: Eff<A, S>, schedule: Schedule<A, any>): Eff<A, S> {
  function loop(state: any): Eff<A, S> {
    return (eff as any).flatMap((value: A) => {
      const decision = schedule.step(value, state);
      if (decision._tag === "Done") return succeed(value);
      if (decision.delay > 0) {
        return (sleep(decision.delay) as any).flatMap(() => loop(decision.state));
      }
      return suspend(() => loop(decision.state));
    }) as any;
  }
  return loop(schedule.initial);
}
