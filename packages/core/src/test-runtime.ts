// Every test service in one call, and a clock that a test can move forward
// without the usual dance of letting fibers reach their sleeps first.
//
//   const t = TestRuntime.make({ seed: 42 });
//   const result = await t.runAdvancing(retry(callApi, policy)); // no real waiting
//   t.logger.atLevel("warn"); t.console.output; t.clock.now();

import type { Eff, EffectCheck, Needs } from "./eff.js";
import { Clock, TestClock } from "./clock.js";
import { Random, TestRandom } from "./random.js";
import { Console, TestConsole } from "./console.js";
import { Logger, TestLogger } from "./logger.js";
import { Tracer, TestTracer } from "./tracing.js";
import { FileSystem, TestFileSystem } from "./filesystem.js";
import { ConfigProvider, TestConfigProvider } from "./config.js";
import { provide } from "./service.js";
import { runFiber } from "./runtime.js";
import type { Exit } from "./exit.js";
import { Cause } from "./cause.js";

export interface TestRuntimeOptions {
  /** TestClock start time, in ms. Default: 0. */
  readonly startTime?: number;
  /** TestRandom seed. Default: 42. */
  readonly seed?: number;
  /** Files of the TestFileSystem, by path. */
  readonly files?: Record<string, string | Uint8Array>;
  /** Settings of the TestConfigProvider. */
  readonly config?: Record<string, string | undefined>;
  /** Lines the TestConsole gives to `readLine`. */
  readonly input?: readonly string[];
}

/** The services a TestRuntime provides. */
type TestServices =
  | Needs<Clock, "Clock">
  | Needs<Random, "Random">
  | Needs<Console, "Console">
  | Needs<Logger, "Logger">
  | Needs<Tracer, "Tracer">
  | Needs<FileSystem, "FileSystem">
  | Needs<ConfigProvider, "ConfigProvider">;

export interface RunAdvancingOptions {
  /** Fail if the effect isn't done after this much test time. Default: one day. */
  readonly maxMs?: number;
  /**
   * Fail if, for this long in real time, the effect waits on something other
   * than the clock (a Deferred nobody completes, say). Default: 5000.
   */
  readonly maxIdleMs?: number;
}

// Let fibers run until they wait for something: a few turns of the event
// loop, since a step can start more steps.
const settle = async (): Promise<void> => {
  for (let i = 0; i < 3; i++) await new Promise((resolve) => setTimeout(resolve, 0));
};

export class TestRuntime {
  readonly clock: TestClock;
  readonly random: TestRandom;
  readonly console: TestConsole;
  readonly logger: TestLogger;
  readonly tracer: TestTracer;
  readonly files: TestFileSystem;
  readonly config: TestConfigProvider;

  private constructor(options: TestRuntimeOptions) {
    this.clock = new TestClock(options.startTime ?? 0);
    this.random = new TestRandom(options.seed ?? 42);
    this.console = new TestConsole(options.input ?? []);
    this.logger = new TestLogger();
    this.tracer = new TestTracer();
    this.files = new TestFileSystem(options.files ?? {});
    this.config = new TestConfigProvider(options.config ?? {});
  }

  static make(options: TestRuntimeOptions = {}): TestRuntime {
    return new TestRuntime(options);
  }

  /** `eff` with every test service provided. */
  provide<A, S>(eff: Eff<A, S>): Eff<A, Exclude<S, TestServices>> {
    let provided: Eff<A, unknown> = eff;
    provided = provide(provided, Clock, this.clock);
    provided = provide(provided, Random, this.random);
    provided = provide(provided, Console, this.console);
    provided = provide(provided, Logger, this.logger);
    provided = provide(provided, Tracer, this.tracer);
    provided = provide(provided, FileSystem, this.files);
    provided = provide(provided, ConfigProvider, this.config);
    return provided as Eff<A, Exclude<S, TestServices>>;
  }

  /** Run with the test services. Time only moves when the test moves it. */
  run<A, S>(eff: Eff<A, S> & EffectCheck<Exclude<S, TestServices>>): Promise<A> {
    return this.start(eff).then((exit) => {
      if (exit._tag === "Success") return exit.value;
      throw Cause.squash(exit.cause);
    });
  }

  /** Like `run`, resolving to the Exit instead of rejecting. */
  runExit<A, S>(eff: Eff<A, S>): Promise<Exit<unknown, A>> {
    return this.start(eff);
  }

  /**
   * Run with the test services, moving the clock straight to each next
   * sleep's end whenever everything is waiting, until the effect is done.
   * Code that sleeps, retries or times out finishes at once, in the same
   * order as with real time. Fails if it isn't done after `maxMs` of test
   * time, so an effect that never ends can't hang the test.
   */
  async runAdvancing<A, S>(
    eff: Eff<A, S> & EffectCheck<Exclude<S, TestServices>>,
    options: RunAdvancingOptions = {},
  ): Promise<A> {
    const maxMs = options.maxMs ?? 24 * 60 * 60 * 1000;
    const maxIdleMs = options.maxIdleMs ?? 5_000;
    const limit = this.clock.now() + maxMs;
    let done = false;
    const exit = this.start(eff).finally(() => (done = true));
    await settle();
    let idleSince = Date.now();
    while (!done) {
      const next = this.clock.pendingDeadlines()[0];
      if (next === undefined) {
        // Waiting for something other than the clock; give it a moment.
        if (Date.now() - idleSince > maxIdleMs) {
          throw new Error(
            `runAdvancing: waited ${maxIdleMs} ms (real time) for something other than the clock`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 1));
        continue;
      }
      if (next > limit) {
        throw new Error(`runAdvancing: not done after ${maxMs} ms of test time`);
      }
      this.clock.setTime(Math.max(next, this.clock.now()));
      await settle();
      idleSince = Date.now();
    }
    const result = await exit;
    if (result._tag === "Success") return result.value;
    throw Cause.squash(result.cause);
  }

  /** Move the clock forward by `ms`, letting fibers catch up before and after. */
  async advance(ms: number): Promise<void> {
    await settle();
    this.clock.advance(ms);
    await settle();
  }

  private start<A>(eff: Eff<A, unknown>): Promise<Exit<unknown, A>> {
    // The test services are provided, so all that is left is failing.
    return runFiber(this.provide(eff) as Eff<A, never>).await() as Promise<Exit<unknown, A>>;
  }
}
