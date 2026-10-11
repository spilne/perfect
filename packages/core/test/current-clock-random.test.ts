import { describe, expect, test } from "bun:test";
import {
  Clock,
  Random,
  TestClock,
  TestRandom,
  currentClock,
  currentRandom,
  provide,
  realClock,
  realRandom,
  runSync,
  type Eff,
} from "../src";

describe("currentClock and currentRandom", () => {
  test("without anything provided they are the real ones", () => {
    expect(runSync(currentClock)).toBe(realClock);
    expect(runSync(currentRandom)).toBe(realRandom);
  });

  test("they are the provided ones when there are", () => {
    const clock = new TestClock();
    const random = new TestRandom(1);
    expect(runSync(provide(currentClock, Clock, clock))).toBe(clock);
    expect(runSync(provide(currentRandom, Random, random))).toBe(random);
  });

  test("they need no service in the type", () => {
    const needsNothing: Eff<number, never> = currentClock.flatMap((clock) =>
      currentRandom.flatMap((random) => random.nextInt(10).map((n) => n + clock.now())),
    );
    expect(typeof runSync(needsNothing)).toBe("number");
  });
});
