// eff(function*) — runtime generator syntax for effect pipelines.
//
// Run: bun packages/core/examples/03-generator-syntax.ts

import { eff, succeed, fail, sync, type Eff, type Throws } from "../src";
import { assertEq } from "./_assert";

// >>> example: gen-basic
// Use yield* to extract values from effects. Looks like async/await.
const program = eff(function* () {
  const a = yield* succeed(10);
  const b = yield* succeed(20);
  const c = yield* sync(() => a + b);
  return c * 2;
});

assertEq(program.runSync(), 60);
// <<< example

// >>> example: gen-trycatch
// try/catch inside the generator catches typed failures (and defects).
const safe = eff(function* () {
  try {
    yield* fail("boom") as Eff<never, Throws<string>>;
    return "unreachable";
  } catch (e) {
    return `caught: ${e}`;
  }
});

assertEq(await (safe as any).run(), "caught: boom");
// <<< example

// >>> example: gen-flatten
// Returning another effect from the generator flattens it.
const inner = eff(function* () {
  return yield* succeed(7);
});

const outer = eff(function* () {
  // No yield* here — return an Eff and it gets flattened
  return inner;
});

assertEq(outer.runSync(), 7);
// <<< example
