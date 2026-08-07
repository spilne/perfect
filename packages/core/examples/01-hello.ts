// The smallest possible Eff program: build a value, run it, assert.
//
// Run: bun packages/core/examples/01-hello.ts

import { eff, succeed } from "../src";
import { assertEq } from "./_assert";

// >>> example: hello-sync
// runSync — for purely synchronous programs (no Async, no Sleep, no Fork).
const greet = succeed("hello, perfect");
assertEq(greet.runSync(), "hello, perfect");
// <<< example

// >>> example: hello-generator
// The recommended syntax: write effects in generator form, use yield* to
// extract values. No build step required.
const program = eff(function* () {
  const a = yield* succeed(21);
  const b = yield* succeed(2);
  return a * b;
});

assertEq(await program.run(), 42);
// <<< example

// >>> example: hello-flatmap
// The composed-flatMap form — fastest, but reads bottom-up for long chains.
const composed = succeed(21)
  .flatMap((a) => succeed(a * 2))
  .map((b) => b + 0);

assertEq(composed.runSync(), 42);
// <<< example
