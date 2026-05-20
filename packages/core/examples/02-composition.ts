// Composing effects: flatMap, map, tap, zip.
//
// Run: bun packages/core/examples/02-composition.ts

import { succeed, sync, runSync } from "../src";
import { assertEq } from "./_assert";

// >>> example: map-and-flatmap
// .map — transform the value (synchronous)
// .flatMap — chain another effect that depends on the value
const program = succeed(10)
  .map((x) => x + 1) // 11
  .flatMap((x) => sync(() => x * 2)) // 22
  .map((x) => x.toString()); // "22"

assertEq(program.runSync(), "22");
// <<< example

// >>> example: tap
// .tap — observe a value without changing it (returns the same value)
let seen = 0;
const traced = succeed(42)
  .tap((x) => { seen = x; })
  .map((x) => x + 1);

assertEq(traced.runSync(), 43);
assertEq(seen, 42);
// <<< example

// >>> example: zip
// .zip — combine two effects sequentially into a tuple
const pair = succeed("hello").zip(succeed("world"));
assertEq(pair.runSync(), ["hello", "world"]);
// <<< example
