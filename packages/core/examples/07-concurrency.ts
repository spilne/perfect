// Concurrency primitives: fork, race, all.
//
// Run: bun packages/core/examples/07-concurrency.ts

import { succeed, sleep, join, race, all, run } from "../src";
import { assertEq } from "./_assert";

// >>> example: fork-join
// .fork() spawns a fiber. join() awaits its result.
const forkExample = sleep(10)
  .flatMap(() => succeed(42))
  .fork()
  .flatMap((fiber) => join(fiber));

assertEq(await forkExample.run(), 42);
// <<< example

// >>> example: race-method
// .race(other) — fluent two-way race. First to succeed wins.
const fast = sleep(10).flatMap(() => succeed("fast"));
const slow = sleep(50).flatMap(() => succeed("slow"));

assertEq(await fast.race(slow).run(), "fast");
// <<< example

// >>> example: race-variadic
// race([...]) — variadic form for 3+ effects.
const winner = await race([
  sleep(30).flatMap(() => succeed("a")),
  sleep(10).flatMap(() => succeed("b")),
  sleep(20).flatMap(() => succeed("c")),
]).run();
assertEq(winner, "b");
// <<< example

// >>> example: all-parallel
// all() runs effects in parallel and collects their results.
const results = await all([
  sleep(10).flatMap(() => succeed("a")),
  sleep(20).flatMap(() => succeed("b")),
  sleep(30).flatMap(() => succeed("c")),
]).run();

assertEq(results, ["a", "b", "c"]);
// <<< example

// >>> example: all-object
// all() also accepts an object — destructure named results.
const { user, posts, friends } = await all({
  user: sleep(10).flatMap(() => succeed({ id: 7, name: "alice" })),
  posts: sleep(20).flatMap(() => succeed([{ id: 1 }, { id: 2 }])),
  friends: sleep(15).flatMap(() => succeed(["bob", "carol"])),
}).run();

assertEq(user, { id: 7, name: "alice" });
assertEq(posts, [{ id: 1 }, { id: 2 }]);
assertEq(friends, ["bob", "carol"]);
// <<< example
