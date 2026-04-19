// Resource management: acquireRelease, scoped, .ensuring, .onExit.
// Releases run on success, failure, AND interruption — guaranteed.
//
// Run: bun packages/core/examples/08-resources.ts

import {
  eff, succeed, fail, sync, acquireRelease, scoped, run, type Eff, type Throws,
} from "../src";
import { assertEq } from "./_assert";

// >>> example: acquire-release
// .acquireRelease(release) — fluent, pair an acquire with cleanup.
// .scoped() — define when the cleanup fires (the scope boundary).
const events: string[] = [];
const useFile = sync(() => {
  events.push("opened");
  return { read: () => "contents" };
})
  .acquireRelease(() => sync(() => { events.push("closed"); }))
  .flatMap((file) => sync(() => file.read()))
  .scoped();

assertEq(await run(useFile), "contents");
assertEq(events, ["opened", "closed"]);
// <<< example

// >>> example: ensuring
// .ensuring(finalizer) — fluent try/finally for any effect.
let cleanedUp = false;
const tracked = succeed("done")
  .ensuring(sync(() => { cleanedUp = true; }));

assertEq(await run(tracked), "done");
assertEq(cleanedUp, true);
// <<< example

// >>> example: on-exit
// .onExit(handler) — inspect Exit, then propagate the original outcome.
let exitTag = "";
const observed = succeed("ok")
  .onExit((exit) => sync(() => { exitTag = exit._tag; }));

assertEq(await run(observed), "ok");
assertEq(exitTag, "Success");
// <<< example

// >>> example: release-on-failure
// Release fires even when the inner effect fails.
const trace: string[] = [];
const safe = scoped(
  eff(function* () {
    yield* acquireRelease(
      sync(() => trace.push("acquire")),
      () => sync(() => { trace.push("release"); }),
    );
    yield* (fail("crashed") as Eff<never, Throws<string>>);
    return "unreachable";
  }) as any,
).catch((e: any) => succeed(`recovered: ${e}`));

assertEq(await run(safe as any), "recovered: crashed");
assertEq(trace, ["acquire", "release"]);
// <<< example
