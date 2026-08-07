// Errors flow through the type as Throws<E>. Catch them with .catch /
// .catchTag / .catchAllCause; remove them from the type by handling them.
//
// Run: bun packages/core/examples/06-error-handling.ts

import { succeed, fail, sync, type Eff, type Throws } from "../src";
import { assertEq } from "./_assert";

// >>> example: catch-typed
// .catch handles any typed failure, removing Throws<E> from the type.
const program: Eff<string, never> = (fail("nope") as Eff<never, Throws<string>>).catch((e) =>
  succeed(`recovered: ${e}`),
);

assertEq(program.runSync(), "recovered: nope");
// <<< example

// >>> example: catch-tag
// .catchTag — handle one specific tagged error variant.
type Err = { _tag: "NotFound"; id: number } | { _tag: "Forbidden" };

const lookup = (id: number): Eff<string, Throws<Err>> =>
  id === 1 ? succeed("alice") : (fail({ _tag: "NotFound", id }) as Eff<never, Throws<Err>>);

const safe = lookup(99)
  .catchTag("NotFound", (e) => succeed(`(missing ${e.id})`))
  .catchTag("Forbidden", () => succeed("(no access)"));

assertEq(safe.runSync(), "(missing 99)");
// <<< example

// >>> example: catch-cause
// .catchAllCause — see the full Cause (Fail | Die | Interrupt | composites).
const wild = (fail("boom") as Eff<never, Throws<string>>).catchAllCause((cause) =>
  succeed(`cause: ${cause._tag}`),
);

assertEq(wild.runSync(), "cause: Fail");
// <<< example

// >>> example: tap-error
// .tapError — observe a typed failure without handling it (re-fails).
let observedError: string | null = null;
const observed = (fail("bad") as Eff<never, Throws<string>>)
  .tapError(
    (e) =>
      sync(() => {
        observedError = e;
      }) as any,
  )
  .catch(() => succeed("ok"));

assertEq(observed.runSync(), "ok");
assertEq(observedError, "bad");
// <<< example

// >>> example: orelse
// .orElse — if this effect fails, run another.
const fallback = (fail("first") as Eff<never, Throws<string>>).orElse(() => succeed("second"));
assertEq(await fallback.run(), "second");
// <<< example
