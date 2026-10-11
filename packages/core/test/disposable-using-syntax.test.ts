// The `await using` syntax with runDisposable. Kept apart because Node 22
// can't parse it (see EXCLUDED_FROM_NODE_CI in scripts/node-bun-test-run.mjs).

import { expect, test } from "bun:test";
import { acquireRelease, runDisposable, sync } from "../src";

test("await using releases when the block ends, also when it throws", async () => {
  const events: string[] = [];
  const resource = acquireRelease(
    sync(() => {
      events.push("open");
      return "connection";
    }),
    () => sync(() => void events.push("close")),
  );

  {
    await using held = await runDisposable(resource);
    events.push(`use ${held.value}`);
  }
  expect(events).toEqual(["open", "use connection", "close"]);

  events.length = 0;
  const failing = async () => {
    await using held = await runDisposable(resource);
    events.push(`use ${held.value}`);
    throw new Error("boom");
  };
  await expect(failing()).rejects.toThrow("boom");
  expect(events).toEqual(["open", "use connection", "close"]);
});
