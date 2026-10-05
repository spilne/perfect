// The basic building blocks: Queue, Semaphore, Ref, Deferred, and the
// cached / cachedBy memoizers.
//
// Run: bun packages/core/examples/16-basic-primitives.ts

import {
  eff,
  sync,
  sleep,
  fork,
  join,
  all,
  cached,
  cachedBy,
  Queue,
  Semaphore,
  Ref,
  Deferred,
  RequestResolver,
} from "../src";
import { assertEq } from "./_assert";

// >>> example: queue
// A bounded queue: offer waits while the queue is full, take waits while it
// is empty. close() means "no more values"; takers then fail with QueueClosed.
const received = await eff(function* () {
  const queue = yield* Queue.bounded<number>(2);

  const producer = yield* fork(
    eff(function* () {
      for (const n of [1, 2, 3, 4]) yield* queue.offer(n);
      yield* queue.close();
    }),
  );

  const got: number[] = [];
  for (let i = 0; i < 4; i++) got.push(yield* queue.take());
  yield* join(producer);
  return got;
})
  .orDie()
  .run();
assertEq(received, [1, 2, 3, 4]);
// <<< example

// >>> example: semaphore
// At most 2 tasks run at the same time; the others wait for a permit.
let running = 0;
let mostAtOnce = 0;
const task = (ms: number) =>
  sync(() => {
    running++;
    mostAtOnce = Math.max(mostAtOnce, running);
  })
    .flatMap(() => sleep(ms))
    .flatMap(() => sync(() => void running--));

await eff(function* () {
  const permits = yield* Semaphore.make(2);
  yield* all([10, 10, 10, 10].map((ms) => permits.withPermit(task(ms))));
}).run();
assertEq(mostAtOnce, 2);
// <<< example

// >>> example: ref
// A Ref is a mutable cell that fibers can share safely.
const finalCount = await eff(function* () {
  const counter = yield* Ref.make(0);
  yield* all(Array.from({ length: 100 }, () => counter.update((n) => n + 1)));
  // modify returns a value and sets a new state in one step
  const before = yield* counter.modify((n) => [n, 0] as [number, number]);
  return [before, yield* counter.get];
}).run();
assertEq(finalCount, [100, 0]);
// <<< example

// >>> example: deferred
// A Deferred is a value that will be set once, later. Fibers that await it
// wait until someone sets it.
const answer = await eff(function* () {
  const ready = yield* Deferred.make<string>();
  const waiter = yield* fork(ready.await);
  yield* sleep(10);
  yield* ready.succeed("done"); // returns false if it was already set
  return yield* join(waiter);
}).run();
assertEq(answer, "done");
// <<< example

// >>> example: cached
// cached(eff) runs eff once and remembers the result. Failures are not
// remembered, so a failed run is tried again next time. Callers that ask
// at the same time share one run.
let loads = 0;
const config = cached(
  sync(() => {
    loads++;
    return { region: "eu" };
  }),
  { ttlMs: 60_000 }, // optional: forget the value after a minute
);

await all([config, config, config]).run();
assertEq(loads, 1);

await config.invalidate.run(); // forget it now
await config.run();
assertEq(loads, 2);
// <<< example

// >>> example: cached-by
// cachedBy keeps one cached value per key. maxSize drops the least recently
// used key when the cache is full.
const fetched: string[] = [];
const users = cachedBy(
  (id: string) =>
    sync(() => {
      fetched.push(id);
      return { id, name: `user ${id}` };
    }),
  { ttlMs: 30_000, maxSize: 1_000 },
);

await users.get("a").run();
await users.get("a").run();
await users.get("b").run();
assertEq(fetched, ["a", "b"]);
// <<< example

// >>> example: request-resolver
// Lookups made together become one load: here three orders need their
// customers, and the load runs once, for the two distinct ids.
const queries: string[][] = [];
const CustomerById = RequestResolver.make({
  load: (ids: readonly string[]) =>
    sync(() => {
      queries.push([...ids]); // e.g. select * from customers where id = any($1)
      return new Map(ids.map((id) => [id, { id, name: `customer ${id}` }] as const));
    }),
});

const orders = [{ customer: "c1" }, { customer: "c2" }, { customer: "c1" }];
const customers = await all(orders.map((order) => CustomerById.get(order.customer))).run();
assertEq(
  customers.map((customer) => customer?.name),
  ["customer c1", "customer c2", "customer c1"],
);
assertEq(queries, [["c1", "c2"]]);
// <<< example
