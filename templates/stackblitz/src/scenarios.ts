export interface PlaygroundScenario {
  readonly id: string;
  readonly title: string;
  readonly description: string;
  readonly source: string;
}

export const scenarios: readonly PlaygroundScenario[] = [
  {
    id: "concurrency",
    title: "Concurrent map",
    description: "Evaluate twelve jobs on four fibers while retaining input order.",
    source: `return Stream.range(1, 13).parEvalMap(4, (input) =>
  delay(40 + (input % 4) * 20, {
    input,
    square: input * input,
  }),
);`,
  },
  {
    id: "typed-errors",
    title: "Typed failures",
    description: "Materialize a terminal typed error as a Left value without throwing.",
    source: `return Stream.of(12, 6, 0, 3)
  .rechunk(1)
  .evalMap((divisor) =>
    divisor === 0
      ? fail({ _tag: "DivideByZero", dividend: 12 })
      : succeed({ divisor, quotient: 12 / divisor }),
  )
  .either();`,
  },
  {
    id: "retry",
    title: "Source retry",
    description: "Finalize and reacquire a flaky source until its third attempt succeeds.",
    source: `let attempts = 0;

return Stream.retryFrom(
  () => {
    attempts += 1;
    return attempts < 3
      ? Stream.fail({ _tag: "ConnectionLost", attempt: attempts })
      : Stream.of({ status: "connected", attempt: attempts });
  },
  RetryPolicy.exponential({ initial: 60, factor: 2 }).withMaxRetries(3),
);`,
  },
  {
    id: "state",
    title: "Stateful scan",
    description: "Carry count and total state through a stream of observations.",
    source: `return Stream.of(3, 1, 4, 1, 5, 9).scan(
  { count: 0, total: 0 },
  (state, value) => ({
    count: state.count + 1,
    total: state.total + value,
  }),
);`,
  },
  {
    id: "cancellation",
    title: "Take until",
    description: "Stop a ticking source when a second stream emits its first value.",
    source: `const source = Stream.tick(70).scan(
  0,
  (count) => count + 1,
);
const stop = Stream.tick(330);

return source.takeUntil(stop);`,
  },
  {
    id: "observe",
    title: "Single-pass observe",
    description: "Batch a reliable side branch without subscribing to the source twice.",
    source: `const batches = [];
const values = await Stream.range(1, 10)
  .observe((stream) =>
    stream.grouped(3).tap((chunk) => {
      batches.push(Array.from(chunk));
    }),
  )
  .toArray()
  .run();

return { values, batches };`,
  },
  {
    id: "switch-map",
    title: "Switch to latest",
    description: "Cancel stale inner work whenever the outer stream produces a newer request.",
    source: `const requests = Stream.tick(60)
  .scan(0, (request) => request + 1)
  .take(3);

return requests.switchMap((request) =>
  Stream.tick(40)
    .scan(0, (step) => step + 1)
    .take(3)
    .map((step) => ({ request, step })),
);`,
  },
  {
    id: "combine-latest",
    title: "Combine latest",
    description: "React whenever either input changes after both streams have produced a value.",
    source: `const temperature = Stream.tick(80)
  .scan(19, (value) => value + 1)
  .take(2);
const humidity = Stream.tick(50)
  .scan(39, (value) => value + 1)
  .take(3);

return temperature
  .combineLatest(humidity)
  .map(([celsius, percent]) => ({ celsius, percent }));`,
  },
];
