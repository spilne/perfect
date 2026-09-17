import type { Eff } from "../eff";
import { failCause, succeed } from "../constructors";
import { Cause } from "../cause";
import { Queue } from "../queue";
import type { Chunk } from "./chunk";
import type { Stream, Step } from "./stream";
import { combineFinalizers, driverStream } from "./driver-lifecycle";

type MergeEvent<A> =
  | { _tag: "chunk"; chunk: Chunk<A> }
  | { _tag: "end" }
  | { _tag: "fail"; cause: Cause };

function publishSource<A>(params: {
  events: Queue<MergeEvent<A>>;
  source: Stream<A, unknown>;
}): Eff<unknown, unknown> {
  const { events, source } = params;
  return source.step
    .flatMap((step): Eff<unknown, unknown> =>
      step._tag === "Done"
        ? events.offer({ _tag: "end" })
        : events
            .offer({ _tag: "chunk", chunk: step.chunk })
            .flatMap(() => publishSource({ events, source: step.next })),
    )
    .catchAllCause((cause) =>
      Cause.isInterruptedOnly(cause) ? failCause(cause) : events.offer({ _tag: "fail", cause }),
    );
}

export function mergeStreams<A, S, S2>(params: {
  left: Stream<A, S>;
  right: Stream<A, S2>;
}): Stream<A, S | S2> {
  const { left, right } = params;
  // One bounded queue provides backpressure to both producers. The run stops
  // and awaits the drivers before either source is released.
  return driverStream<A>({
    start: (run) =>
      Queue.bounded<MergeEvent<A>>(2).flatMap((events) => {
        let remainingSources = 2;
        const pull = (): Eff<Step<A>, unknown> =>
          events.take().flatMap((event): Eff<Step<A>, unknown> => {
            switch (event._tag) {
              case "fail":
                return failCause(event.cause);
              case "end":
                remainingSources--;
                return remainingSources === 0 ? succeed({ _tag: "Done" }) : pull();
              case "chunk":
                return succeed({ _tag: "Emit", chunk: event.chunk, next });
            }
          });
        const next = run.continueWith(pull);
        return run
          .fork(publishSource({ events, source: left }))
          .flatMap(() => run.fork(publishSource({ events, source: right })))
          .map(() => pull);
      }),
    // Step continuations erase source requirements. Only this boundary
    // restores the union supplied by the two inputs; queue failures remain
    // internal.
    finalizer: combineFinalizers(left._finalizer, right._finalizer),
  }) as Stream<A, S | S2>;
}
