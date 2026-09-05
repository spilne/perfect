import type { Eff } from "../eff";
import { failCause, fork, succeed, suspend } from "../constructors";
import { Cause } from "../cause";
import type { Fiber } from "../fiber";
import { Queue } from "../queue";
import type { Chunk } from "./chunk";
import { Stream, type Step } from "./stream";
import { combineFinalizers, interruptAllEff } from "./driver-lifecycle";

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

function consumeEvents<A>(params: {
  events: Queue<MergeEvent<A>>;
  remainingSources: { count: number };
}): Eff<Step<A>, unknown> {
  const { events, remainingSources } = params;
  return events.take().flatMap((event): Eff<Step<A>, unknown> => {
    switch (event._tag) {
      case "fail":
        return failCause(event.cause);
      case "end":
        remainingSources.count--;
        return remainingSources.count === 0 ? succeed({ _tag: "Done" }) : consumeEvents(params);
      case "chunk":
        return succeed({
          _tag: "Emit",
          chunk: event.chunk,
          next: new Stream(suspend(() => consumeEvents(params))),
        });
    }
  });
}

export function mergeStreams<A, S, S2>(params: {
  left: Stream<A, S>;
  right: Stream<A, S2>;
}): Stream<A, S | S2> {
  const { left, right } = params;
  const drivers: Fiber<unknown>[] = [];
  // One bounded queue provides backpressure to both producers. Interrupt and
  // await the drivers before releasing either source on early termination.
  const setup = Queue.bounded<MergeEvent<A>>(2).flatMap((events) =>
    fork(publishSource({ events, source: left })).flatMap((leftDriver) =>
      fork(publishSource({ events, source: right })).flatMap((rightDriver) => {
        drivers.push(leftDriver, rightDriver);
        return consumeEvents({ events, remainingSources: { count: 2 } });
      }),
    ),
  );
  // Step continuations erase source requirements. Only this boundary restores
  // the union supplied by the two inputs; queue failures remain internal.
  return new Stream(
    suspend(() => setup) as Eff<Step<A>, S | S2>,
    combineFinalizers(
      interruptAllEff(drivers),
      combineFinalizers(left._finalizer, right._finalizer),
    ),
  );
}
