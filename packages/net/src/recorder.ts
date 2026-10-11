// Record a conversation once, test against it from then on.
//
//   const { connection, transcript } = yield* Recorder.wrap(realConnection);
//   ... use `connection` as usual ...
//   save(yield* transcript);
//
//   // Later, in a test, with no server:
//   const replay = yield* Replay.of(saved);
//   yield* client(replay.connection);   // gets the recorded answers
//   yield* replay.verify;               // and sent exactly what was recorded
//
// The replay keeps the recorded order: a message the other side sent after
// one of ours is handed over only once the code under test has sent that
// one. A send that differs from the recording fails, and `verify` says where.

import { isDeepStrictEqual } from "node:util";
import {
  TaggedError,
  async,
  currentClock,
  fail,
  succeed,
  suspend,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import { Stream } from "@spilne/perfect-core/stream";
import type { Connection } from "./connection.js";
import { ConnectionError, type NetError } from "./errors.js";

/** One message of a conversation, seen from the recorded side. */
export interface Recorded<M> {
  readonly direction: "sent" | "received";
  /** When it was sent, or taken from `messages`, by the Clock in use. */
  readonly atMs: number;
  readonly message: M;
}

/** A conversation, in order. Plain data: save it as JSON if the messages are. */
export type Transcript<M> = readonly Recorded<M>[];

/** The replayed conversation went differently from the recording. */
export class ReplayMismatch extends TaggedError("ReplayMismatch")<{
  readonly message: string;
  /** Position in the transcript where it went wrong. */
  readonly at: number;
}>() {}

export const Recorder = {
  /**
   * A connection that behaves exactly like `connection` and writes down
   * every message sent and received. Received messages are written down
   * when they are taken from `messages`.
   */
  wrap<M>(
    connection: Connection<M>,
  ): Eff<{ connection: Connection<M>; transcript: Eff<Transcript<M>, never> }, never> {
    return currentClock.map((clock) => {
      const recorded: Recorded<M>[] = [];
      const note = (direction: Recorded<M>["direction"], message: M) =>
        recorded.push({ direction, atMs: clock.now(), message });
      return {
        connection: {
          remote: connection.remote,
          send: (message) => connection.send(message).map(() => void note("sent", message)),
          messages: connection.messages.tap((message) => void note("received", message)),
          close: connection.close,
        },
        transcript: sync(() => [...recorded]),
      };
    });
  },
} as const;

export interface ReplayOptions<M> {
  /** Whether a sent message matches the recorded one. Default: deep equality. */
  readonly equals?: (sent: M, recorded: M) => boolean;
}

export interface Replay<M> {
  /** Plays the other side of the recording. */
  readonly connection: Connection<M>;
  /** Fails unless every recorded message was sent and received, in order. */
  readonly verify: Eff<void, Throws<ReplayMismatch>>;
}

export const Replay = {
  /** A fake connection that plays back the other side of `transcript`. */
  of<M>(transcript: Transcript<M>, options: ReplayOptions<M> = {}): Eff<Replay<M>, never> {
    return sync(() => {
      const equals = options.equals ?? isDeepStrictEqual;
      // How far the conversation has got.
      let next = 0;
      let mismatch: ReplayMismatch | undefined;
      let closed = false;
      // Readers waiting for their turn; woken whenever `next` moves.
      const waiting = new Set<() => void>();
      const advance = () => {
        next++;
        const wake = [...waiting];
        waiting.clear();
        for (const resume of wake) resume();
      };
      const broken = (message: string) => new ConnectionError({ address: "replay", message });

      const send = (message: M): Eff<void, Throws<NetError>> =>
        suspend(() => {
          if (mismatch) return fail(broken(mismatch.message));
          if (closed) return fail(broken("the connection is closed"));
          const expected = transcript[next];
          const problem =
            expected === undefined
              ? `sent ${show(message)} after the recording ended`
              : expected.direction === "received"
                ? `sent ${show(message)} while the recording expects to receive ${show(expected.message)} first`
                : equals(message, expected.message)
                  ? undefined
                  : `sent ${show(message)}, but the recording sent ${show(expected.message)}`;
          if (problem) {
            mismatch = new ReplayMismatch({ message: problem, at: next });
            return fail(broken(problem));
          }
          advance();
          return succeed(undefined);
        });

      // The next recorded message to receive, once it is its turn; undefined
      // when the recording is over (the other side closed).
      const receive: Eff<M | undefined, never> = suspend(() => {
        if (closed || mismatch || next >= transcript.length) return succeed(undefined);
        const entry = transcript[next]!;
        if (entry.direction === "received") {
          advance();
          return succeed(entry.message);
        }
        // Our turn to send first.
        return async<void, never>((resume) => {
          const wake = () => resume(succeed(undefined));
          waiting.add(wake);
          return () => waiting.delete(wake);
        }).flatMap(() => receive);
      });

      const messages = Stream.unfoldEffect(undefined, () =>
        receive.map((message) => (message === undefined ? null : ([message, undefined] as const))),
      );

      const close = sync(() => {
        closed = true;
        const wake = [...waiting];
        waiting.clear();
        for (const resume of wake) resume();
      });

      const verify: Eff<void, Throws<ReplayMismatch>> = suspend(() => {
        if (mismatch) return fail(mismatch);
        if (next < transcript.length) {
          const entry = transcript[next]!;
          const what = entry.direction === "sent" ? "send" : "receive";
          return fail(
            new ReplayMismatch({
              message: `the conversation stopped early: it didn't ${what} ${show(entry.message)}`,
              at: next,
            }),
          );
        }
        return succeed(undefined);
      });

      return {
        connection: { remote: "replay", send, messages, close },
        verify,
      };
    });
  },
} as const;

function show(message: unknown): string {
  try {
    const text = JSON.stringify(message);
    return text === undefined
      ? String(message)
      : text.length > 200
        ? `${text.slice(0, 200)}…`
        : text;
  } catch {
    return String(message);
  }
}
