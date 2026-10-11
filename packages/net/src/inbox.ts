// Received messages waiting to be read. The socket pushes into it from its
// callbacks; the connection's `messages` stream takes from it. When too many
// messages pile up, the socket is paused until the reader catches up, so a
// slow reader slows the sender down instead of using up memory.

import { async, fail, succeed, type Eff, type Throws } from "@spilne/perfect-core";

type State<E> =
  | { readonly _tag: "Open" }
  | { readonly _tag: "Ended" }
  | { readonly _tag: "Failed"; readonly error: E };

export class Inbox<T, E> {
  private items: T[] = [];
  private state: State<E> = { _tag: "Open" };
  private waiting: ((batch: Eff<T[] | undefined, Throws<E>>) => void) | undefined;
  private paused = false;

  constructor(
    private readonly highWaterMark: number,
    private readonly pause: () => void,
    private readonly resume: () => void,
  ) {}

  push(item: T): void {
    if (this.state._tag !== "Open") return;
    this.items.push(item);
    this.wakeReader();
    if (!this.paused && this.items.length >= this.highWaterMark) {
      this.paused = true;
      this.pause();
    }
  }

  /** No more messages. What is already here can still be read. */
  end(): void {
    if (this.state._tag !== "Open") return;
    this.state = { _tag: "Ended" };
    this.wakeReader();
  }

  /** The connection broke. Reading fails once the messages already here are read. */
  fail(error: E): void {
    if (this.state._tag !== "Open") return;
    this.state = { _tag: "Failed", error };
    this.wakeReader();
  }

  /**
   * Everything received so far, waiting for at least one message.
   * `undefined` once the inbox has ended and is empty.
   */
  readonly take: Eff<T[] | undefined, Throws<E>> = async<T[] | undefined, E>((resume) => {
    const ready = this.ready();
    if (ready) return resume(ready);
    this.waiting = resume;
    return () => {
      if (this.waiting === resume) this.waiting = undefined;
    };
  });

  private ready(): Eff<T[] | undefined, Throws<E>> | undefined {
    if (this.items.length > 0) {
      const batch = this.items;
      this.items = [];
      if (this.paused) {
        this.paused = false;
        this.resume();
      }
      return succeed(batch);
    }
    if (this.state._tag === "Ended") return succeed(undefined);
    if (this.state._tag === "Failed") return fail(this.state.error);
    return undefined;
  }

  private wakeReader(): void {
    const resume = this.waiting;
    if (!resume) return;
    const ready = this.ready();
    if (!ready) return;
    this.waiting = undefined;
    resume(ready);
  }
}
