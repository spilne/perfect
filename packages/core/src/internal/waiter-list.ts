// A first-in, first-out list of fibers waiting for something (an item, a
// permit, a resource).
//
// The important part: when a waiting fiber gives up (for example its
// timeout fires), it can remove itself from the list right away. Before,
// we only marked it as "cancelled" and left it in the array until someone
// handed out the next item. On a quiet queue that never happened, so every
// timed-out take() stayed in memory forever.
//
// Each waiter is its own list node (it extends Waiter), so adding a waiter
// costs one object, not a waiter plus a separate node.
export class Waiter {
  prev: Waiter | null = null;
  next: Waiter | null = null;
  linked = false;
}

export class WaiterList<W extends Waiter> {
  private head: W | null = null;
  private tail: W | null = null;
  length = 0;

  push(waiter: W): W {
    waiter.linked = true;
    waiter.prev = this.tail;
    waiter.next = null;
    if (this.tail === null) this.head = waiter;
    else this.tail.next = waiter;
    this.tail = waiter;
    this.length++;
    return waiter;
  }

  peek(): W | undefined {
    return this.head ?? undefined;
  }

  shift(): W | undefined {
    const waiter = this.head;
    if (waiter === null) return undefined;
    this.remove(waiter);
    return waiter;
  }

  /** Remove this waiter. Safe to call twice, the second call does nothing. */
  remove(waiter: W): void {
    if (!waiter.linked) return;
    waiter.linked = false;
    if (waiter.prev === null) this.head = waiter.next as W | null;
    else waiter.prev.next = waiter.next;
    if (waiter.next === null) this.tail = waiter.prev as W | null;
    else waiter.next.prev = waiter.prev;
    waiter.prev = null;
    waiter.next = null;
    this.length--;
  }

  /** Take out every waiter, oldest first, and leave the list empty. */
  drain(): W[] {
    const out: W[] = [];
    let waiter = this.head;
    while (waiter !== null) {
      const next = waiter.next as W | null;
      waiter.linked = false;
      waiter.prev = null;
      waiter.next = null;
      out.push(waiter);
      waiter = next;
    }
    this.head = null;
    this.tail = null;
    this.length = 0;
    return out;
  }
}
