// A first-in, first-out list built on a circular array.
//
// We use this instead of a plain array because array.shift() has to move
// every remaining item forward. On Node that gets very slow once the array
// is big, so a queue with a lot of items backed up got slower and slower.
// Here, taking the first item just moves a pointer.
export class Deque<T> {
  private items: Array<T | undefined>;
  private head = 0;
  private mask: number;
  length = 0;

  constructor(initialCapacity = 16) {
    let capacity = 16;
    while (capacity < initialCapacity) capacity *= 2;
    this.items = new Array(capacity);
    this.mask = capacity - 1;
  }

  push(value: T): void {
    if (this.length === this.items.length) this.grow();
    this.items[(this.head + this.length) & this.mask] = value;
    this.length++;
  }

  shift(): T | undefined {
    if (this.length === 0) return undefined;
    const value = this.items[this.head];
    this.items[this.head] = undefined;
    this.head = (this.head + 1) & this.mask;
    this.length--;
    return value;
  }

  peek(): T | undefined {
    return this.length === 0 ? undefined : this.items[this.head];
  }

  get(index: number): T {
    return this.items[(this.head + index) & this.mask] as T;
  }

  /** Put a value at position `index` (0 is the front). Slow for big lists,
   *  so only use it on rare paths. */
  insert(index: number, value: T): void {
    if (this.length === this.items.length) this.grow();
    for (let i = this.length; i > index; i--) {
      this.items[(this.head + i) & this.mask] = this.items[(this.head + i - 1) & this.mask];
    }
    this.items[(this.head + index) & this.mask] = value;
    this.length++;
  }

  /** Take out every value, oldest first, and leave the list empty. */
  drain(): T[] {
    const out = new Array<T>(this.length);
    for (let i = 0; i < out.length; i++) {
      const slot = (this.head + i) & this.mask;
      out[i] = this.items[slot] as T;
      this.items[slot] = undefined;
    }
    this.head = 0;
    this.length = 0;
    return out;
  }

  clear(): void {
    this.drain();
  }

  private grow(): void {
    const next = new Array<T | undefined>(this.items.length * 2);
    for (let i = 0; i < this.length; i++) next[i] = this.items[(this.head + i) & this.mask];
    this.items = next;
    this.head = 0;
    this.mask = next.length - 1;
  }
}
