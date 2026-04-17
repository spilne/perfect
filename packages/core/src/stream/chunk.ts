// Chunk<A> — immutable batch container
// Array-backed with offset/length for zero-copy slicing.
// All stream operators work on chunks, not individual elements.

export class Chunk<A> {
  private constructor(
    private readonly array: ReadonlyArray<A>,
    private readonly start: number,
    readonly length: number,
  ) {}

  static empty<A>(): Chunk<A> {
    return new Chunk<A>([], 0, 0);
  }

  static of<A>(...items: A[]): Chunk<A> {
    return new Chunk(items, 0, items.length);
  }

  static fromArray<A>(arr: ReadonlyArray<A>): Chunk<A> {
    return new Chunk(arr, 0, arr.length);
  }

  static single<A>(value: A): Chunk<A> {
    return new Chunk([value], 0, 1);
  }

  get isEmpty(): boolean {
    return this.length === 0;
  }

  get(index: number): A {
    return this.array[this.start + index]!;
  }

  head(): A | undefined {
    return this.length > 0 ? this.array[this.start] : undefined;
  }

  last(): A | undefined {
    return this.length > 0 ? this.array[this.start + this.length - 1] : undefined;
  }

  take(n: number): Chunk<A> {
    if (n >= this.length) return this;
    if (n <= 0) return Chunk.empty();
    return new Chunk(this.array, this.start, n);
  }

  drop(n: number): Chunk<A> {
    if (n <= 0) return this;
    if (n >= this.length) return Chunk.empty();
    return new Chunk(this.array, this.start + n, this.length - n);
  }

  map<B>(f: (a: A) => B): Chunk<B> {
    const result = new Array<B>(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = f(this.array[this.start + i]!);
    }
    return new Chunk(result, 0, this.length);
  }

  filter(p: (a: A) => boolean): Chunk<A> {
    const result: A[] = [];
    for (let i = 0; i < this.length; i++) {
      const item = this.array[this.start + i]!;
      if (p(item)) result.push(item);
    }
    return new Chunk(result, 0, result.length);
  }

  flatMap<B>(f: (a: A) => Chunk<B>): Chunk<B> {
    const result: B[] = [];
    for (let i = 0; i < this.length; i++) {
      const chunk = f(this.array[this.start + i]!);
      for (let j = 0; j < chunk.length; j++) {
        result.push(chunk.get(j));
      }
    }
    return new Chunk(result, 0, result.length);
  }

  concat(that: Chunk<A>): Chunk<A> {
    if (this.isEmpty) return that;
    if (that.isEmpty) return this;
    const result = new Array<A>(this.length + that.length);
    for (let i = 0; i < this.length; i++) result[i] = this.array[this.start + i]!;
    for (let i = 0; i < that.length; i++) result[this.length + i] = that.array[that.start + i]!;
    return new Chunk(result, 0, result.length);
  }

  forEach(f: (a: A) => void): void {
    for (let i = 0; i < this.length; i++) f(this.array[this.start + i]!);
  }

  reduce<B>(zero: B, f: (b: B, a: A) => B): B {
    let acc = zero;
    for (let i = 0; i < this.length; i++) acc = f(acc, this.array[this.start + i]!);
    return acc;
  }

  find(p: (a: A) => boolean): A | undefined {
    for (let i = 0; i < this.length; i++) {
      const item = this.array[this.start + i]!;
      if (p(item)) return item;
    }
    return undefined;
  }

  every(p: (a: A) => boolean): boolean {
    for (let i = 0; i < this.length; i++) {
      if (!p(this.array[this.start + i]!)) return false;
    }
    return true;
  }

  some(p: (a: A) => boolean): boolean {
    for (let i = 0; i < this.length; i++) {
      if (p(this.array[this.start + i]!)) return true;
    }
    return true;
  }

  toArray(): A[] {
    if (this.start === 0 && this.length === this.array.length) {
      return this.array.slice() as A[];
    }
    return this.array.slice(this.start, this.start + this.length) as A[];
  }

  *[Symbol.iterator](): Iterator<A> {
    for (let i = 0; i < this.length; i++) {
      yield this.array[this.start + i]!;
    }
  }
}
