// Consistent hashing: decide which node owns a key so that when a node joins
// or leaves, only the keys of that node move.
//
//   const ring = HashRing.of(["a", "b", "c"]);
//   ring.owner("user:42");         // "b"
//   ring.owners("user:42", 2);     // ["b", "a"], e.g. for a replica
//   ring.moved(ring.with("d"), keys)  // which of `keys` go where
//
// Each node takes `vnodes` points on a ring of 32-bit hashes; a key belongs
// to the first point at or after its own hash. Many points per node spread
// the keys evenly. A ring is an immutable value: `with` and `without` return
// a new one.

const DEFAULT_VNODES = 128;

/** A key that changed owner between two rings. */
export interface Move {
  readonly key: string;
  readonly from: string | undefined;
  readonly to: string | undefined;
}

interface Point {
  readonly hash: number;
  readonly node: string;
}

export class HashRing {
  private constructor(
    /** The nodes on the ring, sorted. */
    readonly nodes: readonly string[],
    private readonly vnodes: number,
    private readonly points: readonly Point[],
  ) {}

  /** A ring of `nodes`, each with `vnodes` points (default 128). */
  static of(nodes: Iterable<string>, options: { vnodes?: number } = {}): HashRing {
    const vnodes = options.vnodes ?? DEFAULT_VNODES;
    const unique = [...new Set(nodes)].sort();
    const points: Point[] = [];
    for (const node of unique) {
      for (let i = 0; i < vnodes; i++) points.push({ hash: hash32(`${node}#${i}`), node });
    }
    // Ties are practically impossible, but sort them by node so every
    // process builds exactly the same ring.
    points.sort((a, b) => a.hash - b.hash || (a.node < b.node ? -1 : 1));
    return new HashRing(unique, vnodes, points);
  }

  /** The node that owns `key`, or undefined on an empty ring. */
  owner(key: string): string | undefined {
    if (this.points.length === 0) return undefined;
    return this.points[this.firstPointFor(key)]!.node;
  }

  /**
   * Up to `count` different nodes for `key`, in ring order: the owner first,
   * then the nodes that would take over from it. Use them for replicas.
   */
  owners(key: string, count: number): string[] {
    const found: string[] = [];
    if (this.points.length === 0) return found;
    const start = this.firstPointFor(key);
    for (let i = 0; i < this.points.length && found.length < count; i++) {
      const node = this.points[(start + i) % this.points.length]!.node;
      if (!found.includes(node)) found.push(node);
    }
    return found;
  }

  /** This ring with `node` added. */
  with(node: string): HashRing {
    return this.nodes.includes(node)
      ? this
      : HashRing.of([...this.nodes, node], { vnodes: this.vnodes });
  }

  /** This ring without `node`. */
  without(node: string): HashRing {
    return this.nodes.includes(node)
      ? HashRing.of(
          this.nodes.filter((n) => n !== node),
          { vnodes: this.vnodes },
        )
      : this;
  }

  /** Which of `keys` have a different owner on `next`. */
  moved(next: HashRing, keys: Iterable<string>): Move[] {
    const moves: Move[] = [];
    for (const key of keys) {
      const from = this.owner(key);
      const to = next.owner(key);
      if (from !== to) moves.push({ key, from, to });
    }
    return moves;
  }

  /** Index of the first point at or after the key's hash, wrapping around. */
  private firstPointFor(key: string): number {
    const hash = hash32(key);
    let low = 0;
    let high = this.points.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (this.points[middle]!.hash < hash) low = middle + 1;
      else high = middle;
    }
    return low === this.points.length ? 0 : low;
  }
}

const utf8 = new TextEncoder();

/**
 * MurmurHash3 (32-bit) of the string's UTF-8 bytes. It is fast, spreads
 * similar strings like "a#1", "a#2" far apart, and is the same everywhere,
 * so every process agrees on the ring.
 */
export function hash32(text: string, seed = 0): number {
  const bytes = utf8.encode(text);
  let h = seed >>> 0;
  const blocks = bytes.length - (bytes.length % 4);
  for (let i = 0; i < blocks; i += 4) {
    let k = bytes[i]! | (bytes[i + 1]! << 8) | (bytes[i + 2]! << 16) | (bytes[i + 3]! << 24);
    k = Math.imul(k, 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);
    h ^= k;
    h = (h << 13) | (h >>> 19);
    h = (Math.imul(h, 5) + 0xe6546b64) | 0;
  }
  let k = 0;
  switch (bytes.length % 4) {
    case 3:
      k ^= bytes[blocks + 2]! << 16;
    // falls through
    case 2:
      k ^= bytes[blocks + 1]! << 8;
    // falls through
    case 1:
      k ^= bytes[blocks]!;
      k = Math.imul(k, 0xcc9e2d51);
      k = (k << 15) | (k >>> 17);
      k = Math.imul(k, 0x1b873593);
      h ^= k;
  }
  h ^= bytes.length;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}
