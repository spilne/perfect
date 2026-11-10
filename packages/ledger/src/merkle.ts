// ---------------------------------------------------------------------------
// Append-only Merkle log, RFC 6962 / RFC 9162 (SHA-256).
//
//   leafHash(data)        SHA-256(0x00 || data)
//   nodeHash(left,right)  SHA-256(0x01 || left || right)
//   MerkleLog             in-memory log: root, inclusion and consistency proofs
//   verifyInclusion       checks a leaf is in the tree of a given size and root
//   verifyConsistency     checks a tree of one size is a prefix of a larger one
//
// Everything is synchronous and pure. The verifiers need only hashes, so an
// auditor can run them without the log. Sizes must be safe integers.
//
// A verifier uses a size only to shape the path, so a proof can also pass for
// a nearby size with the same shape. The root is what commits to the content;
// bind size and root together, as a signed checkpoint does.
// ---------------------------------------------------------------------------
import { createHash } from "node:crypto";
import { bytesEqual, concatBytes } from "./bytes.js";

export type Hash = Uint8Array;

const LEAF_PREFIX = Uint8Array.of(0x00);
const NODE_PREFIX = Uint8Array.of(0x01);

function sha256(data: Uint8Array): Hash {
  return new Uint8Array(createHash("sha256").update(data).digest());
}

/** Root of the empty tree: SHA-256 of the empty string. */
export const EMPTY_ROOT: Hash = sha256(new Uint8Array(0));

export function leafHash(data: Uint8Array): Hash {
  return sha256(concatBytes(LEAF_PREFIX, data));
}

export function nodeHash(params: { readonly left: Hash; readonly right: Hash }): Hash {
  return sha256(concatBytes(NODE_PREFIX, params.left, params.right));
}

/** Largest power of two strictly less than `n` (n >= 2). */
function splitPoint(n: number): number {
  let k = 1;
  while (k * 2 < n) k *= 2;
  return k;
}

const isOdd = (n: number): boolean => n % 2 === 1;
const half = (n: number): number => Math.floor(n / 2);
const isPowerOfTwo = (n: number): boolean => n > 0 && (n & (n - 1)) === 0 && n < 2 ** 30;

export class MerkleLog {
  private readonly leaves: Hash[] = [];
  private readonly cache = new Map<string, Hash>();

  get size(): number {
    return this.leaves.length;
  }

  /** Appends an entry and returns its index. */
  append(data: Uint8Array): number {
    this.leaves.push(leafHash(data));
    return this.leaves.length - 1;
  }

  /** Leaf hash of the entry at `index`. */
  leafHashAt(index: number): Hash {
    this.checkIndex(index);
    return this.leaves[index]!;
  }

  /** Root of the first `size` entries (default: all). */
  rootHash(size: number = this.size): Hash {
    this.checkSize(size);
    return this.range(0, size);
  }

  /** Audit path proving entry `index` is in the tree of `size` entries. */
  inclusionProof(params: { readonly index: number; readonly size?: number }): Hash[] {
    const size = params.size ?? this.size;
    this.checkSize(size);
    if (!Number.isSafeInteger(params.index) || params.index < 0 || params.index >= size) {
      throw new RangeError(`index ${params.index} is outside a tree of ${size}`);
    }
    return this.path(params.index, 0, size);
  }

  /** Proof that the tree of `from` entries is a prefix of the tree of `to` entries. */
  consistencyProof(params: { readonly from: number; readonly to?: number }): Hash[] {
    const to = params.to ?? this.size;
    this.checkSize(to);
    if (!Number.isSafeInteger(params.from) || params.from < 1 || params.from > to) {
      throw new RangeError(`from ${params.from} must be between 1 and ${to}`);
    }
    return this.subProof(params.from, 0, to, true);
  }

  private path(m: number, lo: number, hi: number): Hash[] {
    const n = hi - lo;
    if (n === 1) return [];
    const k = splitPoint(n);
    return m < k
      ? [...this.path(m, lo, lo + k), this.range(lo + k, hi)]
      : [...this.path(m - k, lo + k, hi), this.range(lo, lo + k)];
  }

  private subProof(m: number, lo: number, hi: number, complete: boolean): Hash[] {
    const n = hi - lo;
    if (m === n) return complete ? [] : [this.range(lo, hi)];
    const k = splitPoint(n);
    return m <= k
      ? [...this.subProof(m, lo, lo + k, complete), this.range(lo + k, hi)]
      : [...this.subProof(m - k, lo + k, hi, false), this.range(lo, lo + k)];
  }

  private range(lo: number, hi: number): Hash {
    const n = hi - lo;
    if (n === 0) return EMPTY_ROOT;
    if (n === 1) return this.leaves[lo]!;
    const key = isPowerOfTwo(n) ? `${lo}:${hi}` : undefined;
    const hit = key === undefined ? undefined : this.cache.get(key);
    if (hit) return hit;
    const k = splitPoint(n);
    const result = nodeHash({ left: this.range(lo, lo + k), right: this.range(lo + k, hi) });
    if (key !== undefined) this.cache.set(key, result);
    return result;
  }

  private checkIndex(index: number): void {
    if (!Number.isSafeInteger(index) || index < 0 || index >= this.size) {
      throw new RangeError(`index ${index} is outside a log of ${this.size}`);
    }
  }

  private checkSize(size: number): void {
    if (!Number.isSafeInteger(size) || size < 0 || size > this.size) {
      throw new RangeError(`size ${size} is outside a log of ${this.size}`);
    }
  }
}

/** RFC 9162 section 2.1.3.2: does `proof` place `leaf` at `index` in the tree of `size` with `root`? */
export function verifyInclusion(params: {
  readonly leaf: Hash;
  readonly index: number;
  readonly size: number;
  readonly proof: ReadonlyArray<Hash>;
  readonly root: Hash;
}): boolean {
  const { index, size, proof } = params;
  if (!Number.isSafeInteger(index) || !Number.isSafeInteger(size)) return false;
  if (index < 0 || index >= size) return false;
  let fn = index;
  let sn = size - 1;
  let r = params.leaf;
  for (const p of proof) {
    if (sn === 0) return false;
    if (isOdd(fn) || fn === sn) {
      r = nodeHash({ left: p, right: r });
      while (!isOdd(fn) && fn !== 0) {
        fn = half(fn);
        sn = half(sn);
      }
    } else {
      r = nodeHash({ left: r, right: p });
    }
    fn = half(fn);
    sn = half(sn);
  }
  return sn === 0 && bytesEqual(r, params.root);
}

/** RFC 9162 section 2.1.4.2: is the tree of `fromSize` with `fromRoot` a prefix of the tree of `toSize` with `toRoot`? */
export function verifyConsistency(params: {
  readonly fromSize: number;
  readonly fromRoot: Hash;
  readonly toSize: number;
  readonly toRoot: Hash;
  readonly proof: ReadonlyArray<Hash>;
}): boolean {
  const { fromSize, toSize } = params;
  if (!Number.isSafeInteger(fromSize) || !Number.isSafeInteger(toSize)) return false;
  if (fromSize < 1 || fromSize > toSize) return false;
  if (fromSize === toSize) {
    return params.proof.length === 0 && bytesEqual(params.fromRoot, params.toRoot);
  }
  const proof = isPowerOfTwo(fromSize) ? [params.fromRoot, ...params.proof] : [...params.proof];
  if (proof.length === 0) return false;
  let fn = fromSize - 1;
  let sn = toSize - 1;
  while (isOdd(fn)) {
    fn = half(fn);
    sn = half(sn);
  }
  let fr = proof[0]!;
  let sr = proof[0]!;
  for (const c of proof.slice(1)) {
    if (sn === 0) return false;
    if (isOdd(fn) || fn === sn) {
      fr = nodeHash({ left: c, right: fr });
      sr = nodeHash({ left: c, right: sr });
      while (!isOdd(fn) && fn !== 0) {
        fn = half(fn);
        sn = half(sn);
      }
    } else {
      sr = nodeHash({ left: sr, right: c });
    }
    fn = half(fn);
    sn = half(sn);
  }
  return sn === 0 && bytesEqual(fr, params.fromRoot) && bytesEqual(sr, params.toRoot);
}
