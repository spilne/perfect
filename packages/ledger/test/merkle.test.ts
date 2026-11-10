import { describe, expect, it } from "bun:test";
import {
  EMPTY_ROOT,
  fromHex,
  leafHash,
  MerkleLog,
  nodeHash,
  toHex,
  verifyConsistency,
  verifyInclusion,
} from "../src";

// Leaves and roots from the Certificate Transparency reference tests (RFC 6962).
const LEAVES = [
  "",
  "00",
  "10",
  "2021",
  "3031",
  "40414243",
  "5051525354555657",
  "606162636465666768696a6b6c6d6e6f",
].map(fromHex);
const ROOTS = [
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
  "fac54203e7cc696cf0dfcb42c92a1d9dbaf70ad9e621f4bd8d98662f00e3c125",
  "aeb6bcfe274b70a14fb067a5e5578264db0fa9b51af5e0ba159158f329e06e77",
  "d37ee418976dd95753c1c73862b9398fa2a2cf9b4ff0fdfe8b30cd95209614b7",
  "4e3bbb1f7b478dcfe71fb631631519a3bca12c9aefca1612bfce4c13a86264d4",
  "76e67dadbcdf1e10e1b74ddc608abd2f98dfb16fbce75277b5232a127f2087ef",
  "ddb89be403809e325750d3d263cd78929c2942b7942a34b77e122c9594a74c8c",
  "5dc9da79a70659a9ad559cb701ded9a2ab9d823aad2f4960cfe370eff4604328",
];

function logOf(n: number): MerkleLog {
  const log = new MerkleLog();
  for (let i = 0; i < n; i++) log.append(new TextEncoder().encode(`entry-${i}`));
  return log;
}

/** The definition in RFC 6962 section 2.1, with no caching or proofs. */
function referenceRoot(leaves: ReadonlyArray<Uint8Array>): Uint8Array {
  if (leaves.length === 0) return EMPTY_ROOT;
  if (leaves.length === 1) return leaves[0]!;
  let k = 1;
  while (k * 2 < leaves.length) k *= 2;
  return nodeHash({
    left: referenceRoot(leaves.slice(0, k)),
    right: referenceRoot(leaves.slice(k)),
  });
}

describe("MerkleLog roots", () => {
  it("matches the RFC 6962 reference roots for every prefix", () => {
    const log = new MerkleLog();
    expect(toHex(log.rootHash())).toBe(ROOTS[0]!);
    LEAVES.forEach((leaf, i) => {
      log.append(leaf);
      expect(toHex(log.rootHash())).toBe(ROOTS[i + 1]!);
    });
    expect(toHex(log.rootHash(3))).toBe(ROOTS[3]!);
  });

  it("agrees with the textbook recursive definition up to 70 entries", () => {
    const log = logOf(70);
    for (let n = 0; n <= 70; n++) {
      const leaves = Array.from({ length: n }, (_, i) => log.leafHashAt(i));
      expect(toHex(log.rootHash(n))).toBe(toHex(referenceRoot(leaves)));
    }
  });

  it("separates leaves from interior nodes", () => {
    const a = leafHash(new Uint8Array([1]));
    const b = leafHash(new Uint8Array([2]));
    const joined = nodeHash({ left: a, right: b });
    expect(toHex(leafHash(new Uint8Array([...a, ...b])))).not.toBe(toHex(joined));
  });
});

describe("inclusion proofs", () => {
  it("verify for every index of every size up to 40", () => {
    const log = logOf(40);
    for (let size = 1; size <= 40; size++) {
      const root = log.rootHash(size);
      for (let index = 0; index < size; index++) {
        const proof = log.inclusionProof({ index, size });
        expect(verifyInclusion({ leaf: log.leafHashAt(index), index, size, proof, root })).toBe(
          true,
        );
      }
    }
  });

  it("reject a wrong leaf, index, size, root, or a changed proof", () => {
    const log = logOf(13);
    const root = log.rootHash();
    const proof = log.inclusionProof({ index: 5 });
    const base = { leaf: log.leafHashAt(5), index: 5, size: 13, proof, root };
    expect(verifyInclusion(base)).toBe(true);
    expect(verifyInclusion({ ...base, leaf: log.leafHashAt(6) })).toBe(false);
    expect(verifyInclusion({ ...base, index: 4 })).toBe(false);
    expect(verifyInclusion({ ...base, size: 20 })).toBe(false);
    expect(verifyInclusion({ ...base, size: 5 })).toBe(false);
    expect(verifyInclusion({ ...base, root: log.rootHash(12) })).toBe(false);
    expect(verifyInclusion({ ...base, proof: proof.slice(1) })).toBe(false);
    expect(verifyInclusion({ ...base, proof: [...proof, proof[0]!] })).toBe(false);
    const flipped = proof.map((h, i) => (i === 1 ? h.map((b) => b ^ 1) : h));
    expect(verifyInclusion({ ...base, proof: flipped })).toBe(false);
    expect(verifyInclusion({ ...base, index: 13 })).toBe(false);
    expect(verifyInclusion({ ...base, index: -1 })).toBe(false);
  });

  it("refuse out-of-range requests", () => {
    const log = logOf(4);
    expect(() => log.inclusionProof({ index: 4 })).toThrow(RangeError);
    expect(() => log.inclusionProof({ index: 0, size: 5 })).toThrow(RangeError);
    expect(() => log.rootHash(5)).toThrow(RangeError);
  });
});

describe("consistency proofs", () => {
  it("verify for every pair of sizes up to 40", () => {
    const log = logOf(40);
    for (let to = 1; to <= 40; to++) {
      for (let from = 1; from <= to; from++) {
        const proof = log.consistencyProof({ from, to });
        expect(
          verifyConsistency({
            fromSize: from,
            fromRoot: log.rootHash(from),
            toSize: to,
            toRoot: log.rootHash(to),
            proof,
          }),
        ).toBe(true);
      }
    }
  });

  it("reject a history that was rewritten", () => {
    const honest = logOf(20);
    const forked = new MerkleLog();
    for (let i = 0; i < 20; i++)
      forked.append(new TextEncoder().encode(i === 3 ? "forged" : `entry-${i}`));
    const proof = forked.consistencyProof({ from: 10, to: 20 });
    expect(
      verifyConsistency({
        fromSize: 10,
        fromRoot: honest.rootHash(10),
        toSize: 20,
        toRoot: forked.rootHash(20),
        proof,
      }),
    ).toBe(false);
  });

  it("reject a changed proof, wrong sizes and a non-empty proof for equal sizes", () => {
    const log = logOf(21);
    const proof = log.consistencyProof({ from: 6, to: 21 });
    const base = {
      fromSize: 6,
      fromRoot: log.rootHash(6),
      toSize: 21,
      toRoot: log.rootHash(21),
      proof,
    };
    expect(verifyConsistency(base)).toBe(true);
    expect(verifyConsistency({ ...base, proof: proof.slice(1) })).toBe(false);
    expect(verifyConsistency({ ...base, proof: [...proof, proof[0]!] })).toBe(false);
    expect(verifyConsistency({ ...base, fromSize: 7 })).toBe(false);
    expect(verifyConsistency({ ...base, toSize: 40 })).toBe(false);
    expect(verifyConsistency({ ...base, toSize: 10 })).toBe(false);
    expect(verifyConsistency({ ...base, fromSize: 0 })).toBe(false);
    expect(verifyConsistency({ ...base, fromSize: 22 })).toBe(false);
    expect(
      verifyConsistency({
        fromSize: 5,
        fromRoot: log.rootHash(5),
        toSize: 5,
        toRoot: log.rootHash(5),
        proof: [log.rootHash(5)],
      }),
    ).toBe(false);
    expect(() => log.consistencyProof({ from: 0 })).toThrow(RangeError);
  });
});
