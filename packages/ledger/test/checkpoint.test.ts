import { describe, expect, it } from "bun:test";
import {
  ed25519Signer,
  ed25519Verify,
  formatSignedCheckpoint,
  keyId,
  MerkleLog,
  parseSignedCheckpoint,
  signCheckpoint,
  verifyCheckpoint,
  type Checkpoint,
  type TrustedKey,
} from "../src";

const seed = (n: number) => new Uint8Array(32).fill(n);
const log = new MerkleLog();
for (let i = 0; i < 9; i++) log.append(new TextEncoder().encode(`e${i}`));
const checkpoint: Checkpoint = { origin: "example.com/audit", size: 9, rootHash: log.rootHash() };

const operator = ed25519Signer(seed(1));
const witnessA = ed25519Signer(seed(2));
const witnessB = ed25519Signer(seed(3));
const stranger = ed25519Signer(seed(4));
const trusted: TrustedKey[] = [
  { name: "operator", publicKey: operator.publicKey },
  { name: "witness-a", publicKey: witnessA.publicKey },
  { name: "witness-b", publicKey: witnessB.publicKey },
];

function cosigned() {
  const s1 = signCheckpoint({ checkpoint, name: "operator", signer: operator });
  const s2 = signCheckpoint({ checkpoint, name: "witness-a", signer: witnessA, previous: s1 });
  return signCheckpoint({ checkpoint, name: "witness-b", signer: witnessB, previous: s2 });
}

describe("Ed25519", () => {
  it("signs and verifies, and rejects other messages, keys and lengths", () => {
    const msg = new TextEncoder().encode("hello");
    const sig = operator.sign(msg);
    expect(ed25519Verify({ publicKey: operator.publicKey, message: msg, signature: sig })).toBe(
      true,
    );
    expect(ed25519Verify({ publicKey: witnessA.publicKey, message: msg, signature: sig })).toBe(
      false,
    );
    expect(
      ed25519Verify({ publicKey: operator.publicKey, message: new Uint8Array(1), signature: sig }),
    ).toBe(false);
    expect(
      ed25519Verify({ publicKey: operator.publicKey, message: msg, signature: sig.slice(1) }),
    ).toBe(false);
    expect(ed25519Verify({ publicKey: new Uint8Array(5), message: msg, signature: sig })).toBe(
      false,
    );
    expect(() => ed25519Signer(new Uint8Array(5))).toThrow(RangeError);
  });
});

describe("signed checkpoints", () => {
  it("round-trips through the signed-note text and still verifies", () => {
    const text = formatSignedCheckpoint(cosigned());
    expect(text.startsWith("example.com/audit\n9\n")).toBe(true);
    expect(text).toContain("\n\n— operator ");
    const parsed = parseSignedCheckpoint(text);
    expect(parsed.checkpoint.size).toBe(9);
    expect(verifyCheckpoint({ signed: parsed, trusted, threshold: 3 })).toBe(true);
  });

  it("counts distinct trusted keys against the threshold", () => {
    const one = signCheckpoint({ checkpoint, name: "operator", signer: operator });
    expect(verifyCheckpoint({ signed: one, trusted, threshold: 1 })).toBe(true);
    expect(verifyCheckpoint({ signed: one, trusted, threshold: 2 })).toBe(false);
    const twice = signCheckpoint({ checkpoint, name: "operator", signer: operator, previous: one });
    expect(verifyCheckpoint({ signed: twice, trusted, threshold: 2 })).toBe(false);
    expect(verifyCheckpoint({ signed: cosigned(), trusted, threshold: 2 })).toBe(true);
  });

  it("ignores signers that are not trusted, or sign under a trusted name with the wrong key", () => {
    const rogue = signCheckpoint({ checkpoint, name: "operator", signer: stranger });
    expect(verifyCheckpoint({ signed: rogue, trusted, threshold: 1 })).toBe(false);
    const unknown = signCheckpoint({ checkpoint, name: "someone", signer: stranger });
    expect(verifyCheckpoint({ signed: unknown, trusted, threshold: 1 })).toBe(false);
  });

  it("rejects a checkpoint whose size or root was changed after signing", () => {
    const signed = cosigned();
    const grown = { ...signed, checkpoint: { ...checkpoint, size: 10 } };
    const swapped = { ...signed, checkpoint: { ...checkpoint, rootHash: log.rootHash(8) } };
    expect(verifyCheckpoint({ signed: grown, trusted, threshold: 1 })).toBe(false);
    expect(verifyCheckpoint({ signed: swapped, trusted, threshold: 1 })).toBe(false);
  });

  it("binds signatures to the origin, so one log cannot be passed off as another", () => {
    const signed = cosigned();
    const other = { ...signed, checkpoint: { ...checkpoint, origin: "example.com/other" } };
    expect(verifyCheckpoint({ signed: other, trusted, threshold: 1 })).toBe(false);
  });

  it("derives the key id from name and key", () => {
    const a = keyId({ name: "operator", publicKey: operator.publicKey });
    expect(a.length).toBe(4);
    expect(keyId({ name: "operator2", publicKey: operator.publicKey })).not.toEqual(a);
  });

  it("refuses malformed input", () => {
    const good = formatSignedCheckpoint(cosigned());
    expect(() => parseSignedCheckpoint("no signatures\n1\nAAAA\n")).toThrow(RangeError);
    expect(() => parseSignedCheckpoint(good.replace("\n9\n", "\n09\n"))).toThrow(RangeError);
    expect(() => parseSignedCheckpoint(good.replace("— operator", "- operator"))).toThrow(
      RangeError,
    );
    expect(() => parseSignedCheckpoint(good.slice(0, -1))).toThrow(RangeError);
    expect(() => parseSignedCheckpoint(good.replace(/\n\n/, "\nextra\n\n"))).toThrow(RangeError);
    expect(() =>
      signCheckpoint({
        checkpoint: { ...checkpoint, origin: "a\nb" },
        name: "x",
        signer: operator,
      }),
    ).toThrow(RangeError);
    expect(() => signCheckpoint({ checkpoint, name: "bad name", signer: operator })).toThrow(
      RangeError,
    );
    expect(() => verifyCheckpoint({ signed: cosigned(), trusted, threshold: 0 })).toThrow(
      RangeError,
    );
    const other = { ...checkpoint, size: 3 };
    expect(() =>
      signCheckpoint({ checkpoint: other, name: "w", signer: witnessA, previous: cosigned() }),
    ).toThrow(RangeError);
  });
});
