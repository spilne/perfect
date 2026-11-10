// ---------------------------------------------------------------------------
// Signed checkpoints in the C2SP signed-note format (c2sp.org/signed-note,
// c2sp.org/tlog-checkpoint), so existing transparency-log tooling can read
// them:
//
//   <origin>\n<size>\n<base64 root>\n\n— <name> <base64(keyId || signature)>\n
//
// The text above the blank line is what each signature covers. keyId is the
// first 4 bytes of SHA-256(name || 0x0A || 0x01 || public key). Several
// witnesses can sign the same text; `verifyCheckpoint` requires `threshold`
// distinct trusted keys to have done so.
// ---------------------------------------------------------------------------
import { createHash } from "node:crypto";
import { bytesEqual, concatBytes } from "./bytes.js";
import { ed25519Verify, type Signer } from "./ed25519.js";
import type { Hash } from "./merkle.js";

export interface Checkpoint {
  readonly origin: string;
  readonly size: number;
  readonly rootHash: Hash;
}

export interface CheckpointSignature {
  readonly name: string;
  readonly keyId: Uint8Array;
  readonly signature: Uint8Array;
}

export interface SignedCheckpoint {
  readonly checkpoint: Checkpoint;
  readonly signatures: ReadonlyArray<CheckpointSignature>;
}

/** A key a verifier trusts, by the name it signs under. */
export interface TrustedKey {
  readonly name: string;
  readonly publicKey: Uint8Array;
}

const enc = new TextEncoder();
const dec = new TextDecoder("utf-8", { fatal: true });
const DASH = "—";

export function keyId(params: {
  readonly name: string;
  readonly publicKey: Uint8Array;
}): Uint8Array {
  const digest = createHash("sha256")
    .update(concatBytes(enc.encode(params.name), Uint8Array.of(0x0a, 0x01), params.publicKey))
    .digest();
  return new Uint8Array(digest.subarray(0, 4));
}

function assertCheckpoint(c: Checkpoint): void {
  if (c.origin.length === 0 || /[\n\r]/.test(c.origin)) {
    throw new RangeError("origin must be a non-empty single line");
  }
  if (!Number.isSafeInteger(c.size) || c.size < 0)
    throw new RangeError("size must be a non-negative integer");
  if (c.rootHash.length !== 32) throw new RangeError("rootHash must be 32 bytes");
}

/** The signed text of a checkpoint, ending in a newline. */
export function checkpointText(checkpoint: Checkpoint): Uint8Array {
  assertCheckpoint(checkpoint);
  const root = Buffer.from(checkpoint.rootHash).toString("base64");
  return enc.encode(`${checkpoint.origin}\n${checkpoint.size}\n${root}\n`);
}

/** Signs `checkpoint` as `name`, adding to any signatures already collected. */
export function signCheckpoint(params: {
  readonly checkpoint: Checkpoint;
  readonly name: string;
  readonly signer: Signer;
  readonly previous?: SignedCheckpoint;
}): SignedCheckpoint {
  const { name, signer, checkpoint } = params;
  if (name.length === 0 || /[\s+]/.test(name))
    throw new RangeError("name must be non-empty with no spaces or '+'");
  if (params.previous && !sameCheckpoint(params.previous.checkpoint, checkpoint)) {
    throw new RangeError("previous signatures are for a different checkpoint");
  }
  const signature = signer.sign(checkpointText(checkpoint));
  return {
    checkpoint,
    signatures: [
      ...(params.previous?.signatures ?? []),
      { name, keyId: keyId({ name, publicKey: signer.publicKey }), signature },
    ],
  };
}

function sameCheckpoint(a: Checkpoint, b: Checkpoint): boolean {
  return a.origin === b.origin && a.size === b.size && bytesEqual(a.rootHash, b.rootHash);
}

/** Serializes to the signed-note text form. */
export function formatSignedCheckpoint(signed: SignedCheckpoint): string {
  const lines = signed.signatures.map(
    (s) =>
      `${DASH} ${s.name} ${Buffer.from(concatBytes(s.keyId, s.signature)).toString("base64")}\n`,
  );
  return `${dec.decode(checkpointText(signed.checkpoint))}\n${lines.join("")}`;
}

/** Parses the signed-note text form. Throws RangeError on anything malformed. */
export function parseSignedCheckpoint(text: string): SignedCheckpoint {
  const split = text.indexOf("\n\n");
  if (split < 0) throw new RangeError("missing signature block");
  const body = text.slice(0, split + 1);
  const bodyLines = body.split("\n");
  if (bodyLines.length !== 4 || bodyLines[3] !== "")
    throw new RangeError("checkpoint must have three lines");
  const [origin, sizeText, rootText] = bodyLines as [string, string, string, string];
  if (!/^(0|[1-9][0-9]*)$/.test(sizeText)) throw new RangeError("invalid size");
  const rootHash = new Uint8Array(Buffer.from(rootText, "base64"));
  const checkpoint: Checkpoint = { origin, size: Number(sizeText), rootHash };
  if (Buffer.from(rootHash).toString("base64") !== rootText)
    throw new RangeError("invalid root encoding");
  assertCheckpoint(checkpoint);

  const sigText = text.slice(split + 2);
  if (sigText.length === 0 || !sigText.endsWith("\n"))
    throw new RangeError("signature block must end in a newline");
  const signatures = sigText
    .slice(0, -1)
    .split("\n")
    .map((line): CheckpointSignature => {
      const match = new RegExp(`^${DASH} (\\S+) (\\S+)$`).exec(line);
      if (!match) throw new RangeError("malformed signature line");
      const raw = new Uint8Array(Buffer.from(match[2]!, "base64"));
      if (raw.length !== 4 + 64) throw new RangeError("malformed signature");
      return { name: match[1]!, keyId: raw.slice(0, 4), signature: raw.slice(4) };
    });
  return { checkpoint, signatures };
}

/**
 * True when at least `threshold` distinct keys in `trusted` have a valid
 * signature on the checkpoint. Signatures from unknown names or key ids are
 * ignored, as are repeats of the same key.
 */
export function verifyCheckpoint(params: {
  readonly signed: SignedCheckpoint;
  readonly trusted: ReadonlyArray<TrustedKey>;
  readonly threshold: number;
}): boolean {
  if (!Number.isInteger(params.threshold) || params.threshold < 1) {
    throw new RangeError("threshold must be a positive integer");
  }
  const text = checkpointText(params.signed.checkpoint);
  const counted = new Set<number>();
  for (const sig of params.signed.signatures) {
    params.trusted.forEach((key, i) => {
      if (counted.has(i) || key.name !== sig.name) return;
      if (!bytesEqual(keyId(key), sig.keyId)) return;
      if (ed25519Verify({ publicKey: key.publicKey, message: text, signature: sig.signature }))
        counted.add(i);
    });
  }
  return counted.size >= params.threshold;
}
