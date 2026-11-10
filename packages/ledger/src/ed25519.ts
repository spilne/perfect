import { createPrivateKey, createPublicKey, sign, verify } from "node:crypto";

const PKCS8_PREFIX = Uint8Array.from(Buffer.from("302e020100300506032b657004220420", "hex"));
const SPKI_PREFIX = Uint8Array.from(Buffer.from("302a300506032b6570032100", "hex"));

export const ED25519_KEY_BYTES = 32;
export const ED25519_SIGNATURE_BYTES = 64;

/** Signs messages with one Ed25519 key. */
export interface Signer {
  readonly publicKey: Uint8Array;
  sign(message: Uint8Array): Uint8Array;
}

/** `Signer` for a 32-byte Ed25519 seed. */
export function ed25519Signer(seed: Uint8Array): Signer {
  if (seed.length !== ED25519_KEY_BYTES) throw new RangeError("Ed25519 seed must be 32 bytes");
  const der = Buffer.concat([PKCS8_PREFIX, seed]);
  const key = createPrivateKey({ key: der, format: "der", type: "pkcs8" });
  const spki = createPublicKey(key).export({ format: "der", type: "spki" });
  const publicKey = new Uint8Array(spki.subarray(spki.length - ED25519_KEY_BYTES));
  return {
    publicKey,
    sign: (message) => new Uint8Array(sign(null, message, key)),
  };
}

/** True when `signature` is a valid Ed25519 signature of `message` by `publicKey`. Never throws. */
export function ed25519Verify(params: {
  readonly publicKey: Uint8Array;
  readonly message: Uint8Array;
  readonly signature: Uint8Array;
}): boolean {
  if (params.publicKey.length !== ED25519_KEY_BYTES) return false;
  if (params.signature.length !== ED25519_SIGNATURE_BYTES) return false;
  try {
    const key = createPublicKey({
      key: Buffer.concat([SPKI_PREFIX, params.publicKey]),
      format: "der",
      type: "spki",
    });
    return verify(null, params.message, key, params.signature);
  } catch {
    return false;
  }
}
