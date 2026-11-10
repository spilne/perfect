// Ed25519 on raw 32-byte keys, shared by gossip authentication and the
// connection handshake. Node's crypto wants DER-wrapped keys, so this adds
// and strips the fixed wrappers.

import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  sign as edSign,
  verify as edVerify,
  type KeyObject,
} from "node:crypto";

/** An Ed25519 key pair as raw bytes: a 32-byte seed and the 32-byte public key. */
export interface Ed25519Identity {
  readonly privateKey: Uint8Array;
  readonly publicKey: Uint8Array;
}

export const ED25519_KEY_BYTES = 32;
export const ED25519_SIGNATURE_BYTES = 64;

const PKCS8_PREFIX = Uint8Array.from([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
]);
const SPKI_PREFIX = Uint8Array.from([
  0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
]);

export const concatBytes = (...parts: ReadonlyArray<Uint8Array>): Uint8Array => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
};

export const toHex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");

const rawPublicKey = (key: KeyObject): Uint8Array =>
  new Uint8Array(key.export({ format: "der", type: "spki" })).subarray(SPKI_PREFIX.length);

/** A fresh key pair. Keep `privateKey` secret; share `publicKey`. */
export function generateEd25519Identity(): Ed25519Identity {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const seed = new Uint8Array(privateKey.export({ format: "der", type: "pkcs8" })).subarray(
    PKCS8_PREFIX.length,
  );
  return { privateKey: seed.slice(), publicKey: rawPublicKey(publicKey).slice() };
}

/** Signs with a private seed; `publicKey` is the matching public key. Throws RangeError on a bad seed. */
export function ed25519Signer(privateKey: Uint8Array): {
  readonly publicKey: Uint8Array;
  sign(message: Uint8Array): Uint8Array;
} {
  if (privateKey.length !== ED25519_KEY_BYTES) {
    throw new RangeError("an Ed25519 private key is a 32-byte seed");
  }
  const secret = createPrivateKey({
    key: Buffer.from(concatBytes(PKCS8_PREFIX, privateKey)),
    format: "der",
    type: "pkcs8",
  });
  return {
    publicKey: rawPublicKey(createPublicKey(secret)),
    sign: (message) => new Uint8Array(edSign(null, message, secret)),
  };
}

/** The set of public keys that are allowed in. Throws RangeError on a key of the wrong length. */
export function ed25519Keyring(trusted: ReadonlyArray<Uint8Array>): {
  has(publicKey: Uint8Array): boolean;
  /** Whether `signature` is `publicKey`'s signature of `message` and the key is trusted. Never throws. */
  verify(params: {
    readonly publicKey: Uint8Array;
    readonly message: Uint8Array;
    readonly signature: Uint8Array;
  }): boolean;
} {
  const keys = new Map<string, KeyObject>();
  for (const raw of trusted) {
    if (raw.length !== ED25519_KEY_BYTES) throw new RangeError("an Ed25519 public key is 32 bytes");
    keys.set(
      toHex(raw),
      createPublicKey({
        key: Buffer.from(concatBytes(SPKI_PREFIX, raw)),
        format: "der",
        type: "spki",
      }),
    );
  }
  return {
    has: (publicKey) => keys.has(toHex(publicKey)),
    verify: ({ publicKey, message, signature }) => {
      try {
        const key = keys.get(toHex(publicKey));
        if (!key || signature.length !== ED25519_SIGNATURE_BYTES) return false;
        return edVerify(null, message, key, signature);
      } catch {
        return false;
      }
    },
  };
}
