// Authenticating gossip messages. Gossip has no security of its own: anyone
// who can reach a member's port can claim that any member is dead. An
// authenticator lets a cluster that shares a secret reject messages from
// everybody else.
//
//   Gossip.join({ ..., authenticator: GossipAuth.hmacSha256(process.env.CLUSTER_KEY!) })
//
// What it does and doesn't do:
// - Every message carries an HMAC tag over its bytes; a message with a
//   missing or wrong tag is dropped (and counted) before it is parsed.
// - It is not encryption: membership and metadata still travel in clear.
// - It does not stop replay: someone who recorded a valid message can send it
//   again. Gossip's incarnation numbers make most replayed news harmless (it
//   is older than what a member already knows) but a replay can still
//   delay the refutation of a stale suspicion.
// - With `hmacSha256` every member shares the key, so a leaked key or a
//   compromised member can forge anything. `ed25519` gives each member its own
//   key instead; use TLS (see `Transport`) where you need encryption too.

import {
  createHmac,
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  sign as edSign,
  timingSafeEqual,
  verify as edVerify,
  type KeyObject,
} from "node:crypto";

/** Signs the messages a member sends and checks the ones it receives. */
export interface GossipAuthenticator {
  /** The length of every tag `sign` returns. */
  readonly tagBytes: number;
  /** The tag to put in front of `payload`. */
  sign(payload: Uint8Array): Uint8Array;
  /** Whether `tag` is a valid tag of `payload`. Must not throw. */
  verify(params: { readonly payload: Uint8Array; readonly tag: Uint8Array }): boolean;
}

/** An Ed25519 key pair as raw bytes: a 32-byte seed and the 32-byte public key. */
export interface Ed25519Identity {
  readonly privateKey: Uint8Array;
  readonly publicKey: Uint8Array;
}

// PKCS#8 wrapper for a raw 32-byte Ed25519 seed.
const PKCS8_ED25519_PREFIX = Uint8Array.from([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
]);
// SPKI wrapper for a raw 32-byte Ed25519 public key.
const SPKI_ED25519_PREFIX = Uint8Array.from([
  0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
]);
const ED25519_KEY_BYTES = 32;
const ED25519_SIGNATURE_BYTES = 64;

const concat = (a: Uint8Array, b: Uint8Array) => {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
};
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
const publicKeyObject = (raw: Uint8Array): KeyObject =>
  createPublicKey({
    key: Buffer.from(concat(SPKI_ED25519_PREFIX, raw)),
    format: "der",
    type: "spki",
  });
const rawPublicKey = (key: KeyObject): Uint8Array =>
  new Uint8Array(key.export({ format: "der", type: "spki" })).subarray(SPKI_ED25519_PREFIX.length);

export const GossipAuth = {
  /** A fresh Ed25519 identity for a member. Keep `privateKey` secret; share `publicKey`. */
  generateEd25519Identity(): Ed25519Identity {
    const { privateKey, publicKey } = generateKeyPairSync("ed25519");
    const seed = new Uint8Array(privateKey.export({ format: "der", type: "pkcs8" })).subarray(
      PKCS8_ED25519_PREFIX.length,
    );
    return { privateKey: seed.slice(), publicKey: rawPublicKey(publicKey).slice() };
  },

  /**
   * Per-member identities: each message is signed with this member's Ed25519
   * key and carries its public key; a message is accepted only when that key
   * is one of `trusted` and the signature checks out. Unlike `hmacSha256`, a
   * leaked key lets someone impersonate one member, not the whole cluster,
   * and a member can be removed by leaving its key out of `trusted`.
   *
   * The tag is the 32-byte public key followed by the 64-byte signature.
   * Everything else about `hmacSha256` still holds: no encryption, no replay
   * protection, and the signature proves who sent a message, not that what the
   * sender says about other members is true. `trusted` is fixed for the life
   * of the authenticator; build a new one to change the set.
   */
  ed25519(params: {
    /** This member's 32-byte private seed (see `generateEd25519Identity`). */
    readonly privateKey: Uint8Array;
    /** The 32-byte public keys of every member allowed in, this member's included or not. */
    readonly trusted: ReadonlyArray<Uint8Array>;
  }): GossipAuthenticator {
    const { privateKey, trusted } = params;
    if (privateKey.length !== ED25519_KEY_BYTES) {
      throw new RangeError("an Ed25519 private key is a 32-byte seed");
    }
    const secret = createPrivateKey({
      key: Buffer.from(concat(PKCS8_ED25519_PREFIX, privateKey)),
      format: "der",
      type: "pkcs8",
    });
    const own = rawPublicKey(createPublicKey(secret));
    const keys = new Map<string, KeyObject>();
    for (const raw of trusted) {
      if (raw.length !== ED25519_KEY_BYTES) {
        throw new RangeError("an Ed25519 public key is 32 bytes");
      }
      keys.set(hex(raw), publicKeyObject(raw));
    }
    return {
      tagBytes: ED25519_KEY_BYTES + ED25519_SIGNATURE_BYTES,
      sign: (payload) => concat(own, new Uint8Array(edSign(null, payload, secret))),
      verify: ({ payload, tag }) => {
        try {
          if (tag.length !== ED25519_KEY_BYTES + ED25519_SIGNATURE_BYTES) return false;
          const key = keys.get(hex(tag.subarray(0, ED25519_KEY_BYTES)));
          if (!key) return false;
          return edVerify(null, payload, key, tag.subarray(ED25519_KEY_BYTES));
        } catch {
          return false;
        }
      },
    };
  },

  /**
   * HMAC-SHA-256 with a key shared by the whole cluster. Use a random key of
   * at least 16 bytes.
   */
  hmacSha256(key: string | Uint8Array): GossipAuthenticator {
    const secret = typeof key === "string" ? new TextEncoder().encode(key) : key;
    if (secret.length < 16) {
      throw new RangeError("a gossip key needs at least 16 bytes");
    }
    const tagOf = (payload: Uint8Array) =>
      new Uint8Array(createHmac("sha256", secret).update(payload).digest());
    return {
      tagBytes: 32,
      sign: tagOf,
      verify: ({ payload, tag }) => {
        const expected = tagOf(payload);
        return tag.length === expected.length && timingSafeEqual(tag, expected);
      },
    };
  },
} as const;
