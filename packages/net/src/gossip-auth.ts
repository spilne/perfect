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

import { createHmac, timingSafeEqual } from "node:crypto";
import {
  ED25519_KEY_BYTES,
  ED25519_SIGNATURE_BYTES,
  concatBytes,
  ed25519Keyring,
  ed25519Signer,
  generateEd25519Identity,
  type Ed25519Identity,
} from "./ed25519.js";

export type { Ed25519Identity };

/** Signs the messages a member sends and checks the ones it receives. */
export interface GossipAuthenticator {
  /** The length of every tag `sign` returns. */
  readonly tagBytes: number;
  /** The tag to put in front of `payload`. */
  sign(payload: Uint8Array): Uint8Array;
  /** Whether `tag` is a valid tag of `payload`. Must not throw. */
  verify(params: { readonly payload: Uint8Array; readonly tag: Uint8Array }): boolean;
}

export const GossipAuth = {
  /** A fresh Ed25519 identity for a member. Keep `privateKey` secret; share `publicKey`. */
  generateEd25519Identity,

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
    const signer = ed25519Signer(params.privateKey);
    const keyring = ed25519Keyring(params.trusted);
    const tagBytes = ED25519_KEY_BYTES + ED25519_SIGNATURE_BYTES;
    return {
      tagBytes,
      sign: (payload) => concatBytes(signer.publicKey, signer.sign(payload)),
      verify: ({ payload, tag }) =>
        tag.length === tagBytes &&
        keyring.verify({
          publicKey: tag.subarray(0, ED25519_KEY_BYTES),
          message: payload,
          signature: tag.subarray(ED25519_KEY_BYTES),
        }),
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
