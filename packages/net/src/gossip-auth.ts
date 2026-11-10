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
// - Every member shares the key, so a leaked key or a compromised member can
//   forge anything. Use TLS (see `Transport`) where you need identities.

import { createHmac, timingSafeEqual } from "node:crypto";

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
