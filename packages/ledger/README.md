# @spilne/perfect-ledger

Building blocks for tamper-evident ledgers: canonical encoding, an append-only Merkle log with inclusion and consistency proofs, and signed checkpoints. Pure functions, no dependencies beyond `node:crypto`, and no use of the perfect runtime: the same code runs in a service, a CLI or an auditor's script.

```ts
import {
  MerkleLog,
  signCheckpoint,
  ed25519Signer,
  verifyCheckpoint,
  verifyInclusion,
} from "@spilne/perfect-ledger";

const log = new MerkleLog();
const index = log.append(entryBytes);

// Publish a signed statement of what the log contains.
const operator = ed25519Signer(seed);
const signed = signCheckpoint({
  checkpoint: { origin: "example.com/audit", size: log.size, rootHash: log.rootHash() },
  name: "operator",
  signer: operator,
});

// An auditor, holding only the checkpoint and a proof, checks an entry is in it.
verifyCheckpoint({
  signed,
  trusted: [{ name: "operator", publicKey: operator.publicKey }],
  threshold: 1,
});
verifyInclusion({
  leaf: leafHash(entryBytes),
  index,
  size: log.size,
  proof: log.inclusionProof({ index }),
  root: signed.checkpoint.rootHash,
});
```

## What is in it

- **`MerkleLog`**: RFC 6962 / RFC 9162 tree over SHA-256, in memory. `rootHash`, `inclusionProof`, `consistencyProof`. Tested against the Certificate Transparency reference roots, the textbook definition for sizes up to 70, and every index and size pair up to 40.
- **`verifyInclusion`, `verifyConsistency`**: need only hashes, not the log. Consistency proves a newer root extends an older one, so an operator cannot rewrite history without detection.
- **`encodeCanonical`, `decodeCanonical`**: one byte string per value (integers, strings, bytes, arrays, records), with sorted keys. Floats are refused. The decoder accepts only canonical input. Hash entries through this so equal data always hashes equal.
- **Signed checkpoints**: the C2SP signed-note text format (`origin`, `size`, base64 `root`, then `— name signature` lines), so existing transparency-log tools can read them. `verifyCheckpoint` takes a threshold of distinct trusted keys, which is how witness cosigning (k of n) works.

## Things to know

- A verifier uses a tree size only to shape the path; the root commits to the content. Always bind size and root together, as a checkpoint does.
- `MerkleLog` keeps leaf hashes in memory. Persistence, an HTTP API and a CLI belong in layers on top that use perfect's effects; this package stays pure so they can share it.
- Ed25519 uses `node:crypto`; `Signer` is an interface, so a KMS-backed signer can replace it.
