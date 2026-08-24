# Releasing Perfect

Perfect publishes each adapter and runtime component as a separate public package
under the `@perfect` npm scope. Public packages start at `0.1.0` and use independent
semantic versions, so changing Redis does not force an unrelated Kafka release.

Twelve packages publish: `core`, `http`, `http-otel`, `kafka`, `kafka-kafkajs`,
`kafka-platformatic`, `otel`, `postgres`, `redis`, `swc-plugin`, `topology`,
`transform`. `@perfect/integration` is private and never publishes.

---

## One-time setup

Nothing has been published yet, so every step below is still pending.

### 1. Settle the scope name

Do this before anything else — it is the only step that can invalidate all the
others, and it is effectively irreversible once packages exist.

As of 2026-08-23, `@perfect/core` returns 404 and no package exists anywhere
under the `@perfect` scope. That is **not** proof the scope is free: a scope can
be owned with nothing published in it, and that looks identical from outside.
(The bare `perfect` package name is taken, but that is a package name, not a
scope, and does not affect us.)

Check properly:

```sh
npm login                # browser flow
npm org ls perfect       # permission error => exists and is someone else's
                         # not found      => likely claimable
```

If it is taken, pick the fallback **now**, before publishing anything.
`@spilne/*` matches the GitHub organisation and is the obvious candidate.
Changing scope after release means republishing everything and breaking anyone
who already installed.

### 2. Claim the scope

Public packages only, so the free plan is enough:

1. <https://www.npmjs.com/org/create>
2. Create the org named `perfect` — this grants the `@perfect` scope.

A personal-account scope (`@<username>`) also works, but an organisation is
preferable here because maintainers can be added without sharing credentials.

### 3. Verify ownership

```sh
npm whoami
npm org ls perfect       # must list you
```

Do not continue until `npm org ls perfect` shows you as a member. Everything
downstream assumes the account can actually write to the scope.

### 4. Validate the release locally

```sh
bun run release:check
```

Builds every JavaScript package **and** the SWC WASM artifact, dry-run packs all
twelve public packages, and rejects leaked `workspace:*` dependency ranges. This
is the cheapest place to catch problems — it needs no token and touches no
registry.

Requires the Rust toolchain, which is pinned to 1.90.0 (see
[Pinned toolchain](#pinned-rust-toolchain) below).

### 5. Create an automation token

<https://www.npmjs.com/settings/~/tokens> → **Generate New Token** → **Granular
Access**, or Classic → **Automation**.

The token type matters. An **automation** token bypasses two-factor auth; a
plain publish token under 2FA enforcement will hang the release waiting for an
OTP that CI can never supply. Scope it to the `@perfect` org with read+write and
set an expiry you will remember to rotate.

### 6. Wire it into GitHub

```sh
gh secret set NPM_TOKEN --repo spilne/perfect            # paste the token
gh variable set NPM_RELEASE_ENABLED --repo spilne/perfect --body true
```

While `NPM_RELEASE_ENABLED` is unset, `release.yml` runs only via manual
dispatch. That is deliberate: a stray push to `main` cannot publish.

For the very first release, consider leaving the variable unset and triggering
the workflow manually instead — same path end to end, with an explicit finger on
the button.

### 7. Verify the first publish

```sh
npm view @perfect/core
cd /tmp && npm init -y && npm install @perfect/core     # from the real registry
```

Then drop the "not yet on npm" banner from `README.md`. The StackBlitz template
becomes independently runnable at the same moment — it has been waiting on this.

---

## Change workflow

Create a changeset with every user-visible package change:

```sh
bun run changeset
```

On `main`, the release workflow maintains a version PR. Merging that PR builds
and publishes changed packages in internal dependency order, then creates git
tags. Changesets owns version calculation and changelogs; the Bun-native
publisher owns packing and registry publication.

Before merging a release PR, validate every package locally:

```sh
bun run release:check
```

Actual publication also verifies npm login, skips versions already present in the
registry, and defaults to the `latest` tag. Set `PERFECT_NPM_TAG` to use a
different distribution tag.

---

## Things that will bite

### Publishing is effectively irreversible

`npm unpublish` is restricted to the first 72 hours, and only while nothing
depends on the package. Treat the first publish as permanent — which is why
step 1 comes first.

### Pinned Rust toolchain

`rust-toolchain.toml`, `ci.yml` and `release.yml` all pin **rustc 1.90.0**. This
is not incidental: on 2026-08-23 the runner's floating `stable` reached 1.98.0
and the `wasm32-wasip1` link began rejecting SWC's host imports —

```
rust-lld: error: undefined symbol: __set_transform_result
```

— which a plugin is supposed to leave undefined for the WASM host to supply at
load time. `release:publish` runs `build:swc`, so an unpinned release would fail
at the wasm build *after* passing every other gate. If you raise the pin, raise
it in all three places and run `bun run build:swc` before trusting it.

### Provenance is granted but not used

`release.yml` grants `id-token: write`, the permission npm provenance
attestations need, but `scripts/publish-packages.ts` publishes with
`bun publish --access public` and never passes `--provenance`. The permission is
currently inert. Provenance cannot be applied retroactively to a published
version, so decide before the first release whether you want it.

### The bare `perfect` name is unavailable

Already taken by an unrelated package. The scope is what matters; there is no
plan to publish an unscoped `perfect`.
