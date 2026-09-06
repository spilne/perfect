# Releasing Perfect

Perfect publishes each adapter and runtime component as a separate public package
under the `@spilne` npm scope with a `perfect-` package prefix. Public packages
start at `0.1.0` and use independent semantic versions, so changing Redis does
not force an unrelated Kafka release.

Twelve packages publish: `core`, `http`, `http-otel`, `kafka`, `kafka-kafkajs`,
`kafka-platformatic`, `otel`, `postgres`, `redis`, `swc-plugin`, `topology`,
`transform`. `@spilne/perfect-integration` is private and never publishes.

---

## One-time setup

Verify these prerequisites before a release; repository banners are not a
live registry or account-permission check.

### 1. Verify the npm scope

GitHub and npm organisations are independent. Membership in the `spilne` GitHub
organisation does not grant permission to publish under the `@spilne` npm scope.

Check properly:

```sh
npm login                # browser flow
npm org ls spilne        # must list you as a member
```

Do not continue until the npm account can write to `@spilne`. Changing scope
after release means republishing everything and breaking existing installs.

### 2. Create the npm organisation if needed

Public packages only, so the free plan is enough:

1. <https://www.npmjs.com/org/create>
2. Create the org named `spilne` — this grants the `@spilne` scope.

A personal-account scope (`@<username>`) also works, but an organisation is
preferable here because maintainers can be added without sharing credentials.

### 3. Verify ownership

```sh
npm whoami
npm org ls spilne        # must list you
```

Membership alone does not prove package publishing rights. Verify that your
account has write access to the packages, or permission to create them in the
scope for the first release.

### 4. Validate the release locally

```sh
bun install --frozen-lockfile
bun run ci
bun run release:check
```

Builds every JavaScript package **and** the SWC WASM artifact, dry-run packs all
twelve public packages, and rejects leaked `workspace:*` dependency ranges. This
is the cheapest place to catch problems — it needs no token and touches no
registry.

Requires Rust and the `wasm32-wasip1` target. The repository uses the `stable`
toolchain; see [Rust toolchain](#rust-toolchain) below.

### 5. Configure publishing credentials

The current workflow uses `NPM_TOKEN` and invokes `bun publish`. For that
workflow, create a granular access token with write access to the required
packages or scope, an expiration date, and only the permissions needed.
Organization-management access alone does not grant package publishing rights.
Classic tokens are no longer supported. See [npm's token documentation](https://docs.npmjs.com/about-access-tokens/).

Unattended publication must satisfy the package's 2FA policy. Check the
[current token creation guidance](https://docs.npmjs.com/creating-and-viewing-access-tokens/)
before selecting Bypass 2FA; do not enable it where fully enforced 2FA is
required. Never commit credentials. Moving to trusted publishing requires a
separate workflow change; the existing Bun publisher is not configured for it.

### 6. Wire it into GitHub

```sh
gh secret set NPM_TOKEN --repo spilne/perfect            # paste the token
```

In GitHub repository settings:

- Under **Actions → General → Workflow permissions**, enable **Allow GitHub
  Actions to create and approve pull requests**. The workflow declares the
  write permissions it needs; the repository default can remain read-only.
- Under **Pages → Build and deployment**, set **Source** to **GitHub Actions**.

Leave `NPM_RELEASE_ENABLED` unset for the first release. The release job then
runs only via manual dispatch; pushes to `main` cannot publish automatically.

After merging the release candidate and its changesets into `main`, run:

```sh
gh workflow run release.yml --repo spilne/perfect --ref main
```

With pending changesets, this creates or updates a version PR. Review the
versions, changelogs, and internal dependency updates, then merge that PR.
Run the same command again to publish. **With no pending changesets, a manual
run can publish immediately**; it is not a dry run. Only runs on `main` can
release. The workflow checks all package artifacts, tests the built SWC plugin,
and builds the documentation before attempting publication. Monitor both the
package release and documentation deployment in GitHub's Actions tab.

After the first successful release, optionally enable automatic processing on
pushes to `main`:

```sh
gh variable set NPM_RELEASE_ENABLED --repo spilne/perfect --body true
```

### 7. Verify the first publish

```sh
npm view @spilne/perfect-core
release_smoke_dir=$(mktemp -d)
cd "$release_smoke_dir"
npm init -y
npm install @spilne/perfect-core     # from the real registry
```

Then drop the "not yet on npm" banner from `README.md`. The StackBlitz template
becomes independently runnable at the same moment — it has been waiting on this.

---

## Change workflow

Create a changeset with every user-visible package change:

```sh
bun run changeset
```

When automatic processing is enabled, the release workflow maintains a version
PR on pushes to `main`. Merging that PR builds and publishes unpublished package
versions in internal dependency order, then creates git tags. In manual mode,
dispatch the workflow once to prepare the PR and again after merging to publish.
Changesets owns version calculation and changelogs; the Bun-native publisher
owns packing and registry publication.

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

Do not treat unpublishing as a rollback strategy. npm allows it within the
first 72 hours when registry dependency conditions are met; later removal
has additional eligibility conditions. Check the [npm unpublish policy](https://docs.npmjs.com/policies/unpublish/)
before relying on removal. Prefer a corrected release when users already
depend on the package.

### Rust toolchain

`rust-toolchain.toml`, `ci.yml` and `release.yml` all use **stable** Rust with
the `wasm32-wasip1` target. An earlier SWC version caused the linker error:

```
rust-lld: error: undefined symbol: __set_transform_result
```

Upgrading `swc_core` resolved that issue and removed the need for the temporary
Rust pin. Keep the Rust plugin and JavaScript `@swc/core` host compatible when
upgrading. Always run `bun run release:check` with the release toolchain: the
ordinary `ci` script does not build the WASM artifact.

### Provenance is granted but not used

`release.yml` grants `id-token: write`, the permission npm provenance
attestations need, but `scripts/publish-packages.ts` publishes with
`bun publish --access public` and never passes `--provenance`. The permission is
currently inert. Provenance cannot be applied retroactively to a published
version, so decide before the first release whether you want it.

### Package names do not mirror directories

npm package names have the form `@scope/package`, so the packages publish as
`@spilne/perfect-core`, `@spilne/perfect-kafka`, and so on. Their workspace
directories remain `packages/core`, `packages/kafka`, and so on.
