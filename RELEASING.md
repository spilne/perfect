# Releasing Perfect

Perfect publishes each adapter and runtime component as a separate public package
under the `@perfect` npm scope. Public packages start at `0.1.0` and use independent
semantic versions, so changing Redis does not force an unrelated Kafka release.

## One-time setup

1. Confirm the publishing account or npm organization controls the `@perfect`
   scope. `@perfect/core` is currently unpublished, but an authenticated ownership
   check is still required.
2. Add an npm automation token as the `NPM_TOKEN` repository secret.
3. Set the `NPM_RELEASE_ENABLED` repository variable to `true` after the first two
   checks. Until then, the release workflow can only be started manually.

## Change workflow

Create a changeset with every user-visible package change:

```sh
bun run changeset
```

On `main`, the release workflow maintains a version PR. Merging that PR builds and
publishes changed packages in internal dependency order, then creates git tags.
Changesets owns version calculation and changelogs; the Bun-native publisher owns
packing and registry publication.

Before merging a release PR, validate every package locally:

```sh
bun run release:check
```

The check builds JavaScript packages and the SWC WASM artifact, then dry-run packs
every public package and rejects leaked workspace dependency ranges. Actual
publication also verifies npm login, skips versions already present in the registry,
and defaults to the `latest` tag.
Set `PERFECT_NPM_TAG` to use a different distribution tag.
