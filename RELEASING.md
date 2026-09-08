# Releasing Perfect

All public `@spilne/perfect-*` packages share one version and release together.
Nx Release updates manifests, internal dependency ranges, the Bun lockfile, and
`CHANGELOG.md`, then creates one release commit and a `vX.Y.Z` Git tag.
The private playground follows the version so its dependencies stay current;
the private integration test package is excluded from versioning. Neither publishes.

## One-time setup

- Install Bun, Node.js, and the repository's stable Rust toolchain with the
  `wasm32-wasip1` target. Run `bun install --frozen-lockfile`.
- Configure the GitHub Actions repository secret `NPM_TOKEN` with publishing
  access to the `@spilne` scope. For unattended token-based publishing, follow
  [npm's token guidance](https://docs.npmjs.com/creating-and-viewing-access-tokens/).
- Set GitHub Pages' source to **GitHub Actions** for documentation deployment.

`NPM_RELEASE_ENABLED` is no longer used. The workflow does not create version PRs
and does not publish on branch pushes. Pushing a release tag is the publishing trigger.

## First release

Commit and merge the release tooling into `main`, then start from a clean,
up-to-date checkout of `main`:

```sh
git pull --ff-only
make release-dry VERSION=0.1.0
make release VERSION=0.1.0
git push --atomic origin main v0.1.0
```

The public manifests already contain `0.1.0`. The command detects the first
release automatically and permits that existing version without bumping it.
Dry runs preview changes without validation builds, file writes, commits, or tags.

## Subsequent releases

```sh
make release                       # patch bump, e.g. 0.1.0 -> 0.1.1
make release VERSION=minor         # e.g. 0.1.1 -> 0.2.0
make release VERSION=0.3.0         # explicit stable version
make release-dry VERSION=0.3.0     # preview only
```

Choose one release command. It runs `bun run ci` and `bun run release:check`
before changing versions. It requires a clean working tree on `main` and does
not push or publish. On success it prints the exact atomic push command.
The version must increase after the first release; prereleases are not currently supported.

Nx generates changelog entries from commit history. Use descriptive `feat:`,
`fix:`, or `perf:` commits for useful release notes. The selected version or bump
controls versioning; no changeset files are required.

If validation or versioning fails, inspect `git status` and any partial changes
before retrying. Do not push a tag until the release command succeeds.
If branch protection prevents pushing the release commit, merge that exact
commit through a PR without squashing or rebasing it, then push its tag.

## What happens after pushing

The Release workflow:

1. Verifies that the tag matches every public package version and points to the
   checked-out commit, and checks internal dependency ranges.
2. Runs CI, builds all release artifacts, dry-run packs every public package,
   tests the SWC plugin, and builds the documentation.
3. Publishes public packages in dependency order using the npm `latest` tag.
4. Creates one GitHub Release with generated notes, then deploys documentation.

The publisher skips package versions already present on npm, so a partially
failed workflow can be retried using **Re-run failed jobs** on the same tag.
It also leaves an existing GitHub Release in place. Do not move a released tag;
use a new version for fixes.

Monitor the Release workflow in GitHub Actions. After the first successful
publish, verify `npm view @spilne/perfect-core version` and a fresh install,
then remove the pre-publication banner from `README.md`.

`bun run release:check` can be run separately to build and inspect artifacts
without npm credentials. Publishing remains Bun-based; npm provenance and
trusted publishing are not configured by this change.
