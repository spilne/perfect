# Changesets

Add a changeset for every user-visible change to a public `@perfect/*` package:

```sh
bun run changeset
```

Packages use independent semantic versions. Choose the smallest accurate bump for
each affected package; internal dependency ranges are updated automatically.
