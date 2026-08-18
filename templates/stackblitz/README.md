# Perfect StackBlitz playground

This standalone Vite project is the source for Perfect's one-click StackBlitz
starter. It provides editable presets for concurrency, typed failures, source
retry, state, cancellation, single-pass observation, and reactive composition
without requiring a compiler plugin. Edited code runs in a disposable Web Worker
with a five-second limit. The editor exposes `Stream`, `RetryPolicy`, `delay`,
`succeed`, and `fail` as globals.

The project intentionally depends on the registry package rather than a relative
workspace path because StackBlitz imports only this directory. It becomes directly
runnable when `@perfect/core@0.1.0` is published.

From the Perfect repository, validate the template against the local package:

```bash
bun run build:packages
bun run smoke:stackblitz
```

After publication, this directory also runs independently:

```bash
npm install
npm run dev
```
