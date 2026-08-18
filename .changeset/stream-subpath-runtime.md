---
"@perfect/core": patch
---

Initialize fluent effect syntax when consumers import `@perfect/core/stream`
directly, cover the standalone subpath with the package export smoke test, and
complete the Promin-compatible stream convenience surface: custom equality for
`changes`, pacing and pausing, factory repetition, multi-stream merge, detached
taps, full-Cause helpers, terminal collection helpers, and typed whole-stream
deadlines. `RetryPolicy.exponential` now accepts object options and the builder
includes equal jitter; retry primitives are also available from the focused
`@perfect/core/retry` subpath.
