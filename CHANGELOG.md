## Pre-release development

Add typed stream recovery, source-reacquiring `retryFrom`, effect-preserving
queue bridges, `takeUntil`, and reliable single-pass `observe`/
`broadcastThrough` with structured finalization. Harden CSV parsing across
chunk boundaries and make base64 pipes binary-safe with UTF-8 text helpers.

Initialize fluent effect syntax when consumers import `@spilne/perfect-core/stream`
directly, cover the standalone subpath with the package export smoke test, and
complete the Promin-compatible stream convenience surface: custom equality for
`changes`, pacing and pausing, factory repetition, multi-stream merge, detached
taps, full-Cause helpers, terminal collection helpers, and typed whole-stream
deadlines. `RetryPolicy.exponential` now accepts object options and the builder
includes equal jitter; retry primitives are also available from the focused
`@spilne/perfect-core/retry` subpath.
