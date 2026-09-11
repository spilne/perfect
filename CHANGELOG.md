## 0.1.0 (2026-09-11)

### 🚀 Features

- **core:** add Promin stream conveniences and retry API refinements ([6bee020](https://github.com/spilne/perfect/commit/6bee020))
- **core:** add Stream.scanEffect and Pipe constructors ([e503177](https://github.com/spilne/perfect/commit/e503177))
- **core:** add RawStream escape hatch ([c98be73](https://github.com/spilne/perfect/commit/c98be73))
- **core:** add Layer.build auto-wiring with cycle detection ([3ab0c5f](https://github.com/spilne/perfect/commit/3ab0c5f))
- **core:** add FileSystem service and Console.readLine ([81d7081](https://github.com/spilne/perfect/commit/81d7081))
- **docs:** add Stream playground template and docs integration ([0dec88c](https://github.com/spilne/perfect/commit/0dec88c))
- **http:** add retryHttp docs and Retry namespace examples ([4ec61f9](https://github.com/spilne/perfect/commit/4ec61f9))
- **release:** use shared Nx versions and tag-triggered publishing ([0bc5aca](https://github.com/spilne/perfect/commit/0bc5aca))

### 🩹 Fixes

- **ci:** repair the three failures in the first real CI run ([1111610](https://github.com/spilne/perfect/commit/1111610))
- **ci:** upload hidden .perf artifact, and de-flake the Redis breaker test ([4b3a1fd](https://github.com/spilne/perfect/commit/4b3a1fd))
- **core:** repair the three standing typecheck errors ([da72c9c](https://github.com/spilne/perfect/commit/da72c9c))
- **http:** keep the response body readable after the fetch effect ([3f2277b](https://github.com/spilne/perfect/commit/3f2277b))
- **perf:** stop gating on benchmarks below the runner's timing floor ([0dc2138](https://github.com/spilne/perfect/commit/0dc2138))
- **playground:** use retry config object in source retry scenario ([19af0dd](https://github.com/spilne/perfect/commit/19af0dd))
- **release:** pin the Rust toolchain and stop pinning a stale Bun ([7413132](https://github.com/spilne/perfect/commit/7413132))
- **release:** pass npm credentials through Bun token variable ([7877758](https://github.com/spilne/perfect/commit/7877758))
- **zed:** stop the extension hijacking every TypeScript file ([0ccf3c0](https://github.com/spilne/perfect/commit/0ccf3c0))

### 🔥 Performance

- replace the absolute gate with a same-run baseline comparison ([c0e805b](https://github.com/spilne/perfect/commit/c0e805b))
- **gate:** tighten thresholds from 4-233x headroom to 4-13x ([f703e12](https://github.com/spilne/perfect/commit/f703e12))

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
