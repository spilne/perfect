## 0.2.0 (2026-09-18)

### 🚀 Features

- **core:** add Stream.toAsyncIterable ([99fbfd0](https://github.com/spilne/perfect/commit/99fbfd0))
- **core:** add bounded-concurrency forEachPar ([c3479f4](https://github.com/spilne/perfect/commit/c3479f4))
- **core:** add concurrent stream flattening ([d3be6cc](https://github.com/spilne/perfect/commit/d3be6cc))
- **core:** add uninterruptibleMask ([20294f7](https://github.com/spilne/perfect/commit/20294f7))

### 🩹 Fixes

- **build:** emit explicit .js extensions in published JS and declarations ([eacf9ca](https://github.com/spilne/perfect/commit/eacf9ca))
- **ci:** stabilize Redis tests and restore Node test spies ([2206436](https://github.com/spilne/perfect/commit/2206436))
- **core:** ignore repeated interrupt() until the queued one is delivered ([992c3b2](https://github.com/spilne/perfect/commit/992c3b2))
- **core:** address toAsyncIterable review feedback ([472264c](https://github.com/spilne/perfect/commit/472264c))
- **core:** make forEachPar lazy, drain-aware, and cheaper per item ([1068fc5](https://github.com/spilne/perfect/commit/1068fc5))
- **core:** keep finalizers when a pending interrupt meets a finalizer boundary ([47117ba](https://github.com/spilne/perfect/commit/47117ba))
- **core:** ignore callbacks from a wait the fiber has already left ([7eb3580](https://github.com/spilne/perfect/commit/7eb3580))
- **core:** interrupt a queued or running fiber without a second loop run ([cb09d64](https://github.com/spilne/perfect/commit/cb09d64))
- **core:** harden parJoin retry, validation, and finalizer failures ([df1523b](https://github.com/spilne/perfect/commit/df1523b))
- **core:** keep stream operator fibers alive across racing pulls ([67a7188](https://github.com/spilne/perfect/commit/67a7188))
- **core:** look past empty chunks in head, collectFirst and last ([6b865ac](https://github.com/spilne/perfect/commit/6b865ac))
- **core:** reject invalid stream counts and windows instead of clamping ([1b3b0da](https://github.com/spilne/perfect/commit/1b3b0da))
- **core:** keep an interrupted fiber from recovering through error handlers ([e829772](https://github.com/spilne/perfect/commit/e829772))
- **core:** close interrupt() edge cases around dropped runs and cancelers ([4aefb4b](https://github.com/spilne/perfect/commit/4aefb4b))
- **core:** keep stream run fibers owned until they have stopped ([124b6f4](https://github.com/spilne/perfect/commit/124b6f4))
- **core:** resume only retried stream pulls and keep delivered failures ([81e7a9b](https://github.com/spilne/perfect/commit/81e7a9b))
- **core:** validate the remaining stream durations ([d280bae](https://github.com/spilne/perfect/commit/d280bae))
- **core:** keep forEachPar waiting for children under sticky interruption ([4c06bd4](https://github.com/spilne/perfect/commit/4c06bd4))
- **core:** give back a value handed to a waiter interrupted before it runs ([495a1d9](https://github.com/spilne/perfect/commit/495a1d9))
- **core:** return from all() and race() only after their children finish ([72b076a](https://github.com/spilne/perfect/commit/72b076a))
- **core:** keep a Ready fiber's pending failure when it is interrupted ([d370b4b](https://github.com/spilne/perfect/commit/d370b4b))
- **core:** settle ordered parEvalMap slots when a worker is interrupted ([e7bac55](https://github.com/spilne/perfect/commit/e7bac55))
- **core:** install cleanup before singleflight and generators start work ([71a3e90](https://github.com/spilne/perfect/commit/71a3e90))
- **core:** keep stream run bookkeeping working under sticky interruption ([b2dd423](https://github.com/spilne/perfect/commit/b2dd423))
- **core:** count every step against the op budget again ([bdda6c0](https://github.com/spilne/perfect/commit/bdda6c0))
- **core:** keep a queue FIFO when several takers give items back ([7dff8bd](https://github.com/spilne/perfect/commit/7dff8bd))
- **core:** keep a Pool slot counted until a rejected resource is replaced ([5cc6e5f](https://github.com/spilne/perfect/commit/5cc6e5f))
- **core:** guard a throwing onDiscard and settle all()/race() causes the same way ([b10879d](https://github.com/spilne/perfect/commit/b10879d))
- **core:** enable the handoff tie tests and join cut-pull cleanup failures ([7142253](https://github.com/spilne/perfect/commit/7142253))
- **core:** run forEachPar on ChildGroup ([b736594](https://github.com/spilne/perfect/commit/b736594))
- **core:** keep a failing takeUntil signal when the cut pull's cleanup fails ([88593cd](https://github.com/spilne/perfect/commit/88593cd))
- **core:** fail switchMap when a switched-out inner fails to clean up ([216f42f](https://github.com/spilne/perfect/commit/216f42f))
- **core:** fail stream operators when a source interrupts itself ([ee4742a](https://github.com/spilne/perfect/commit/ee4742a))
- **core:** keep interrupting a fiber whose canceler throws ([54e0faa](https://github.com/spilne/perfect/commit/54e0faa))
- **core:** raise an interrupt that arrives during the fiber scope close ([01ed644](https://github.com/spilne/perfect/commit/01ed644))
- **core:** fail retried pulls again when a delivered cause holds an interrupt ([6803edf](https://github.com/spilne/perfect/commit/6803edf))
- **core:** keep a throwing canceler's defect when the fiber completes early ([289f45d](https://github.com/spilne/perfect/commit/289f45d))
- **core:** let a takeUntil signal that ends empty leave the source pull alone ([b30439e](https://github.com/spilne/perfect/commit/b30439e))
- **core:** make every read of a forEachPar item a defect when it throws ([edbb9b1](https://github.com/spilne/perfect/commit/edbb9b1))
- **core:** settle a stopped ChildGroup only once its stop hook has run ([0c71da2](https://github.com/spilne/perfect/commit/0c71da2))
- **core:** keep a throwing onDiscard's defect when it interrupts its fiber ([7372722](https://github.com/spilne/perfect/commit/7372722))
- **core:** export the public types the root barrel was hiding ([d3ff124](https://github.com/spilne/perfect/commit/d3ff124))
- **core:** spawn the worker executor from a path the published package ships ([ba085ea](https://github.com/spilne/perfect/commit/ba085ea))
- **http:** report interrupted requests to middleware ([f595839](https://github.com/spilne/perfect/commit/f595839))
- **perf:** warm the stream traversal benchmark past its settle ([ff0bf90](https://github.com/spilne/perfect/commit/ff0bf90))
- **perf:** confirm a flagged regression before failing the build ([9f5bd39](https://github.com/spilne/perfect/commit/9f5bd39))
- **postgres:** unlisten when a change stream subscriber is interrupted as LISTEN completes ([63797e6](https://github.com/spilne/perfect/commit/63797e6))
- **redis:** push back an item or token popped for an interrupted waiter ([2826cab](https://github.com/spilne/perfect/commit/2826cab))
- **redis:** keep locks, permits and breaker probes safe under interruption ([a9e04b8](https://github.com/spilne/perfect/commit/a9e04b8))
- **redis:** decode a RedisQueue item in the step that receives it ([095d824](https://github.com/spilne/perfect/commit/095d824))

### 🔥 Performance

- **core:** avoid temporary arrays in stream chunk traversal ([a55b92d](https://github.com/spilne/perfect/commit/a55b92d))
- **core:** close eff() generators without tracking their state ([823fa89](https://github.com/spilne/perfect/commit/823fa89))
- **core:** count only effects, not values, against the op budget ([3cd466a](https://github.com/spilne/perfect/commit/3cd466a))
- **core:** mark a pause on a value without adding a Fiber field ([30e2785](https://github.com/spilne/perfect/commit/30e2785))
- **core:** trim ChildGroup bookkeeping for all() and race() ([2a55a41](https://github.com/spilne/perfect/commit/2a55a41))
- **core:** skip the TaggedError payload serialisation when props carry a message ([4987587](https://github.com/spilne/perfect/commit/4987587))

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
