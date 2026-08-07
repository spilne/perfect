.PHONY: install test test-core test-transform lint fmt fmt-check typecheck build smoke bench bench-vs-effect perf-report perf-gate docs docs-check docs-dev docs-build docs-preview build-rust build-swc ci

install:
	bun install

test:
	bun test --recursive packages/

test-core:
	cd packages/core && bun test

test-transform:
	cd packages/transform && bun test

lint:
	oxlint -c .oxlintrc.json packages/

fmt:
	oxfmt packages/ '!**/dist/**'

fmt-check:
	oxfmt --check packages/ '!**/dist/**'

typecheck:
	bun run typecheck

build:
	bun run build:packages

smoke:
	bun scripts/smoke-package-exports.ts

bench:
	cd packages/core && bun run bench/index.ts

bench-vs-effect:
	cd packages/core && bun run bench/vs-effect-ts.ts

perf-report:
	cd packages/core && bun run bench/perf-report.ts

perf-gate:
	cd packages/core && bun run bench/perf-gate.ts

docs:
	bun documentation/build.ts

docs-check:
	bun documentation/build.ts --check

docs-dev:
	bun documentation/build.ts
	vitepress dev documentation

docs-build:
	bun documentation/build.ts
	vitepress build documentation

docs-preview:
	vitepress preview documentation

build-rust:
	cd crates/perfect-transform && cargo build --release

build-swc:
	cd crates/swc-plugin-perfect && cargo build --target wasm32-wasip1 --release
	mkdir -p packages/swc-plugin/dist
	cp crates/swc-plugin-perfect/target/wasm32-wasip1/release/swc_plugin_perfect.wasm packages/swc-plugin/dist/plugin.wasm

ci: fmt-check lint typecheck test build smoke docs-check perf-gate
