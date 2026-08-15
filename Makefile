.PHONY: install test test-core test-transform lint fmt fmt-check typecheck build smoke bench bench-vs-effect perf-report perf-gate docs docs-check docs-dev docs-build docs-preview build-swc ci

install:
	bun install

test:
	bun run test

test-core:
	bun run test:core

test-transform:
	bun run test:transform

lint:
	bun run lint

fmt:
	bun run fmt

fmt-check:
	bun run fmt:check

typecheck:
	bun run typecheck

build:
	bun run build:packages

smoke:
	bun run smoke:packages

bench:
	bun run bench

bench-vs-effect:
	bun run bench:vs-effect

perf-report:
	bun run perf:report

perf-gate:
	bun run perf:gate

docs:
	bun run documentation:build

docs-check:
	bun run documentation:check

docs-dev:
	bun run documentation:dev

docs-build:
	bun run documentation:build:html

docs-preview:
	bun run documentation:preview

build-swc:
	bun run build:swc

ci: fmt-check lint typecheck test build smoke docs-check perf-gate
