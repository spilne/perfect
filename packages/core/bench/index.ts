import { succeed, run, runSync } from "../src";
import type { Eff } from "../src";

const N = 10_000;
const ROUNDS = 100;

// ── Spilne: sync flatMap chain ─────────────────────────────────────
function buildChain(n: number): Eff<number, never> {
  let eff: Eff<number, never> = succeed(0);
  for (let i = 0; i < n; i++) {
    eff = eff.flatMap((x) => succeed(x + 1));
  }
  return eff;
}

// ── Promise: equivalent chain ──────────────────────────────────────
function buildPromiseChain(n: number): Promise<number> {
  let p: Promise<number> = Promise.resolve(0);
  for (let i = 0; i < n; i++) {
    p = p.then((x) => x + 1);
  }
  return p;
}

// ── Raw loop (baseline) ────────────────────────────────────────────
function rawLoop(n: number): number {
  let x = 0;
  for (let i = 0; i < n; i++) x += 1;
  return x;
}

async function bench(name: string, fn: () => any | Promise<any>, rounds: number) {
  // warmup
  for (let i = 0; i < 5; i++) await fn();

  const times: number[] = [];
  for (let i = 0; i < rounds; i++) {
    const start = Bun.nanoseconds();
    await fn();
    const end = Bun.nanoseconds();
    times.push(end - start);
  }

  times.sort((a, b) => a - b);
  const median = times[Math.floor(times.length / 2)]!;
  const p99 = times[Math.floor(times.length * 0.99)]!;
  const nsPerOp = Math.round(median / N);

  console.log(
    `${name.padEnd(30)} median: ${(median / 1e6).toFixed(2).padStart(8)}ms` +
      `  p99: ${(p99 / 1e6).toFixed(2).padStart(8)}ms` +
      `  ns/op: ${String(nsPerOp).padStart(6)}`,
  );
}

console.log(`\nBenchmark: ${N} flatMap/then chain × ${ROUNDS} rounds\n`);

await bench("raw loop (baseline)", () => rawLoop(N), ROUNDS);
await bench("spilne runSync", () => runSync(buildChain(N)), ROUNDS);
await bench("spilne run (async)", () => run(buildChain(N)), ROUNDS);
await bench("Promise.then chain", () => buildPromiseChain(N), ROUNDS);

console.log();
