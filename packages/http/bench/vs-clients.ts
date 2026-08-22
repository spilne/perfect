// @perfect/http against the clients people actually reach for.
//
// The point is NOT "we beat undici" — undici is a raw transport and
// @perfect/http is a typed layer over global fetch. The honest question is what
// the Eff machinery, typed errors and response parsing cost on top of the fetch
// underneath, so `globalThis.fetch` is the baseline every row is measured
// against.
//
// Everything runs against a local Bun.serve on loopback, so the network is not
// the variable. Numbers are per-request wall time including body parsing.
//
// Methodology note: a naive run measured the baseline FIRST and made it look
// slowest, because it absorbed JIT and connection warmup that later rows then
// benefited from. Every client is therefore warmed up before ANY measurement,
// and the whole set is measured twice with the second pass reported.
//
//   bun run bench/vs-clients.ts
//   BENCH_SAMPLES=100 bun run bench/vs-clients.ts

import { do_not_optimize, measure } from "mitata";
import axios from "axios";
import nodeFetch from "node-fetch";
import { request as undiciRequest } from "undici";
import { run } from "@perfect/core";
import { DefaultHttpClient, httpRequestJson, identityParser } from "../src";

const SAMPLES = Number(process.env.BENCH_SAMPLES ?? "50");
const WARMUP = Number(process.env.BENCH_WARMUP ?? "10");
const WARMUP_REQUESTS = Number(process.env.BENCH_WARMUP_REQUESTS ?? "60");

const payload = JSON.stringify({
  id: 1,
  name: "perfect",
  tags: ["effect", "typescript"],
  nested: { a: 1, b: [1, 2, 3] },
});

const server = Bun.serve({
  port: 0,
  fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/json") {
      return new Response(payload, { headers: { "content-type": "application/json" } });
    }
    if (url.pathname === "/echo") {
      return new Response(req.body, { headers: { "content-type": "application/json" } });
    }
    return new Response("not found", { status: 404 });
  },
});

const base = `http://localhost:${server.port}`;
const client = new DefaultHttpClient({ baseUrl: base });
const body = { hello: "world", n: 42 };

type Case = { readonly name: string; readonly fn: () => Promise<unknown> };
type Row = { name: string; median: number; p99: number };

const cases: Case[] = [
  // ── GET + JSON parse ────────────────────────────────────────────
  {
    name: "globalThis.fetch (baseline)",
    fn: async () => do_not_optimize(await (await fetch(`${base}/json`)).json()),
  },
  {
    name: "undici.request",
    fn: async () => {
      const res = await undiciRequest(`${base}/json`);
      return do_not_optimize(await res.body.json());
    },
  },
  {
    name: "node-fetch",
    fn: async () => do_not_optimize(await (await nodeFetch(`${base}/json`)).json()),
  },
  {
    name: "axios",
    fn: async () => do_not_optimize((await axios.get(`${base}/json`)).data),
  },
  {
    name: "@perfect/http httpRequestJson",
    fn: async () => do_not_optimize(await run(httpRequestJson({ url: `${base}/json` }) as any)),
  },
  {
    name: "@perfect/http DefaultHttpClient",
    fn: async () => do_not_optimize(await run(client.get("/json", identityParser) as any)),
  },

  // ── POST + echo ─────────────────────────────────────────────────
  {
    name: "POST globalThis.fetch",
    fn: async () =>
      do_not_optimize(
        await (
          await fetch(`${base}/echo`, {
            method: "POST",
            body: JSON.stringify(body),
            headers: { "content-type": "application/json" },
          })
        ).json(),
      ),
  },
  {
    name: "POST axios",
    fn: async () => do_not_optimize((await axios.post(`${base}/echo`, body)).data),
  },
  {
    name: "POST @perfect/http",
    fn: async () =>
      do_not_optimize(await run(client.post("/echo", identityParser, { json: body }) as any)),
  },
];

async function benchAll(): Promise<Row[]> {
  const rows: Row[] = [];
  for (const c of cases) {
    const stats = await measure(c.fn, {
      min_samples: SAMPLES,
      max_samples: SAMPLES,
      warmup_samples: WARMUP,
    });
    rows.push({ name: c.name, median: stats.p50 / 1_000, p99: stats.p99 / 1_000 });
  }
  return rows;
}

// Warm every client before measuring any of them, so no row pays for another
// row's connection setup or JIT tiering.
for (const c of cases) {
  for (let i = 0; i < WARMUP_REQUESTS; i++) await c.fn();
}

await benchAll(); // discarded — settles the remaining tiering
const rows = await benchAll();

server.stop(true);

// ── Report ────────────────────────────────────────────────────────

const baseline = rows.find((r) => r.name === "globalThis.fetch (baseline)")!.median;
const width = Math.max(...rows.map((r) => r.name.length));

console.log(
  `\nSamples: ${SAMPLES} (mitata warmup ${WARMUP}, ${WARMUP_REQUESTS} priming requests per client)`,
);
console.log("Local Bun.serve on loopback; two measurement passes, second reported.\n");
console.log(`${"client".padEnd(width)}  ${"median".padStart(10)}  ${"p99".padStart(10)}  vs fetch`);
console.log("-".repeat(width + 36));
for (const row of rows) {
  console.log(
    `${row.name.padEnd(width)}  ${`${row.median.toFixed(1)} µs`.padStart(10)}  ${`${row.p99.toFixed(1)} µs`.padStart(10)}  ${(row.median / baseline).toFixed(2)}×`,
  );
}
console.log(
  "\nThe number that matters is @perfect/http vs the fetch baseline: that gap is\n" +
    "the cost of the Eff wrapper, typed errors and parsing — not of the network.\n",
);
