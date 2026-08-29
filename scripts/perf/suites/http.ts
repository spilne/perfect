// HTTP client suite — @spilne/perfect-http against the clients people reach for.
//
// globalThis.fetch is the baseline on purpose. Beating undici would be a
// meaningless claim (it is a raw transport, we are a typed layer over fetch);
// the useful number is what the Eff wrapper, typed errors and parsing cost on
// top of the fetch underneath.
//
// No thresholds and no gating: absolute HTTP timings are dominated by the
// runner's loopback stack. Even the delta is only indicative — a full
// comparison between two commits that touched no HTTP code still reported
// axios +30% and raw fetch +23%. Read the trend, do not fail a build on it.

import { do_not_optimize } from "mitata";
import axios from "axios";
import nodeFetch from "node-fetch";
import { request as undiciRequest } from "undici";
import { run } from "../../../packages/core/src";
import { DefaultHttpClient, httpRequestJson, identityParser } from "../../../packages/http/src";
import type { BenchCase, Suite } from "./types";

const payload = JSON.stringify({
  id: 1,
  name: "perfect",
  tags: ["effect", "typescript"],
  nested: { a: 1, b: [1, 2, 3] },
});

const body = { hello: "world", n: 42 };

let server: ReturnType<typeof Bun.serve> | undefined;
let base = "";
let client: DefaultHttpClient | undefined;

export const httpSuite: Suite = {
  name: "http",
  // Reported, never fatal — see Suite.gating. Measured on loopback, these
  // swing ±30% between identical trees.
  gating: false,

  setup(): void {
    server = Bun.serve({
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
    base = `http://localhost:${server.port}`;
    client = new DefaultHttpClient({ baseUrl: base });
  },

  teardown(): void {
    server?.stop(true);
    server = undefined;
  },

  cases(): readonly BenchCase[] {
    return [
      {
        name: "GET fetch (baseline)",
        unit: "ns/op",
        divisor: 1,
        run: async () => do_not_optimize(await (await fetch(`${base}/json`)).json()),
      },
      {
        name: "GET undici.request",
        unit: "ns/op",
        divisor: 1,
        run: async () => {
          const res = await undiciRequest(`${base}/json`);
          return do_not_optimize(await res.body.json());
        },
      },
      {
        name: "GET node-fetch",
        unit: "ns/op",
        divisor: 1,
        run: async () => do_not_optimize(await (await nodeFetch(`${base}/json`)).json()),
      },
      {
        name: "GET axios",
        unit: "ns/op",
        divisor: 1,
        run: async () => do_not_optimize((await axios.get(`${base}/json`)).data),
      },
      {
        name: "GET @spilne/perfect-http httpRequestJson",
        unit: "ns/op",
        divisor: 1,
        run: async () =>
          do_not_optimize(await run(httpRequestJson({ url: `${base}/json` }) as any)),
      },
      {
        name: "GET @spilne/perfect-http client",
        unit: "ns/op",
        divisor: 1,
        run: async () => do_not_optimize(await run(client!.get("/json", identityParser) as any)),
      },
      {
        name: "POST fetch (baseline)",
        unit: "ns/op",
        divisor: 1,
        run: async () =>
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
        unit: "ns/op",
        divisor: 1,
        run: async () => do_not_optimize((await axios.post(`${base}/echo`, body)).data),
      },
      {
        name: "POST @spilne/perfect-http client",
        unit: "ns/op",
        divisor: 1,
        run: async () =>
          do_not_optimize(await run(client!.post("/echo", identityParser, { json: body }) as any)),
      },
    ];
  },
};
