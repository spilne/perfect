import { defineConfig } from "vitepress";
import container from "markdown-it-container";
import type { RenderRule } from "markdown-it/lib/renderer.mjs";
import { fileURLToPath } from "node:url";

const REPO_URL = "https://github.com/spilne/perfect";
const BRANCH = "main";

const renderSyntaxTabs: RenderRule = (tokens, idx) =>
  tokens[idx].nesting === 1 ? "<SyntaxTabs>\n" : "</SyntaxTabs>\n";

const renderSyntax: RenderRule = (tokens, idx) => {
  if (tokens[idx].nesting !== 1) return "</div>\n";
  const info = tokens[idx].info.trim();
  const match = /^syntax\s+(generator|chainable)\s*$/.exec(info);
  const style = match?.[1] ?? "generator";
  return `<div data-syntax="${style}" class="syntax-tab">\n`;
};

export default defineConfig({
  title: "Perfect",
  description:
    "A TypeScript effect runtime — typed errors, dependency injection, structured concurrency, resource safety.",
  base: "/perfect/",
  cleanUrls: true,
  lastUpdated: true,
  ignoreDeadLinks: "localhostLinks",

  rewrites: {
    "README.md": "index.md",
  },

  head: [
    ["meta", { name: "theme-color", content: "#3c8772" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:title", content: "Perfect" }],
    [
      "meta",
      {
        property: "og:description",
        content: "A TypeScript effect runtime.",
      },
    ],
  ],

  themeConfig: {
    nav: [
      { text: "Guide", link: "/01-getting-started" },
      { text: "Playground", link: "/playground" },
      { text: "Comparison", link: "/comparison" },
      { text: "GitHub", link: REPO_URL },
    ],

    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Getting started", link: "/01-getting-started" },
          { text: "Effects", link: "/02-effects" },
          { text: "Syntax", link: "/03-syntax" },
          { text: "Services and Layers", link: "/04-services-and-layers" },
          { text: "Error handling", link: "/05-error-handling" },
          { text: "Concurrency", link: "/06-concurrency" },
          { text: "Resources and scopes", link: "/07-resources-and-scopes" },
          { text: "Retry and schedule", link: "/08-retry-and-schedule" },
          { text: "Streams", link: "/09-streams" },
          { text: "Testing", link: "/10-testing" },
          {
            text: "Resilience + Coordination",
            link: "/11-resilience-and-coordination",
          },
          { text: "Utilities", link: "/12-utilities" },
          { text: "HTTP", link: "/13-http" },
          { text: "HTTP — OpenTelemetry", link: "/14-http-otel" },
          { text: "Observability", link: "/15-observability" },
          { text: "Messaging and Kafka", link: "/16-messaging" },
          { text: "Redis and PostgreSQL", link: "/17-distributed-backends" },
          { text: "Stateful topologies", link: "/18-topologies" },
          { text: "Package map", link: "/19-packages" },
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "Playground", link: "/playground" },
          { text: "Comparison", link: "/comparison" },
        ],
      },
    ],

    socialLinks: [{ icon: "github", link: REPO_URL }],

    editLink: {
      pattern: `${REPO_URL}/edit/${BRANCH}/documentation/:path`,
      text: "Edit this page on GitHub",
    },

    search: { provider: "local" },

    outline: { level: [2, 3] },
  },

  markdown: {
    config(md) {
      // Rewrite links that escape the documentation/ tree (e.g. `../packages/core/examples/`)
      // to absolute GitHub URLs — VitePress can't resolve them on the static site.
      const defaultNormalize = md.normalizeLink;
      md.normalizeLink = (url: string) => {
        if (url.startsWith("../")) {
          const cleaned = url.replace(/^(\.\.\/)+/, "");
          return `${REPO_URL}/blob/${BRANCH}/${cleaned}`;
        }
        return defaultNormalize(url);
      };

      // `:::: syntax-tabs` (outer, 4 colons) — wraps paired snippets.
      // `::: syntax generator|chainable` (inner, 3 colons) — labels each branch.
      md.use(container, "syntax-tabs", {
        marker: ":",
        validate(params: string) {
          return params.trim() === "syntax-tabs";
        },
        render: renderSyntaxTabs,
      });
      md.use(container, "syntax", {
        marker: ":",
        validate(params: string) {
          return /^syntax\s+(generator|chainable)\s*$/.test(params.trim());
        },
        render: renderSyntax,
      });
    },
  },

  vite: {
    resolve: {
      alias: [
        {
          find: "@perfect/core/stream",
          replacement: fileURLToPath(
            new URL("../../packages/core/src/stream/index.ts", import.meta.url),
          ),
        },
        {
          find: "@perfect/core/retry",
          replacement: fileURLToPath(
            new URL("../../packages/core/src/retry/index.ts", import.meta.url),
          ),
        },
        {
          find: "@perfect/core",
          replacement: fileURLToPath(new URL("../../packages/core/src/index.ts", import.meta.url)),
        },
      ],
    },
  },
});
