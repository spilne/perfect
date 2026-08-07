import { appendFile, mkdir, writeFile } from "node:fs/promises";

const OUT = "../../.perf/mitata-report.md";

await mkdir("../../.perf", { recursive: true });

const proc = Bun.spawn(["bun", "run", "bench/vs-effect-ts.ts"], {
  stdout: "pipe",
  stderr: "pipe",
  env: {
    ...process.env,
    MITATA_FORMAT: "markdown",
    FORCE_COLOR: "0",
  },
});

const [stdout, stderr, exitCode] = await Promise.all([
  new Response(proc.stdout).text(),
  new Response(proc.stderr).text(),
  proc.exited,
]);

process.stdout.write(stdout);
process.stderr.write(stderr);

const report = stderr.length > 0 ? `${stdout}\n\n## stderr\n\n\`\`\`\n${stderr}\n\`\`\`\n` : stdout;
await writeFile(OUT, report);

const summary = process.env.GITHUB_STEP_SUMMARY;
if (summary !== undefined && summary.length > 0) {
  await appendFile(summary, "\n# Mitata Benchmark Report\n\n");
  await appendFile(summary, report.slice(-50_000));
  await appendFile(summary, "\n");
}

if (exitCode !== 0) {
  process.exit(exitCode);
}
