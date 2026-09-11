import { afterEach, expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "../..");
const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function command(options: { cwd: string; args: string[]; version?: string; ref?: string }) {
  const child = Bun.spawn(options.args, {
    cwd: options.cwd,
    env: {
      ...process.env,
      BUN_INSTALL_CACHE_DIR: join(options.cwd, ".bun-cache"),
      VERSION: options.version ?? "",
      GITHUB_REF: options.ref ?? "",
      NX_DAEMON: "false",
      NX_ISOLATE_PLUGINS: "false",
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { code, output: stdout + stderr };
}

async function fixture() {
  const cwd = await mkdtemp(join(tmpdir(), "perfect-release-test-"));
  directories.push(cwd);
  for (const directory of [
    "scripts",
    "packages/core",
    "packages/http",
    "packages/integration",
    "templates/playground",
  ]) {
    await mkdir(join(cwd, directory), { recursive: true });
  }
  await symlink(join(root, "node_modules"), join(cwd, "node_modules"), "dir");
  await writeFile(
    join(cwd, "scripts/release.ts"),
    await readFile(join(root, "scripts/release.ts")),
  );
  await writeFile(join(cwd, "nx.json"), await readFile(join(root, "nx.json")));
  await writeFile(join(cwd, ".gitignore"), "node_modules/\n.nx/\n.bun-cache/\n");
  await writeFile(
    join(cwd, "package.json"),
    JSON.stringify({
      name: "release-fixture",
      private: true,
      workspaces: ["packages/*", "templates/*"],
      packageManager: "bun@1.4.0",
      scripts: { ci: "bun --version", "release:check": "bun --version" },
    }),
  );
  for (const name of ["core", "http", "integration"]) {
    await writeFile(
      join(cwd, `packages/${name}/package.json`),
      JSON.stringify({
        name: `@spilne/perfect-${name}`,
        version: "0.1.0",
        ...(name === "integration" ? { private: true } : {}),
        ...(name === "http" ? { dependencies: { "@spilne/perfect-core": "^0.1.0" } } : {}),
      }),
    );
  }
  await writeFile(
    join(cwd, "templates/playground/package.json"),
    JSON.stringify({
      name: "playground",
      private: true,
      version: "0.1.0",
      dependencies: { "@spilne/perfect-core": "^0.1.0" },
    }),
  );
  for (const args of [
    ["git", "init", "-b", "main"],
    ["git", "config", "user.email", "release@example.test"],
    ["git", "config", "user.name", "Release Test"],
    ["git", "add", "."],
    ["git", "commit", "-m", "feat: initial packages"],
  ]) {
    const result = await command({ cwd, args });
    expect(result.code, result.output).toBe(0);
  }
  return cwd;
}

test("Nx prepares the first shared release, then defaults to patch, with a frozen Bun lockfile", async () => {
  const cwd = await fixture();
  const release = (version?: string) =>
    command({ cwd, args: ["bun", "scripts/release.ts"], version });
  const first = await release("0.1.0");
  expect(first.code, first.output).toBe(0);
  const next = await release();
  expect(next.code, next.output).toBe(0);
  const core = await Bun.file(join(cwd, "packages/core/package.json")).json();
  const http = await Bun.file(join(cwd, "packages/http/package.json")).json();
  const integration = await Bun.file(join(cwd, "packages/integration/package.json")).json();
  expect(core.version).toBe("0.1.1");
  expect(http.version).toBe("0.1.1");
  expect(http.dependencies[core.name]).toBe("^0.1.1");
  expect(integration.version).toBe("0.1.0");
  const status = await command({ cwd, args: ["git", "status", "--porcelain"] });
  expect(status.output).toBe("");
  const frozen = await command({
    cwd,
    args: ["bun", "install", "--lockfile-only", "--frozen-lockfile"],
  });
  expect(frozen.code, frozen.output).toBe(0);
  const verify = await command({
    cwd,
    args: ["bun", "scripts/release.ts", "--verify-tag"],
    ref: "refs/tags/v0.1.1",
  });
  expect(verify.code, verify.output).toBe(0);
  const wrongTag = await command({
    cwd,
    args: ["bun", "scripts/release.ts", "--verify-tag"],
    ref: "refs/tags/v0.2.0",
  });
  expect(wrongTag.code).not.toBe(0);
  const minor = await release("0.2.0");
  expect(minor.code, minor.output).toBe(0);
  const playground = await Bun.file(join(cwd, "templates/playground/package.json")).json();
  expect(playground.dependencies[core.name]).toBe("^0.2.0");
  const minorFrozen = await command({
    cwd,
    args: ["bun", "install", "--lockfile-only", "--frozen-lockfile"],
  });
  expect(minorFrozen.code, minorFrozen.output).toBe(0);
  const older = await release("0.1.0");
  expect(older.code).not.toBe(0);
}, 30_000);

test("dirty releases fail before mutation, while dry runs leave files and tags untouched", async () => {
  const cwd = await fixture();
  await writeFile(join(cwd, "pending.txt"), "work in progress");
  const dirty = await command({ cwd, args: ["bun", "scripts/release.ts"] });
  expect(dirty.code).not.toBe(0);
  expect(dirty.output).toContain("Commit or stash");
  const dry = await command({
    cwd,
    args: ["bun", "scripts/release.ts", "--dry-run"],
    version: "0.2.0",
  });
  expect(dry.code, dry.output).toBe(0);
  expect((await Bun.file(join(cwd, "packages/core/package.json")).json()).version).toBe("0.1.0");
  expect((await command({ cwd, args: ["git", "tag", "--list"] })).output).toBe("");
}, 30_000);
