import { resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const stableVersion = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

function git(args: string[]): string {
  const result = Bun.spawnSync(["git", ...args], { cwd: root });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString().trim());
  return result.stdout.toString().trim();
}

async function run(command: string[]): Promise<void> {
  const child = Bun.spawn(command, {
    cwd: root,
    env: { ...process.env, NX_DAEMON: "false", NX_ISOLATE_PLUGINS: "false" },
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  });
  if ((await child.exited) !== 0) throw new Error(`Failed: ${command.join(" ")}`);
}

const packages = await Promise.all(
  Array.from(new Bun.Glob("packages/*/package.json").scanSync({ cwd: root })).map(async (path) =>
    Bun.file(resolve(root, path)).json(),
  ),
);
const publicPackages = packages.filter((manifest) => !manifest.private);
const versions = new Set(publicPackages.map((manifest) => manifest.version as string));
if (versions.size !== 1) throw new Error("All public packages must have one shared version");
const current = [...versions][0]!;
if (!stableVersion.test(current)) throw new Error(`Expected a stable version, got ${current}`);

const args = process.argv.slice(2);
if (args.includes("--verify-tag")) {
  const tag = `v${current}`;
  if (process.env.GITHUB_REF && process.env.GITHUB_REF !== `refs/tags/${tag}`) {
    throw new Error(`Release must run from refs/tags/${tag}, got ${process.env.GITHUB_REF}`);
  }
  if (git(["rev-parse", `${tag}^{commit}`]) !== git(["rev-parse", "HEAD"])) {
    throw new Error(`${tag} must point to HEAD`);
  }
  for (const manifest of publicPackages) {
    for (const group of [
      "dependencies",
      "devDependencies",
      "peerDependencies",
      "optionalDependencies",
    ]) {
      for (const dependency of publicPackages) {
        const range = manifest[group]?.[dependency.name];
        if (range && ![current, `^${current}`, `~${current}`, `=${current}`].includes(range)) {
          throw new Error(
            `${manifest.name}: ${dependency.name} must reference ${current}, got ${range}`,
          );
        }
      }
    }
  }
  console.log(`Verified ${tag}: ${publicPackages.length} public packages at ${current}`);
} else {
  if (args.some((arg) => arg !== "--dry-run")) throw new Error("Use VERSION=x.y.z make release");
  const dryRun = args.includes("--dry-run");
  const version = process.env.VERSION?.trim() || "patch";
  if (!["patch", "minor", "major"].includes(version) && !stableVersion.test(version)) {
    throw new Error("VERSION must be patch, minor, major, or a stable x.y.z version");
  }
  const firstRelease = !git(["tag", "--list", "v*"])
    .split("\n")
    .some((tag) => stableVersion.test(tag.slice(1)));
  if (stableVersion.test(version)) {
    if (!Bun.semver.satisfies(version, `>${current}`) && !(firstRelease && version === current)) {
      throw new Error(`VERSION must be newer than ${current}`);
    }
    if (git(["tag", "--list", `v${version}`])) throw new Error(`Tag v${version} already exists`);
  }
  if (!dryRun) {
    if (git(["branch", "--show-current"]) !== "main") throw new Error("Release from main");
    if (git(["status", "--porcelain"])) throw new Error("Commit or stash changes before releasing");
    await run(["bun", "run", "ci"]);
    await run(["bun", "run", "release:check"]);
    if (git(["status", "--porcelain"]))
      throw new Error("Validation changed tracked files; review before releasing");
  }
  await run([
    "bun",
    "nx",
    "release",
    version,
    "--skip-publish",
    ...(firstRelease ? ["--first-release"] : []),
    ...(dryRun ? ["--dry-run"] : []),
  ]);
  if (!dryRun) {
    const released = await Bun.file(resolve(root, "packages/core/package.json")).json();
    console.log(`Release prepared. Push with: git push --atomic origin main v${released.version}`);
  }
}
