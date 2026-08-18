import { join } from "node:path";

interface PackageManifest {
  readonly name: string;
  readonly version: string;
  readonly private?: boolean;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
  readonly optionalDependencies?: Readonly<Record<string, string>>;
}

interface WorkspacePackage {
  readonly directory: string;
  readonly manifest: PackageManifest;
}

const root = process.cwd();
const dryRun = process.argv.includes("--dry-run");
const npmTag = process.env.PERFECT_NPM_TAG ?? "latest";
const registry = (process.env.NPM_CONFIG_REGISTRY ?? "https://registry.npmjs.org").replace(
  /\/$/,
  "",
);

async function run(command: readonly string[], directory = root): Promise<void> {
  const process = Bun.spawn(command, {
    cwd: directory,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  });
  const exitCode = await process.exited;
  if (exitCode !== 0) {
    throw new Error(`Command failed (${exitCode}): ${command.join(" ")}`);
  }
}

async function workspacePackages(): Promise<readonly WorkspacePackage[]> {
  const paths = Array.from(new Bun.Glob("packages/*/package.json").scanSync({ cwd: root })).sort();
  const packages = await Promise.all(
    paths.map(async (path) => {
      const manifest = (await Bun.file(join(root, path)).json()) as PackageManifest;
      return {
        directory: join(root, path, ".."),
        manifest,
      };
    }),
  );
  return packages.filter(({ manifest }) => !manifest.private);
}

function dependencyOrder(packages: readonly WorkspacePackage[]): readonly WorkspacePackage[] {
  const byName = new Map(packages.map((entry) => [entry.manifest.name, entry]));
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const ordered: WorkspacePackage[] = [];

  const visit = (entry: WorkspacePackage): void => {
    if (visited.has(entry.manifest.name)) return;
    if (visiting.has(entry.manifest.name)) {
      throw new Error(`Internal package dependency cycle at ${entry.manifest.name}`);
    }

    visiting.add(entry.manifest.name);
    const dependencyNames = Object.keys({
      ...entry.manifest.dependencies,
      ...entry.manifest.devDependencies,
      ...entry.manifest.peerDependencies,
      ...entry.manifest.optionalDependencies,
    }).sort();
    for (const name of dependencyNames) {
      const dependency = byName.get(name);
      if (dependency) visit(dependency);
    }
    visiting.delete(entry.manifest.name);
    visited.add(entry.manifest.name);
    ordered.push(entry);
  };

  for (const entry of [...packages].sort((a, b) =>
    a.manifest.name.localeCompare(b.manifest.name),
  )) {
    visit(entry);
  }
  return ordered;
}

function assertPublishableManifest(manifest: PackageManifest): void {
  const dependencyGroups = [
    manifest.dependencies,
    manifest.devDependencies,
    manifest.peerDependencies,
    manifest.optionalDependencies,
  ];
  for (const dependencies of dependencyGroups) {
    for (const [name, range] of Object.entries(dependencies ?? {})) {
      if (range.startsWith("workspace:")) {
        throw new Error(`${manifest.name} cannot publish ${name} with range ${range}`);
      }
    }
  }
}

async function isPublished({ name, version }: PackageManifest): Promise<boolean> {
  const response = await fetch(
    `${registry}/${encodeURIComponent(name)}/${encodeURIComponent(version)}`,
  );
  if (response.ok) return true;
  if (response.status === 404) return false;
  throw new Error(`Registry lookup for ${name}@${version} failed with HTTP ${response.status}`);
}

const packages = dependencyOrder(await workspacePackages());
if (packages.length === 0) throw new Error("No public workspace packages found");

if (!dryRun) await run(["bun", "pm", "whoami"]);

for (const entry of packages) {
  const id = `${entry.manifest.name}@${entry.manifest.version}`;
  assertPublishableManifest(entry.manifest);
  if (!dryRun && (await isPublished(entry.manifest))) {
    console.log(`skip ${id}: already published`);
    continue;
  }

  console.log(`${dryRun ? "check" : "publish"} ${id}`);
  const command = dryRun
    ? ["bun", "pm", "pack", "--dry-run"]
    : ["bun", "publish", "--frozen-lockfile", "--no-save", "--access", "public", "--tag", npmTag];
  await run(command, entry.directory);
}

if (!dryRun) {
  await run(["bun", "./node_modules/@changesets/cli/bin.js", "git-tag"]);
}
