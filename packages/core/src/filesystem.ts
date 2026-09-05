// FileSystem service — file I/O as effects, with a typed failure channel and
// an in-memory implementation for tests.
//
// Backed by node:fs/promises, which Bun implements too, so one implementation
// covers both runtimes. Every operation that can fail at the OS boundary fails
// with a typed FileSystemError rather than throwing a defect — a missing file
// is an expected outcome, not a bug.

import type { Eff, Throws } from "./eff";
import { Suspend, Op } from "./eff";
import { sync, tryPromise } from "./constructors";
import { service, type ServiceTag } from "./service";
import { Stream } from "./stream";
import { TaggedError } from "./tagged-error";

/** Typed failure for every FileSystem operation. `cause` is the original error. */
export class FileSystemError extends TaggedError("FileSystemError")<{
  readonly op: "readFile" | "writeFile" | "appendFile" | "remove" | "mkdir" | "readDir" | "stat";
  readonly path: string;
  readonly cause: unknown;
}>() {}

export interface FileStat {
  readonly size: number;
  readonly isFile: boolean;
  readonly isDirectory: boolean;
  /** Last modification time in ms since the epoch. */
  readonly modifiedAt: number;
}

/** A filesystem change observed by {@link FileSystem.watch}. */
export interface FileEvent {
  readonly type: "change" | "rename";
  /** Path of the entry that changed, relative to the watched path. */
  readonly path: string;
}

export interface FileSystem {
  readonly readFile: (path: string) => Eff<string, Throws<FileSystemError>>;
  readonly readFileBytes: (path: string) => Eff<Uint8Array, Throws<FileSystemError>>;
  readonly writeFile: (
    path: string,
    contents: string | Uint8Array,
  ) => Eff<void, Throws<FileSystemError>>;
  readonly appendFile: (path: string, contents: string) => Eff<void, Throws<FileSystemError>>;
  readonly exists: (path: string) => Eff<boolean, never>;
  readonly remove: (
    path: string,
    options?: { recursive?: boolean },
  ) => Eff<void, Throws<FileSystemError>>;
  readonly mkdir: (
    path: string,
    options?: { recursive?: boolean },
  ) => Eff<void, Throws<FileSystemError>>;
  readonly readDir: (path: string) => Eff<string[], Throws<FileSystemError>>;
  readonly stat: (path: string) => Eff<FileStat, Throws<FileSystemError>>;
  /**
   * Watch a file or directory. The stream is unbounded — terminate it with
   * `take`, `interruptOn`, or by interrupting the fiber; the underlying
   * watcher is closed by the stream's finalizer either way.
   */
  readonly watch: (path: string, options?: { recursive?: boolean }) => Stream<FileEvent, never>;
}

export const FileSystem: ServiceTag<FileSystem, "FileSystem"> = service<FileSystem>()("FileSystem");

// ── Real filesystem ────────────────────────────────────────────────

type FsModule = typeof import("node:fs/promises");

function fsp(): Promise<FsModule> {
  return import("node:fs/promises");
}

function attempt<A>(
  op: FileSystemError["op"],
  path: string,
  run: (fs: FsModule) => Promise<A>,
): Eff<A, Throws<FileSystemError>> {
  return tryPromise(
    () => fsp().then(run),
    (cause) => new FileSystemError({ op, path, cause }),
  );
}

export class RealFileSystem implements FileSystem {
  readFile(path: string): Eff<string, Throws<FileSystemError>> {
    return attempt("readFile", path, (fs) => fs.readFile(path, "utf8"));
  }

  readFileBytes(path: string): Eff<Uint8Array, Throws<FileSystemError>> {
    return attempt("readFile", path, async (fs) => new Uint8Array(await fs.readFile(path)));
  }

  writeFile(path: string, contents: string | Uint8Array): Eff<void, Throws<FileSystemError>> {
    return attempt("writeFile", path, (fs) => fs.writeFile(path, contents)) as any;
  }

  appendFile(path: string, contents: string): Eff<void, Throws<FileSystemError>> {
    return attempt("appendFile", path, (fs) => fs.appendFile(path, contents)) as any;
  }

  /** Never fails: a permission error or a missing parent both mean "no". */
  exists(path: string): Eff<boolean, never> {
    return tryPromise(
      () =>
        fsp()
          .then((fs) =>
            fs.access(path).then(
              () => true,
              () => false,
            ),
          )
          .catch(() => false),
      () => false,
    ) as any;
  }

  remove(path: string, options: { recursive?: boolean } = {}): Eff<void, Throws<FileSystemError>> {
    return attempt("remove", path, (fs) =>
      fs.rm(path, { recursive: options.recursive ?? false, force: true }),
    ) as any;
  }

  mkdir(path: string, options: { recursive?: boolean } = {}): Eff<void, Throws<FileSystemError>> {
    return attempt("mkdir", path, (fs) =>
      fs.mkdir(path, { recursive: options.recursive ?? false }),
    ) as any;
  }

  readDir(path: string): Eff<string[], Throws<FileSystemError>> {
    return attempt("readDir", path, (fs) => fs.readdir(path));
  }

  stat(path: string): Eff<FileStat, Throws<FileSystemError>> {
    return attempt("stat", path, async (fs) => {
      const s = await fs.stat(path);
      return {
        size: s.size,
        isFile: s.isFile(),
        isDirectory: s.isDirectory(),
        modifiedAt: s.mtimeMs,
      };
    });
  }

  watch(path: string, options: { recursive?: boolean } = {}): Stream<FileEvent, never> {
    // Stream.async's returned cleanup runs on completion, failure and
    // interruption alike, so the watcher can never outlive the stream.
    return Stream.async<FileEvent, never>((emit, close) =>
      sync(() => {
        let disposed = false;
        let closeWatcher: (() => void) | undefined;

        void import("node:fs").then((fs) => {
          if (disposed) return;
          const watcher = fs.watch(
            path,
            { recursive: options.recursive ?? false },
            (type, filename) => {
              emit({
                type: type === "rename" ? "rename" : "change",
                path: typeof filename === "string" ? filename : String(filename ?? ""),
              });
            },
          );
          // A watcher error ends the stream rather than failing it — the typed
          // channel is `never` because callers watch for changes, not for the
          // watcher's own health.
          watcher.on("error", () => close());
          closeWatcher = () => watcher.close();
          if (disposed) closeWatcher();
        });

        return () => {
          disposed = true;
          closeWatcher?.();
        };
      }),
    );
  }
}

export const realFileSystem: FileSystem = new RealFileSystem();

// ── In-memory filesystem for tests ─────────────────────────────────

/**
 * Deterministic FileSystem backed by a Map. Paths are used verbatim — no
 * normalization — so tests should use one consistent spelling.
 *
 *   const fs = new TestFileSystem({ "/etc/app.conf": "debug=true" });
 *   await provide(program, FileSystem, fs).run();
 */
export class TestFileSystem implements FileSystem {
  private readonly files = new Map<string, Uint8Array>();
  private readonly dirs = new Set<string>();
  private readonly watchers = new Set<(event: FileEvent) => void>();
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();
  private clock = 0;
  private readonly times = new Map<string, number>();

  constructor(initial: Record<string, string | Uint8Array> = {}) {
    for (const [path, contents] of Object.entries(initial)) this.setFile(path, contents);
  }

  private setFile(path: string, contents: string | Uint8Array): void {
    this.files.set(path, typeof contents === "string" ? this.encoder.encode(contents) : contents);
    this.times.set(path, ++this.clock);
  }

  private fail<A>(op: FileSystemError["op"], path: string): Eff<A, Throws<FileSystemError>> {
    return new Suspend(
      Op.Fail,
      { _tag: "Fail", error: new FileSystemError({ op, path, cause: `ENOENT: ${path}` }) },
      null,
    ) as any;
  }

  private emit(event: FileEvent): void {
    for (const watcher of this.watchers) watcher(event);
  }

  readFile(path: string): Eff<string, Throws<FileSystemError>> {
    const bytes = this.files.get(path);
    if (bytes === undefined) return this.fail("readFile", path);
    return sync(() => this.decoder.decode(bytes)) as any;
  }

  readFileBytes(path: string): Eff<Uint8Array, Throws<FileSystemError>> {
    const bytes = this.files.get(path);
    if (bytes === undefined) return this.fail("readFile", path);
    return sync(() => bytes) as any;
  }

  writeFile(path: string, contents: string | Uint8Array): Eff<void, Throws<FileSystemError>> {
    return sync(() => {
      const existed = this.files.has(path);
      this.setFile(path, contents);
      this.emit({ type: existed ? "change" : "rename", path });
    }) as any;
  }

  appendFile(path: string, contents: string): Eff<void, Throws<FileSystemError>> {
    return sync(() => {
      const existing = this.files.get(path);
      const addition = this.encoder.encode(contents);
      if (existing === undefined) {
        this.setFile(path, addition);
      } else {
        const merged = new Uint8Array(existing.length + addition.length);
        merged.set(existing, 0);
        merged.set(addition, existing.length);
        this.files.set(path, merged);
        this.times.set(path, ++this.clock);
      }
      this.emit({ type: "change", path });
    }) as any;
  }

  exists(path: string): Eff<boolean, never> {
    return sync(() => this.files.has(path) || this.dirs.has(path));
  }

  remove(path: string, options: { recursive?: boolean } = {}): Eff<void, Throws<FileSystemError>> {
    return sync(() => {
      this.files.delete(path);
      this.dirs.delete(path);
      if (options.recursive) {
        const prefix = path.endsWith("/") ? path : `${path}/`;
        // Deleting during Map/Set iteration is well-defined in JS, so no copy.
        for (const key of this.files.keys()) {
          if (key.startsWith(prefix)) this.files.delete(key);
        }
        for (const key of this.dirs) {
          if (key.startsWith(prefix)) this.dirs.delete(key);
        }
      }
      this.emit({ type: "rename", path });
    }) as any;
  }

  mkdir(path: string, _options: { recursive?: boolean } = {}): Eff<void, Throws<FileSystemError>> {
    return sync(() => {
      this.dirs.add(path);
    }) as any;
  }

  readDir(path: string): Eff<string[], Throws<FileSystemError>> {
    const prefix = path.endsWith("/") ? path : `${path}/`;
    const entries = new Set<string>();
    for (const key of [...this.files.keys(), ...this.dirs]) {
      if (!key.startsWith(prefix)) continue;
      const rest = key.slice(prefix.length);
      if (rest.length === 0) continue;
      entries.add(rest.split("/")[0]!);
    }
    if (entries.size === 0 && !this.dirs.has(path)) return this.fail("readDir", path);
    return sync(() => [...entries].sort()) as any;
  }

  stat(path: string): Eff<FileStat, Throws<FileSystemError>> {
    const bytes = this.files.get(path);
    if (bytes !== undefined) {
      return sync(() => ({
        size: bytes.length,
        isFile: true,
        isDirectory: false,
        modifiedAt: this.times.get(path) ?? 0,
      })) as any;
    }
    if (this.dirs.has(path)) {
      return sync(() => ({ size: 0, isFile: false, isDirectory: true, modifiedAt: 0 })) as any;
    }
    return this.fail("stat", path);
  }

  watch(path: string, options: { recursive?: boolean } = {}): Stream<FileEvent, never> {
    const prefix = path.endsWith("/") ? path : `${path}/`;
    return Stream.async<FileEvent, never>((emit) =>
      sync(() => {
        const listener = (event: FileEvent): void => {
          if (event.path === path) {
            emit({ type: event.type, path: event.path });
            return;
          }
          if (options.recursive && event.path.startsWith(prefix)) {
            emit({ type: event.type, path: event.path.slice(prefix.length) });
          }
        };
        this.watchers.add(listener);
        return () => {
          this.watchers.delete(listener);
        };
      }),
    );
  }

  /** Snapshot of every file, decoded as text. For assertions. */
  snapshot(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [path, bytes] of this.files) out[path] = this.decoder.decode(bytes);
    return out;
  }
}
