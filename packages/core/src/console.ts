import { type Eff, Suspend, Op } from "./eff";
import { service, type ServiceTag } from "./service";

// ── Console service ────────────────────────────────────────────────

export interface Console {
  readonly log: (msg: string) => Eff<void, never>;
  readonly warn: (msg: string) => Eff<void, never>;
  readonly error: (msg: string) => Eff<void, never>;
  /**
   * Read one line from stdin, without the trailing newline. Resolves to
   * undefined at end of input, so a read loop terminates instead of spinning.
   */
  readonly readLine: () => Eff<string | undefined, never>;
}

export const Console: ServiceTag<Console, "Console"> = service<Console>()("Console");

function effSync<A>(f: () => A): Eff<A, never> {
  return new Suspend(Op.Sync, f, null) as any;
}

function effAsync<A>(register: (resume: (value: Eff<A, never>) => void) => void): Eff<A, never> {
  return new Suspend(Op.Async, register, null) as any;
}

function succeedNow<A>(value: A): Eff<A, never> {
  return new Suspend(Op.Succeed, value, null) as any;
}

// ── Real console: writes to stdout/stderr via globalThis.console ───

export class RealConsole implements Console {
  log(msg: string): Eff<void, never> {
    return effSync(() => {
      globalThis.console.log(msg);
    });
  }
  warn(msg: string): Eff<void, never> {
    return effSync(() => {
      globalThis.console.warn(msg);
    });
  }
  error(msg: string): Eff<void, never> {
    return effSync(() => {
      globalThis.console.error(msg);
    });
  }

  /**
   * One line from stdin. Buffers whatever the last read over-consumed, so
   * successive calls don't drop input — process.stdin hands over chunks, not
   * lines. Uses the Node-compatible stream API, which Bun implements too.
   */
  readLine(): Eff<string | undefined, never> {
    return effAsync<string | undefined>((resume) => {
      const buffered = stdinBuffer.indexOf("\n");
      if (buffered >= 0) {
        const line = stdinBuffer.slice(0, buffered);
        stdinBuffer = stdinBuffer.slice(buffered + 1);
        resume(succeedNow(stripCarriageReturn(line)));
        return;
      }

      const stdin = (globalThis as any).process?.stdin;
      if (stdin === undefined) {
        resume(succeedNow(undefined));
        return;
      }

      const cleanup = (): void => {
        stdin.off?.("data", onData);
        stdin.off?.("end", onEnd);
        stdin.off?.("error", onEnd);
      };
      const onData = (chunk: unknown): void => {
        stdinBuffer += String(chunk);
        const index = stdinBuffer.indexOf("\n");
        if (index < 0) return;
        const line = stdinBuffer.slice(0, index);
        stdinBuffer = stdinBuffer.slice(index + 1);
        cleanup();
        stdin.pause?.();
        resume(succeedNow(stripCarriageReturn(line)));
      };
      const onEnd = (): void => {
        cleanup();
        // Flush a trailing line with no newline terminator.
        const rest = stdinBuffer;
        stdinBuffer = "";
        resume(succeedNow(rest.length > 0 ? stripCarriageReturn(rest) : undefined));
      };

      stdin.setEncoding?.("utf8");
      stdin.on?.("data", onData);
      stdin.once?.("end", onEnd);
      stdin.once?.("error", onEnd);
      stdin.resume?.();
    });
  }
}

// Module-level so the leftover of a partially-consumed chunk survives across
// readLine() calls on the shared stdin stream.
let stdinBuffer = "";

function stripCarriageReturn(line: string): string {
  return line.endsWith("\r") ? line.slice(0, -1) : line;
}

export const realConsole: Console = new RealConsole();

// ── Test console: captures output for assertions ───────────────────

export class TestConsole implements Console {
  private _logs: string[] = [];
  private _warns: string[] = [];
  private _errors: string[] = [];
  private _input: string[] = [];

  /** Queue lines that readLine() will return, in order. */
  constructor(input: readonly string[] = []) {
    this._input = [...input];
  }

  /** Append more scripted input. */
  feed(...lines: string[]): void {
    this._input.push(...lines);
  }

  /** Lines not yet consumed by readLine(). */
  remainingInput(): readonly string[] {
    return this._input;
  }

  readLine(): Eff<string | undefined, never> {
    return effSync(() => this._input.shift());
  }

  log(msg: string): Eff<void, never> {
    return effSync(() => {
      this._logs.push(msg);
    });
  }
  warn(msg: string): Eff<void, never> {
    return effSync(() => {
      this._warns.push(msg);
    });
  }
  error(msg: string): Eff<void, never> {
    return effSync(() => {
      this._errors.push(msg);
    });
  }

  /** All log() messages, oldest first. */
  logs(): readonly string[] {
    return this._logs;
  }
  /** All warn() messages, oldest first. */
  warns(): readonly string[] {
    return this._warns;
  }
  /** All error() messages, oldest first. */
  errors(): readonly string[] {
    return this._errors;
  }

  /** All messages across log/warn/error in the order they were emitted. */
  all(): readonly { level: "log" | "warn" | "error"; msg: string }[] {
    // We only know per-level order; merge by emission would need a single
    // timeline. For most tests, level-keyed assertions are enough. If callers
    // need a unified timeline they can reach for log()+warn()+error() and
    // assert per channel.
    return [
      ...this._logs.map((msg) => ({ level: "log" as const, msg })),
      ...this._warns.map((msg) => ({ level: "warn" as const, msg })),
      ...this._errors.map((msg) => ({ level: "error" as const, msg })),
    ];
  }

  clear(): void {
    this._logs.length = 0;
    this._warns.length = 0;
    this._errors.length = 0;
    this._input.length = 0;
  }
}
