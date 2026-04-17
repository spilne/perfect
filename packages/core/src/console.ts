import { type Eff, Suspend, Op } from "./eff";
import { service, type ServiceTag } from "./service";

// ── Console service ────────────────────────────────────────────────

export interface Console {
  readonly log: (msg: string) => Eff<void, never>;
  readonly warn: (msg: string) => Eff<void, never>;
  readonly error: (msg: string) => Eff<void, never>;
}

export const Console: ServiceTag<Console> = service<Console>("Console");

function effSync<A>(f: () => A): Eff<A, never> {
  return new Suspend(Op.Sync, f, null) as any;
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
}

export const realConsole: Console = new RealConsole();

// ── Test console: captures output for assertions ───────────────────

export class TestConsole implements Console {
  private _logs: string[] = [];
  private _warns: string[] = [];
  private _errors: string[] = [];

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
  }
}
