// Logger service — structured, leveled logging with context annotations.
//
// Same service pattern as Clock/Random/Console: a real default is seeded
// into the empty context (ConsoleLogger at "info"), tests provide a
// TestLogger. The sink is a SYNC function — one entry object, one call —
// so logging stays cheap and the effect layer stays thin.
//
//   Log.info("user created", { userId })          // effect, reads context
//   Log.annotated(program, { requestId })          // annotations inherited
//   provide(program, Logger, new TestLogger())     // capture in tests

import { type Eff, Suspend, Op } from "./eff";
import { service, type ServiceTag } from "./service";
import { clockNow } from "./clock";

export type LogLevel = "trace" | "debug" | "info" | "warn" | "error" | "fatal";

const LEVEL_ORDER: Record<LogLevel, number> = {
  trace: 0,
  debug: 1,
  info: 2,
  warn: 3,
  error: 4,
  fatal: 5,
};

export function levelEnabled(min: LogLevel, level: LogLevel): boolean {
  return LEVEL_ORDER[level] >= LEVEL_ORDER[min];
}

export interface LogEntry {
  readonly level: LogLevel;
  readonly message: string;
  /** ms timestamp read from the Clock service (TestClock-controllable). */
  readonly timestamp: number;
  readonly annotations: Record<string, unknown>;
}

export interface Logger {
  readonly minLevel: LogLevel;
  log(entry: LogEntry): void;
}

export const Logger: ServiceTag<Logger> = service<Logger>("Logger");

// Annotations ride the context under their own key so they compose with
// provide() scoping — Log.annotated regions nest and unwind naturally.
export const LOG_ANNOTATIONS_KEY = Symbol.for("spilne/svc/LogAnnotations");

const succeedUndefined = new Suspend(Op.Succeed, undefined, null);

const getAnnotations: Eff<Record<string, unknown>, never> = new Suspend(
  Op.GetCtx,
  LOG_ANNOTATIONS_KEY,
  null,
) as any;

function logAt(
  level: LogLevel,
  message: string,
  extra?: Record<string, unknown>,
): Eff<void, never> {
  return new Suspend(Op.FlatMap, new Suspend(Op.GetCtx, Logger.key, null), (logger: Logger) => {
    if (!levelEnabled(logger.minLevel, level)) return succeedUndefined;
    return new Suspend(
      Op.FlatMap,
      clockNow as any,
      (timestamp: number) =>
        new Suspend(
          Op.FlatMap,
          getAnnotations as any,
          (ann: Record<string, unknown>) =>
            new Suspend(
              Op.Sync,
              () => {
                logger.log({
                  level,
                  message,
                  timestamp,
                  annotations: extra ? { ...ann, ...extra } : ann,
                });
              },
              null,
            ),
        ),
    );
  }) as any;
}

export const Log = {
  trace: (message: string, extra?: Record<string, unknown>) => logAt("trace", message, extra),
  debug: (message: string, extra?: Record<string, unknown>) => logAt("debug", message, extra),
  info: (message: string, extra?: Record<string, unknown>) => logAt("info", message, extra),
  warn: (message: string, extra?: Record<string, unknown>) => logAt("warn", message, extra),
  error: (message: string, extra?: Record<string, unknown>) => logAt("error", message, extra),
  fatal: (message: string, extra?: Record<string, unknown>) => logAt("fatal", message, extra),

  /** Current annotation map (from enclosing `annotated` regions). */
  annotations: getAnnotations,

  /**
   * Run `eff` with additional log annotations. Merges over the enclosing
   * region's annotations; unwinds when the region completes.
   */
  annotated<A, S>(eff: Eff<A, S>, annotations: Record<string, unknown>): Eff<A, S> {
    return new Suspend(Op.FlatMap, getAnnotations as any, (current: Record<string, unknown>) => {
      const merged = new Map<symbol, unknown>([
        [LOG_ANNOTATIONS_KEY, { ...current, ...annotations }],
      ]);
      return new Suspend(Op.Provide, eff, merged);
    }) as any;
  },
} as const;

// ── Console logger: human-readable lines via globalThis.console ────

function formatEntry(entry: LogEntry): string {
  const ts = new Date(entry.timestamp).toISOString();
  const ann = Object.entries(entry.annotations)
    .map(([k, v]) => ` ${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
    .join("");
  return `${ts} ${entry.level.toUpperCase().padEnd(5)} ${entry.message}${ann}`;
}

export class ConsoleLogger implements Logger {
  constructor(readonly minLevel: LogLevel = "info") {}

  log(entry: LogEntry): void {
    const line = formatEntry(entry);
    if (entry.level === "error" || entry.level === "fatal") globalThis.console.error(line);
    else if (entry.level === "warn") globalThis.console.warn(line);
    else globalThis.console.log(line);
  }
}

// ── JSON logger: one JSON object per line (log shippers) ───────────

export class JsonLogger implements Logger {
  constructor(readonly minLevel: LogLevel = "info") {}

  log(entry: LogEntry): void {
    globalThis.console.log(JSON.stringify(entry));
  }
}

// ── Test logger: captures entries for assertions ───────────────────

export class TestLogger implements Logger {
  private _entries: LogEntry[] = [];

  constructor(readonly minLevel: LogLevel = "trace") {}

  log(entry: LogEntry): void {
    this._entries.push(entry);
  }

  get entries(): readonly LogEntry[] {
    return this._entries;
  }

  atLevel(level: LogLevel): readonly LogEntry[] {
    return this._entries.filter((e) => e.level === level);
  }

  get messages(): string[] {
    return this._entries.map((e) => e.message);
  }

  clear(): void {
    this._entries = [];
  }
}

export const defaultLogger: Logger = new ConsoleLogger("info");
