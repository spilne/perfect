// Config — read settings (environment variables by default) as typed
// effects.
//
//   const settings = Config.all({
//     port: Config.number("PORT", { default: 3000 }),
//     database: Config.url("DATABASE_URL"),
//     apiKey: Config.secret("API_KEY"),
//   });
//   // Eff<{ port: number; database: URL; apiKey: Secret }, Throws<ConfigError>>
//
// A missing or invalid setting is a typed ConfigError, not an exception, and
// Config.all reports every problem at once instead of stopping at the first.
// Where the raw values come from is the ConfigProvider service: environment
// variables unless you provide another one (TestConfigProvider in tests).

import { type Eff, type Throws, Suspend, Op } from "./eff.js";
import { fail, succeed, suspend } from "./constructors.js";
import { service, type ServiceTag } from "./service.js";
import { TaggedError } from "./tagged-error.js";
import { Duration } from "./duration.js";

// ── Where values come from ──────────────────────────────────────────

/** Gives the raw text of a setting, or undefined when it isn't set. */
export interface ConfigProvider {
  readonly read: (name: string) => string | undefined;
}

export const ConfigProvider: ServiceTag<ConfigProvider, "ConfigProvider"> =
  service<ConfigProvider>()("ConfigProvider");

// process.env exists in Node and Bun; elsewhere (a browser) nothing is set.
const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;

/** Reads environment variables. The default. */
export const envConfigProvider: ConfigProvider = {
  read: (name) => env?.[name],
};

/** Settings from a plain object, for tests. */
export class TestConfigProvider implements ConfigProvider {
  constructor(private readonly values: Record<string, string | undefined> = {}) {}

  read(name: string): string | undefined {
    return this.values[name];
  }
}

// ── Errors ──────────────────────────────────────────────────────────

/** One setting that is missing or can't be used. */
export interface ConfigProblem {
  readonly name: string;
  readonly problem: "missing" | "invalid";
  readonly message: string;
}

/** Settings that are missing or invalid. `problems` lists every one of them. */
export class ConfigError extends TaggedError("ConfigError")<{
  readonly problems: readonly ConfigProblem[];
  readonly message: string;
}>() {}

function configError(problems: readonly ConfigProblem[]): ConfigError {
  return new ConfigError({
    problems,
    message: problems.map((p) => p.message).join("; "),
  });
}

// ── Secrets ─────────────────────────────────────────────────────────

/**
 * A setting that must not end up in logs. Printing it, or turning it into
 * JSON, shows "<secret>"; call `value()` where the real text is needed.
 */
export class Secret {
  readonly #value: string;

  constructor(value: string) {
    this.#value = value;
  }

  value(): string {
    return this.#value;
  }

  toString(): string {
    return "<secret>";
  }

  toJSON(): string {
    return "<secret>";
  }

  [Symbol.for("nodejs.util.inspect.custom")](): string {
    return "Secret(<secret>)";
  }
}

// ── Readers ─────────────────────────────────────────────────────────

/** Options every reader takes. */
export interface ConfigOptions<A> {
  /** Used when the setting isn't set (it is still checked when it is). */
  readonly default?: A;
}

/** An effect that reads one setting. */
export type ConfigReader<A> = Eff<A, Throws<ConfigError>>;

/**
 * Read `name` and turn its text into a value with `parse`, which returns the
 * value or, when the text can't be used, a sentence saying why.
 */
function reader<A>(
  name: string,
  options: ConfigOptions<A> | undefined,
  parse: (text: string) => { ok: A } | { problem: string },
): ConfigReader<A> {
  return new Suspend(
    Op.FlatMap,
    new Suspend(Op.GetCtx, ConfigProvider.key, null),
    (provider: ConfigProvider) => {
      const text = provider.read(name);
      if (text === undefined || text === "") {
        if (options && "default" in options) return succeed(options.default as A);
        return fail(configError([{ name, problem: "missing", message: `${name} is not set` }]));
      }
      const parsed = parse(text);
      if ("ok" in parsed) return succeed(parsed.ok);
      return fail(
        configError([{ name, problem: "invalid", message: `${name}: ${parsed.problem}` }]),
      );
    },
  ) as ConfigReader<A>;
}

export interface NumberOptions extends ConfigOptions<number> {
  readonly integer?: boolean;
  readonly min?: number;
  readonly max?: number;
}

const TRUE = new Set(["true", "1", "yes", "on"]);
const FALSE = new Set(["false", "0", "no", "off"]);

export const Config = {
  /** The text as it is. */
  string(name: string, options?: ConfigOptions<string>): ConfigReader<string> {
    return reader<string>(name, options, (text) => ({ ok: text }));
  },

  /** A number, optionally a whole number and within `min`..`max`. */
  number(name: string, options?: NumberOptions): ConfigReader<number> {
    return reader<number>(name, options, (text) => {
      const value = Number(text.trim());
      if (!Number.isFinite(value)) return { problem: `"${text}" is not a number` };
      if (options?.integer && !Number.isInteger(value)) {
        return { problem: `"${text}" is not a whole number` };
      }
      if (options?.min !== undefined && value < options.min) {
        return { problem: `${value} is below the minimum of ${options.min}` };
      }
      if (options?.max !== undefined && value > options.max) {
        return { problem: `${value} is above the maximum of ${options.max}` };
      }
      return { ok: value };
    });
  },

  /** true/false, 1/0, yes/no or on/off (any case). */
  boolean(name: string, options?: ConfigOptions<boolean>): ConfigReader<boolean> {
    return reader<boolean>(name, options, (text) => {
      const word = text.trim().toLowerCase();
      if (TRUE.has(word)) return { ok: true };
      if (FALSE.has(word)) return { ok: false };
      return { problem: `"${text}" is not true or false` };
    });
  },

  /** An absolute URL. */
  url(name: string, options?: ConfigOptions<URL>): ConfigReader<URL> {
    return reader<URL>(name, options, (text) => {
      try {
        return { ok: new URL(text) };
      } catch {
        return { problem: `"${text}" is not a URL` };
      }
    });
  },

  /** A duration in milliseconds, written as a number of ms or like "30s", "5m". */
  duration(name: string, options?: ConfigOptions<number>): ConfigReader<number> {
    return reader<number>(name, options, (text) => {
      const trimmed = text.trim();
      if (/^\d+(\.\d+)?$/.test(trimmed)) return { ok: Number(trimmed) };
      try {
        return { ok: Duration.parse(trimmed).ms };
      } catch {
        return { problem: `"${text}" is not a duration (like 500, "30s" or "5m")` };
      }
    });
  },

  /** One of a fixed set of words. */
  oneOf<const W extends string>(
    name: string,
    choices: readonly W[],
    options?: ConfigOptions<W>,
  ): ConfigReader<W> {
    return reader<W>(name, options, (text) =>
      (choices as readonly string[]).includes(text)
        ? { ok: text as W }
        : { problem: `"${text}" is not one of ${choices.join(", ")}` },
    );
  },

  /** Text that must not be logged; see {@link Secret}. */
  secret(name: string, options?: ConfigOptions<string>): ConfigReader<Secret> {
    const fallback =
      options !== undefined && "default" in options
        ? { default: new Secret(options.default!) }
        : undefined;
    return reader<Secret>(name, fallback, (text) => ({ ok: new Secret(text) }));
  },

  /** `undefined` when the setting isn't set; an invalid value still fails. */
  optional<A>(read: ConfigReader<A>): ConfigReader<A | undefined> {
    return read.catchTag("ConfigError", (error) =>
      error.problems.every((p) => p.problem === "missing") ? succeed(undefined) : fail(error),
    ) as ConfigReader<A | undefined>;
  },

  /**
   * Read several settings into one object. Unlike `all`, it doesn't stop at
   * the first problem: the ConfigError lists every missing or invalid
   * setting, so a misconfigured deploy is fixed in one go.
   */
  all<const R extends Record<string, ConfigReader<unknown>>>(
    readers: R,
  ): ConfigReader<{ [K in keyof R]: R[K] extends ConfigReader<infer A> ? A : never }> {
    const entries = Object.entries(readers);
    const collect = (
      index: number,
      values: Record<string, unknown>,
      problems: ConfigProblem[],
    ): ConfigReader<any> => {
      if (index === entries.length) {
        return problems.length > 0 ? fail(configError(problems)) : succeed(values);
      }
      const [key, read] = entries[index]!;
      return read.either().flatMap((result) => {
        if (result._tag === "Right") values[key] = result.right;
        else problems.push(...result.left.problems);
        return collect(index + 1, values, problems);
      }) as ConfigReader<any>;
    };
    // suspend: every run starts with fresh results.
    return suspend(() => collect(0, {}, []));
  },
} as const;
