import { describe, expect, test } from "bun:test";
import {
  Config,
  ConfigProvider,
  TestConfigProvider,
  provide,
  type ConfigError,
  type Eff,
  type Throws,
} from "../src";

const withSettings = <A, S>(effect: Eff<A, S>, settings: Record<string, string>) =>
  provide(effect, ConfigProvider, new TestConfigProvider(settings));

// The ConfigError's messages, or "ok" when reading worked.
async function problemsOf(effect: Eff<unknown, Throws<ConfigError>>): Promise<string[] | "ok"> {
  const result = await effect.either().run();
  return result._tag === "Right" ? "ok" : result.left.problems.map((p) => p.message);
}

describe("Config readers", () => {
  test("parse what they can", async () => {
    const settings = {
      NAME: "api",
      PORT: "8080",
      DEBUG: "Yes",
      DATABASE_URL: "postgres://db:5432/app",
      TIMEOUT: "30s",
      ENV: "prod",
    };
    const read = Config.all({
      name: Config.string("NAME"),
      port: Config.number("PORT", { integer: true, min: 1, max: 65_535 }),
      debug: Config.boolean("DEBUG"),
      database: Config.url("DATABASE_URL"),
      timeoutMs: Config.duration("TIMEOUT"),
      env: Config.oneOf("ENV", ["dev", "prod"]),
    });
    const values = await withSettings(read, settings).orDie().run();
    expect(values.name).toBe("api");
    expect(values.port).toBe(8080);
    expect(values.debug).toBe(true);
    expect(values.database.hostname).toBe("db");
    expect(values.timeoutMs).toBe(30_000);
    expect(values.env).toBe("prod");
  });

  test("say what is wrong with a value", async () => {
    const settings = { PORT: "eighty", RATIO: "1.5", DEBUG: "maybe", URL: "not a url", ENV: "qa" };
    expect(await problemsOf(withSettings(Config.number("PORT"), settings))).toEqual([
      'PORT: "eighty" is not a number',
    ]);
    expect(
      await problemsOf(withSettings(Config.number("RATIO", { integer: true }), settings)),
    ).toEqual(['RATIO: "1.5" is not a whole number']);
    expect(await problemsOf(withSettings(Config.boolean("DEBUG"), settings))).toEqual([
      'DEBUG: "maybe" is not true or false',
    ]);
    expect(await problemsOf(withSettings(Config.url("URL"), settings))).toEqual([
      'URL: "not a url" is not a URL',
    ]);
    expect(await problemsOf(withSettings(Config.oneOf("ENV", ["dev", "prod"]), settings))).toEqual([
      'ENV: "qa" is not one of dev, prod',
    ]);
  });

  test("a default is used only when the setting isn't set", async () => {
    expect(
      await withSettings(Config.number("PORT", { default: 3000 }), {})
        .orDie()
        .run(),
    ).toBe(3000);
    // An invalid value is still an error, not silently replaced by the default.
    expect(
      await problemsOf(withSettings(Config.number("PORT", { default: 3000 }), { PORT: "x" })),
    ).toEqual(['PORT: "x" is not a number']);
    expect(await problemsOf(withSettings(Config.string("NAME"), {}))).toEqual(["NAME is not set"]);
  });

  test("optional gives undefined only for a missing setting", async () => {
    const port = Config.optional(Config.number("PORT"));
    expect(await withSettings(port, {}).orDie().run()).toBeUndefined();
    expect(await problemsOf(withSettings(port, { PORT: "x" }))).toEqual([
      'PORT: "x" is not a number',
    ]);
  });

  test("all reports every problem at once", async () => {
    const read = Config.all({
      port: Config.number("PORT"),
      database: Config.url("DATABASE_URL"),
      debug: Config.boolean("DEBUG", { default: false }),
    });
    expect(await problemsOf(withSettings(read, { PORT: "x" }))).toEqual([
      'PORT: "x" is not a number',
      "DATABASE_URL is not set",
    ]);
  });

  test("environment variables are the default source", async () => {
    process.env.PERFECT_CONFIG_TEST = "from-env";
    try {
      expect(await Config.string("PERFECT_CONFIG_TEST").orDie().run()).toBe("from-env");
    } finally {
      delete process.env.PERFECT_CONFIG_TEST;
    }
  });

  test("a secret doesn't show up when printed", async () => {
    const key = await withSettings(Config.secret("API_KEY"), { API_KEY: "s3cr3t" }).orDie().run();
    expect(key.value()).toBe("s3cr3t");
    expect(String(key)).toBe("<secret>");
    expect(JSON.stringify({ key })).toBe('{"key":"<secret>"}');
  });
});
