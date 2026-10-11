import { describe, expect, test } from "bun:test";
import { scoped } from "@spilne/perfect-core";
import { Discovery } from "../src";
import { Mdns } from "../src/mdns";

describe("Discovery", () => {
  test("dns finds every address of a name", async () => {
    const found = await Discovery.dns({ hostname: "localhost", port: 7946 }).run();
    expect(found.length).toBeGreaterThan(0);
    expect(found.every((a) => "port" in a && a.port === 7946)).toBe(true);
  });

  test("a name that doesn't resolve finds nothing instead of failing", async () => {
    expect(await Discovery.dns({ hostname: "nothing-here.invalid", port: 1 }).run()).toEqual([]);
    expect(await Discovery.srv("_nothing._tcp.invalid").run()).toEqual([]);
  });

  test("all merges what several discoveries find", async () => {
    const a = { host: "a", port: 1 };
    const b = { host: "b", port: 1 };
    const found = await Discovery.all([Discovery.of([a, b]), Discovery.of([b])]).run();
    expect(found).toEqual([a, b]);
  });
});

describe("Mdns", () => {
  // A port of our own, so the test doesn't talk to real mDNS responders.
  const options = { port: 50_000 + Math.floor(Math.random() * 10_000) };

  test("finds the members announced on the network", async () => {
    const found = await scoped(
      Mdns.announce(
        { service: "perfect-test", id: "one", address: { host: "10.0.0.1", port: 7001 } },
        options,
      )
        .flatMap(() =>
          Mdns.announce(
            { service: "perfect-test", id: "two", address: { host: "10.0.0.2", port: 7002 } },
            options,
          ),
        )
        .flatMap(() =>
          Mdns.announce(
            { service: "other-service", id: "three", address: { host: "10.0.0.3", port: 7003 } },
            options,
          ),
        )
        .flatMap(() => Mdns.discover({ service: "perfect-test", waitMs: 300 }, options)),
    )
      .orDie()
      .run();
    expect([...found].sort((x, y) => JSON.stringify(x).localeCompare(JSON.stringify(y)))).toEqual([
      { host: "10.0.0.1", port: 7001 },
      { host: "10.0.0.2", port: 7002 },
    ]);
  });
});
