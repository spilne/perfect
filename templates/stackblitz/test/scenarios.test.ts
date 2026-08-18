import { describe, expect, test } from "bun:test";
import { evaluateSource } from "../src/runtime";
import { scenarios } from "../src/scenarios";

describe("StackBlitz playground scenarios", () => {
  for (const scenario of scenarios) {
    test(scenario.title, async () => {
      const result = await evaluateSource(scenario.source);
      expect(result).toBeDefined();
      expect(JSON.stringify(result)).not.toContain("[object Object]");
    });
  }

  test("reports invalid edits", async () => {
    expect(evaluateSource("return Stream.missing();")).rejects.toBeInstanceOf(Error);
  });
});
