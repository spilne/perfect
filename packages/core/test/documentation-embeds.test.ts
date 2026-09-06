import { expect, test } from "bun:test";
import { rewriteEmbeddedExamples } from "../../../documentation/embeds";

test("documentation embeds accept formatter blank lines and stay idempotent", () => {
  for (const gap of ["\n", "\n\n", "\r\n\r\n"]) {
    const input = `<!-- @embed packages/core/examples/demo.ts#hello -->${gap}\`\`\`ts\nold\n\`\`\`${gap}<!-- @end -->`;
    const output = rewriteEmbeddedExamples(input, ({ file, region }) => {
      expect(file).toBe("packages/core/examples/demo.ts");
      expect(region).toBe("hello");
      return "succeed(42);";
    });
    expect(output).toContain("succeed(42);");
    expect(output).not.toContain("old");
    expect(rewriteEmbeddedExamples(output, () => "succeed(42);")).toBe(output);
  }
});

test("documentation embeds reject unmatched markers instead of silently skipping", () => {
  for (const input of [
    "<!-- @embed file.ts#region -->",
    "<!-- @end -->",
    "<!-- @embed malformed -->\n<!-- @end -->",
  ]) {
    expect(() => rewriteEmbeddedExamples(input, () => "")).toThrow("Malformed example embed");
  }
  expect(rewriteEmbeddedExamples("ordinary markdown", () => "unused")).toBe("ordinary markdown");
});
