// node:test reporter that prints one JSON line per finished test or suite, so
// node-bun-test-shim.test.ts can read titles without parsing TAP escapes.
export default async function* testNames(source) {
  for await (const event of source) {
    if (event.type !== "test:pass" && event.type !== "test:fail") continue;
    yield `${JSON.stringify({
      name: event.data.name,
      nesting: event.data.nesting,
      failed: event.type === "test:fail",
    })}\n`;
  }
}
