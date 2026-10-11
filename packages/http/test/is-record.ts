// Fake parsers in these tests take `unknown` and need to read fields off it.
export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;
