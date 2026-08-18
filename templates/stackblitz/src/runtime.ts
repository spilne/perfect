import { RetryPolicy } from "@perfect/core/retry";
import { Stream } from "@perfect/core/stream";

type PlaygroundFunction = (...values: unknown[]) => Promise<unknown>;
type PlaygroundFunctionConstructor = new (...parameters: string[]) => PlaygroundFunction;

const AsyncFunction = Object.getPrototypeOf(async function () {})
  .constructor as PlaygroundFunctionConstructor;

export async function evaluateSource(source: string): Promise<unknown> {
  const execute = new AsyncFunction(
    "Stream",
    "RetryPolicy",
    "delay",
    "succeed",
    "fail",
    `"use strict";\n${source}\n//# sourceURL=perfect-playground.js`,
  );
  const value = await execute(Stream, RetryPolicy, delay, succeed, fail);

  if (value instanceof Stream) return (value as Stream<unknown, never>).toArray().run();
  if (hasRun(value)) return value.run();
  return value;
}

function delay<A>(ms: number, value: A) {
  return Stream.tick(ms)
    .take(1)
    .drain()
    .map(() => value);
}

function succeed<A>(value: A) {
  return Stream.succeed(value)
    .toArray()
    .map((values) => values[0]!);
}

function fail<E>(error: E) {
  return Stream.fail(error).drain();
}

function hasRun(value: unknown): value is { run(): Promise<unknown> } {
  return (
    value !== null &&
    typeof value === "object" &&
    typeof (value as { readonly run?: unknown }).run === "function"
  );
}
