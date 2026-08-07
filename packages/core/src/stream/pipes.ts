// Built-in pipes — reusable stream transformations

import { Stream } from "./stream";
import { Chunk } from "./chunk";
import type { Pipe } from "./stream";

const stripCR = (s: string): string => (s.endsWith("\r") ? s.slice(0, -1) : s);

export const lines: Pipe<string, string> = (input) => {
  let buffer = "";
  return input
    .flatMap((chunk) => {
      buffer += chunk;
      const parts = buffer.split("\n");
      buffer = parts.pop() ?? "";
      if (parts.length === 0) return Stream.empty();
      return Stream.fromArray(parts.map(stripCR));
    })
    .concat(
      Stream.suspend(() => (buffer.length > 0 ? Stream.succeed(stripCR(buffer)) : Stream.empty())),
    );
};

export const csv: Pipe<string, string[]> = (input) =>
  input.through(lines).map((line) => line.split(",").map((cell) => cell.trim()));

export const jsonl: Pipe<string, unknown> = (input) =>
  input.through(lines).collect((line) => {
    try {
      return JSON.parse(line);
    } catch {
      return undefined;
    }
  });

/**
 * UTF-8 decode a Uint8Array stream, preserving multi-byte char boundaries
 * across chunk splits (a single TextDecoder is shared with
 * `{ stream: true }`).
 */
export const utf8Decode: Pipe<Uint8Array, string> = (input) => {
  const decoder = new TextDecoder("utf-8");
  return input
    .map((buf) => decoder.decode(buf, { stream: true }))
    .concat(
      Stream.suspend(() => {
        const tail = decoder.decode();
        return tail.length > 0 ? Stream.succeed(tail) : Stream.empty();
      }),
    );
};

export const utf8Encode: Pipe<string, Uint8Array> = (input) =>
  input.map((str) => new TextEncoder().encode(str));

export const base64Encode: Pipe<string, string> = (input) => input.map((str) => btoa(str));

export const base64Decode: Pipe<string, string> = (input) => input.map((str) => atob(str));

export function take<A>(n: number): Pipe<A, A> {
  return (input) => input.take(n);
}

export function drop<A>(n: number): Pipe<A, A> {
  return (input) => input.drop(n);
}

export function filter<A>(p: (a: A) => boolean): Pipe<A, A> {
  return (input) => input.filter(p);
}

export function mapPipe<A, B>(f: (a: A) => B): Pipe<A, B> {
  return (input) => input.map(f);
}

export function grouped<A>(size: number): Pipe<A, Chunk<A>> {
  return (input) => input.grouped(size);
}

export function scan<A, B>(zero: B, f: (acc: B, a: A) => B): Pipe<A, B> {
  return (input) => input.scan(zero, f);
}
