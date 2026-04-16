// Built-in pipes — reusable stream transformations

import { Stream } from "./stream"
import { Chunk } from "./chunk"
import type { Pipe } from "./stream"

export const lines: Pipe<string, string> = (input) => {
  let buffer = ""
  return input.flatMap(chunk => {
    buffer += chunk
    const parts = buffer.split("\n")
    buffer = parts.pop() ?? ""
    return parts.length > 0 ? Stream.fromArray(parts) : Stream.empty()
  }).concat(
    Stream.suspend(() => buffer.length > 0 ? Stream.succeed(buffer) : Stream.empty())
  )
}

export const csv: Pipe<string, string[]> = (input) =>
  input.through(lines).map(line =>
    line.split(",").map(cell => cell.trim())
  )

export const jsonl: Pipe<string, unknown> = (input) =>
  input.through(lines).collect(line => {
    try { return JSON.parse(line) }
    catch { return undefined }
  })

export const utf8Decode: Pipe<Uint8Array, string> = (input) =>
  input.map(buf => new TextDecoder().decode(buf))

export const utf8Encode: Pipe<string, Uint8Array> = (input) =>
  input.map(str => new TextEncoder().encode(str))

export const base64Encode: Pipe<string, string> = (input) =>
  input.map(str => btoa(str))

export const base64Decode: Pipe<string, string> = (input) =>
  input.map(str => atob(str))

export function take<A>(n: number): Pipe<A, A> {
  return (input) => input.take(n)
}

export function drop<A>(n: number): Pipe<A, A> {
  return (input) => input.drop(n)
}

export function filter<A>(p: (a: A) => boolean): Pipe<A, A> {
  return (input) => input.filter(p)
}

export function mapPipe<A, B>(f: (a: A) => B): Pipe<A, B> {
  return (input) => input.map(f)
}

export function grouped<A>(size: number): Pipe<A, Chunk<A>> {
  return (input) => input.grouped(size)
}

export function scan<A, B>(zero: B, f: (acc: B, a: A) => B): Pipe<A, B> {
  return (input) => input.scan(zero, f)
}
