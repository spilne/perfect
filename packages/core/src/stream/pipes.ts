// Built-in pipes — reusable stream transformations

import { Stream } from "./stream";
import { Chunk } from "./chunk";
import type { Pipe } from "./stream";
import type { Throws } from "../eff";
import { succeed, fail } from "../constructors";
import { TaggedError } from "../tagged-error";

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
 * Tab-separated values. Splits text into lines, then each line into cells on
 * tabs — honoring double-quoted fields, where a doubled quote (`""`) inside a
 * quoted field is an escaped literal quote. Cells are not trimmed (quoted
 * content is preserved byte-for-byte).
 */
export const tsv: Pipe<string, string[]> = (input) =>
  input.through(lines).map((line) => splitDelimited(line, "\t", '"'));

/**
 * Whitespace-separated values — each line is trimmed and split on runs of
 * whitespace (spaces and tabs collapse into a single delimiter). Common for
 * log files and column-aligned CLI output. No quoting support.
 */
export const ssv: Pipe<string, string[]> = (input) =>
  input.through(lines).map((line) => line.trim().split(/\s+/));

export interface FixedWidthColumn {
  name: string;
  /** Inclusive start offset of the column within the line. */
  start: number;
  /** Exclusive end offset of the column within the line. */
  end: number;
  /** Trim whitespace from the extracted value. Default: true. */
  trim?: boolean;
}

/**
 * Fixed-width positional columns — common for legacy mainframe data and
 * COBOL exports. Splits text into lines, slices each line at the given
 * `[start, end)` offsets and yields a record keyed by column name.
 *
 * @example
 * ```ts
 * stream.through(Pipes.fixedWidth([
 *   { name: "id", start: 0, end: 5 },
 *   { name: "name", start: 5, end: 25 },
 * ]))
 * // yields: { id: "00001", name: "Alice" }
 * ```
 */
export function fixedWidth(columns: FixedWidthColumn[]): Pipe<string, Record<string, string>> {
  return (input) =>
    input.through(lines).map((line) => {
      const row: Record<string, string> = {};
      for (const col of columns) {
        const value = line.slice(col.start, col.end);
        row[col.name] = col.trim !== false ? value.trim() : value;
      }
      return row;
    });
}

/**
 * Parse lines with a regex using named capture groups — each matching line
 * yields a record of its groups; lines that don't match (or match without
 * named groups) are dropped.
 *
 * @example
 * ```ts
 * stream.through(Pipes.regex(/^(?<level>\w+): (?<msg>.*)$/))
 * // yields: { level: "warn", msg: "disk almost full" }
 * ```
 */
export function regex(pattern: RegExp): Pipe<string, Record<string, string>> {
  // Strip a /g flag so exec's stateful lastIndex can't silently skip lines.
  const re = pattern.global ? new RegExp(pattern.source, pattern.flags.replace("g", "")) : pattern;
  return (input) =>
    input.through(lines).filterMap((line) => {
      const match = re.exec(line);
      if (!match?.groups) return undefined;
      return { ...match.groups };
    });
}

// ── Schema parsing ─────────────────────────────────────────────────
//
// SchemaParser — library-agnostic validation interface. Anything that can
// validate `unknown` into a typed value satisfies it: Zod, Valibot, ArkType,
// @effect/Schema, and plain hand-rolled validators all match this shape.
//
//   const UserSchema = z.object({ id: z.string() });
//   const parser: SchemaParser<User> = UserSchema; // works directly
//
//   const parser: SchemaParser<User> = {
//     safeParse: (data) => isUser(data)
//       ? { success: true, data }
//       : { success: false, error: "not a user" },
//   };

export interface SchemaParser<T> {
  safeParse(data: unknown): { success: true; data: T } | { success: false; error: unknown };
}

/** Typed failure produced by {@link parseAs} on the first invalid element. */
export class SchemaParseError extends TaggedError("SchemaParseError")<{
  readonly error: unknown;
}>() {}

/**
 * Validate each element with a schema, emitting the parsed value. The stream
 * fails with {@link SchemaParseError} on the first invalid element — use
 * {@link parseAsLenient} to drop invalid elements instead.
 */
export function parseAs<T>(schema: SchemaParser<T>): Pipe<unknown, T, Throws<SchemaParseError>> {
  return (input) =>
    input.evalMap((data) => {
      const result = schema.safeParse(data);
      return result.success
        ? succeed(result.data)
        : (fail(new SchemaParseError({ error: result.error })) as any);
    }) as any;
}

/** Validate each element with a schema; elements that fail validation are
 *  silently dropped. Strict sibling: {@link parseAs}. */
export function parseAsLenient<T>(schema: SchemaParser<T>): Pipe<unknown, T> {
  return (input) =>
    input.filterMap((data) => {
      const result = schema.safeParse(data);
      return result.success ? result.data : undefined;
    });
}

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

// ── Binary framing ─────────────────────────────────────────────────

export interface LengthPrefixedOptions {
  /** Size of the length header in bytes: 1, 2 or 4. Default: 4. */
  headerBytes?: 1 | 2 | 4;
  /** Read the header as little-endian. Default: false (big-endian — the
   *  protobuf/gRPC streaming convention). */
  littleEndian?: boolean;
}

/**
 * Re-frame a binary stream into length-prefixed messages: each message is
 * preceded by an unsigned integer header holding its byte length (4-byte
 * big-endian by default). Partial frames are buffered across chunk splits;
 * a trailing incomplete frame is dropped when the stream ends. Combine with
 * {@link binaryDecode} to decode each frame.
 */
export function lengthPrefixed(options?: LengthPrefixedOptions): Pipe<Uint8Array, Uint8Array> {
  const headerBytes = options?.headerBytes ?? 4;
  const littleEndian = options?.littleEndian ?? false;
  return (input) => {
    let buffer = new Uint8Array(0);
    return input.flatMap((chunk) => {
      const combined = new Uint8Array(buffer.length + chunk.length);
      combined.set(buffer);
      combined.set(chunk, buffer.length);
      buffer = combined;

      const messages: Uint8Array[] = [];
      while (buffer.length >= headerBytes) {
        const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
        const msgLen =
          headerBytes === 4
            ? view.getUint32(0, littleEndian)
            : headerBytes === 2
              ? view.getUint16(0, littleEndian)
              : view.getUint8(0);
        if (buffer.length < headerBytes + msgLen) break;
        messages.push(buffer.slice(headerBytes, headerBytes + msgLen));
        buffer = buffer.slice(headerBytes + msgLen);
      }
      return Stream.fromArray(messages);
    });
  };
}

/**
 * Decode each binary chunk with a custom decoder function — protobuf,
 * msgpack, avro, or any binary format. Pair with {@link lengthPrefixed} so
 * each chunk is exactly one framed message.
 *
 * @example
 * ```ts
 * stream.through(Pipes.lengthPrefixed()).through(Pipes.binaryDecode(buf => MyProto.decode(buf)))
 * ```
 */
export function binaryDecode<T>(decode: (buffer: Uint8Array) => T): Pipe<Uint8Array, T> {
  return (input) => input.map(decode);
}

// ── XML ────────────────────────────────────────────────────────────

export interface XmlEvent {
  type: "open" | "close" | "text" | "selfClose";
  tag?: string;
  attributes?: Record<string, string>;
  text?: string;
}

/**
 * Parse XML text into SAX-style events — lightweight, no DOM tree in memory.
 * Handles open tags, close tags, self-closing tags, double-quoted attributes,
 * and trimmed text nodes. Each incoming text chunk is scanned independently
 * (no cross-chunk buffering), so a tag split across chunk boundaries will not
 * be recognized — feed whole documents or tag-complete chunks.
 *
 * @example
 * ```ts
 * stream.through(Pipes.xml)
 *   .filter((e) => e.type === "open" && e.tag === "item")
 * // yields: { type: "open", tag: "item", attributes: { id: "1" } }
 * ```
 */
export const xml: Pipe<string, XmlEvent> = (input) =>
  input.flatMap((chunk) => {
    const events: XmlEvent[] = [];
    const tagRegex = /<\/?([a-zA-Z][\w.-]*)((?:\s+[\w.-]+\s*=\s*"[^"]*")*)\s*(\/?)>|([^<]+)/g;
    let match;

    while ((match = tagRegex.exec(chunk)) !== null) {
      const [full, tag, attrStr, selfClose, text] = match;

      if (text?.trim()) {
        events.push({ type: "text", text: text.trim() });
      } else if (tag) {
        if (full!.startsWith("</")) {
          events.push({ type: "close", tag });
        } else {
          const attributes: Record<string, string> = {};
          if (attrStr) {
            const attrRegex = /([\w.-]+)\s*=\s*"([^"]*)"/g;
            let am;
            while ((am = attrRegex.exec(attrStr)) !== null) {
              attributes[am[1]!] = am[2]!;
            }
          }
          events.push({
            type: selfClose ? "selfClose" : "open",
            tag,
            attributes: Object.keys(attributes).length > 0 ? attributes : undefined,
          });
        }
      }
    }

    return Stream.fromArray(events);
  });

// ── Helpers ────────────────────────────────────────────────────────

// Delimited-line parser honoring quoted fields: a quote char toggles quoting,
// a doubled quote inside a quoted field is an escaped literal quote, and the
// separator is only meaningful outside quotes.
function splitDelimited(line: string, sep: string, quote: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i]!;

    if (inQuotes) {
      if (ch === quote) {
        if (i + 1 < line.length && line[i + 1] === quote) {
          current += quote;
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else if (ch === quote) {
      inQuotes = true;
    } else if (ch === sep) {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }

  fields.push(current);
  return fields;
}
