// Built-in pipes — reusable stream transformations

import { Stream } from "./stream.js";
import { Chunk } from "./chunk.js";
import type { Pipe, Step } from "./stream.js";
import type { Throws } from "../eff.js";
import type { WithError } from "../either.js";
import { succeed, fail } from "../constructors.js";
import { TaggedError } from "../tagged-error.js";

const stripCR = (s: string): string => (s.endsWith("\r") ? s.slice(0, -1) : s);

// ── Stateful chunk transforms ──────────────────────────────────────
//
// Most pipes here are small parsers: they keep some state (a half-read
// line, a half-read frame) and turn each incoming chunk into zero or more
// outputs. This helper runs such a parser one chunk at a time.
//
// `make` is called once per run, so running the same stream twice starts
// with fresh state. (Before, the state lived in the pipe, so a second run
// started with leftovers from the first.)

/** Returned by a parser to fail the stream with a typed error. */
class PipeFailure<E> {
  constructor(readonly error: E) {}
}

interface ChunkParser<A, B, E> {
  /** Turn one chunk into outputs, or fail. */
  step(chunk: Chunk<A>): B[] | PipeFailure<E>;
  /** Called once when the input ends: emit whatever is left. */
  end(): B[] | PipeFailure<E>;
}

function parseChunks<A, B, S, E = never>(
  input: Stream<A, S>,
  make: () => ChunkParser<A, B, E>,
): Stream<B, WithError<S, E>> {
  return Stream.suspend(() => {
    const parser = make();
    const go = (stream: Stream<A, any>): Stream<B, any> =>
      new Stream(
        (stream.step as any).flatMap((s: Step<A>) => {
          if (s._tag === "Done") {
            const rest = parser.end();
            if (rest instanceof PipeFailure) return fail(rest.error);
            if (rest.length === 0) return succeed({ _tag: "Done" });
            return succeed({ _tag: "Emit", chunk: Chunk.fromArray(rest), next: Stream.empty() });
          }
          const out = parser.step(s.chunk);
          if (out instanceof PipeFailure) return fail(out.error);
          return succeed({ _tag: "Emit", chunk: Chunk.fromArray(out), next: go(s.next) });
        }),
        stream._finalizer,
      );
    return go(input);
  }) as Stream<B, WithError<S, E>>;
}

/**
 * Split text into lines. Handles "\n" and "\r\n", and lines that are split
 * across chunks.
 *
 * A line that arrives in many small pieces is joined once, when its end
 * shows up. (The old version re-split the whole buffered text on every
 * chunk, which got very slow for long lines.)
 */
export const lines: Pipe<string, string> = (input) =>
  parseChunks(input, () => {
    // Pieces of the line we are in the middle of.
    let partial: string[] = [];
    return {
      step(chunk) {
        const out: string[] = [];
        for (let i = 0; i < chunk.length; i++) {
          const text = chunk.get(i);
          let start = 0;
          let newline = text.indexOf("\n");
          while (newline !== -1) {
            let line = text.slice(start, newline);
            if (partial.length > 0) {
              partial.push(line);
              line = partial.join("");
              partial = [];
            }
            out.push(stripCR(line));
            start = newline + 1;
            newline = text.indexOf("\n", start);
          }
          if (start < text.length) partial.push(start === 0 ? text : text.slice(start));
        }
        return out;
      },
      end() {
        if (partial.length === 0) return [];
        const line = partial.join("");
        partial = [];
        return [stripCR(line)];
      },
    };
  });

export interface CsvOptions {
  /** Use the first record as object keys. Default: false. */
  header?: boolean;
  /** Field separator. Must be one character. Default: `,`. */
  separator?: string;
  /** Quote character. Must be one character. Default: `"`. */
  quote?: string;
  /** Trim every parsed field. Default: false. */
  trim?: boolean;
}

function csvPipe(options: CsvOptions): Pipe<string, string[] | Record<string, string>> {
  const separator = options.separator ?? ",";
  const quote = options.quote ?? '"';
  if (separator.length !== 1) throw new RangeError("csv separator must be one character");
  if (quote.length !== 1) throw new RangeError("csv quote must be one character");
  if (separator === quote) throw new RangeError("csv separator and quote must differ");

  return (input) =>
    parseChunks(input, () => {
      let field = "";
      let row: string[] = [];
      let inQuotes = false;
      let pendingQuote = false;
      let skipLf = false;
      let headers: string[] | null = null;

      const finishField = () => {
        row.push(options.trim ? field.trim() : field);
        field = "";
      };

      const finishRow = (rows: string[][]) => {
        finishField();
        rows.push(row);
        row = [];
      };

      const parseChunk = (chunk: string): string[][] => {
        const rows: string[][] = [];
        let index = 0;

        while (index < chunk.length) {
          const char = chunk[index]!;

          if (skipLf) {
            skipLf = false;
            if (char === "\n") {
              index++;
              continue;
            }
          }

          if (pendingQuote) {
            pendingQuote = false;
            if (char === quote) {
              field += quote;
              index++;
              continue;
            }
            inQuotes = false;
            continue;
          }

          if (inQuotes) {
            if (char !== quote) {
              field += char;
              index++;
              continue;
            }
            if (index + 1 >= chunk.length) {
              pendingQuote = true;
              index++;
              continue;
            }
            if (chunk[index + 1] === quote) {
              field += quote;
              index += 2;
              continue;
            }
            inQuotes = false;
            index++;
            continue;
          }

          if (char === quote && field.length === 0) {
            inQuotes = true;
            index++;
            continue;
          }
          if (char === separator) {
            finishField();
            index++;
            continue;
          }
          if (char === "\n") {
            finishRow(rows);
            index++;
            continue;
          }
          if (char === "\r") {
            finishRow(rows);
            skipLf = true;
            index++;
            continue;
          }
          field += char;
          index++;
        }

        return rows;
      };

      const finish = (): string[][] => {
        const hasRecord = pendingQuote || inQuotes || field.length > 0 || row.length > 0;
        pendingQuote = false;
        inQuotes = false;
        if (!hasRecord) return [];
        const rows: string[][] = [];
        finishRow(rows);
        return rows;
      };

      // With `header`, the first row names the columns and every later row
      // becomes an object.
      const shape = (rows: string[][]): Array<string[] | Record<string, string>> => {
        if (!options.header) return rows;
        const records: Array<Record<string, string>> = [];
        for (const values of rows) {
          if (headers === null) {
            headers = values;
            continue;
          }
          const names = headers;
          records.push(Object.fromEntries(names.map((name, index) => [name, values[index] ?? ""])));
        }
        return records;
      };

      return {
        step(chunk) {
          const rows: string[][] = [];
          for (let i = 0; i < chunk.length; i++) {
            for (const row of parseChunk(chunk.get(i))) rows.push(row);
          }
          return shape(rows);
        },
        end() {
          return shape(finish());
        },
      };
    });
}

/** Parse CSV text chunks directly. Passing the function to `through` keeps
 * the historical array output; calling `csv({ header: true })` emits records. */
export function csv<S>(input: Stream<string, S>): Stream<string[], S>;
export function csv(options: CsvOptions & { header: true }): Pipe<string, Record<string, string>>;
export function csv(options?: CsvOptions & { header?: false }): Pipe<string, string[]>;
export function csv<S>(
  inputOrOptions?: Stream<string, S> | CsvOptions,
): Stream<string[], S> | Pipe<string, string[]> | Pipe<string, Record<string, string>> {
  if (inputOrOptions instanceof Stream) {
    return csvPipe({})(inputOrOptions) as Stream<string[], S>;
  }
  return csvPipe(inputOrOptions ?? {}) as
    | Pipe<string, string[]>
    | Pipe<string, Record<string, string>>;
}

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

// One encoder for everyone: it has no state, and creating one per value
// was a noticeable cost on streams of many small strings.
const textEncoder = new TextEncoder();

export const utf8Encode: Pipe<string, Uint8Array> = (input) =>
  input.map((str) => textEncoder.encode(str));

function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  const blockSize = 32_768;
  for (let offset = 0; offset < bytes.length; offset += blockSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + blockSize));
  }
  return btoa(binary);
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

/** Encode each binary chunk as an independent base64 value. */
export const base64Encode: Pipe<Uint8Array, string> = (input) => input.map(encodeBase64);

/** Decode each base64 value into its original binary chunk. */
export const base64Decode: Pipe<string, Uint8Array> = (input) => input.map(decodeBase64);

/** UTF-8 text convenience wrapper around {@link base64Encode}. */
export const base64EncodeText: Pipe<string, string> = (input) =>
  input.through(utf8Encode).through(base64Encode);

/** UTF-8 text convenience wrapper around {@link base64Decode}. */
export const base64DecodeText: Pipe<string, string> = (input) =>
  input.through(base64Decode).through(utf8Decode);

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
  /**
   * Largest frame we accept, in bytes. A bigger header fails the stream
   * with {@link FrameTooLargeError}. Without a limit, one corrupt header can
   * make the pipe wait for (and buffer) up to 4 GB. Default: no limit.
   */
  maxFrameBytes?: number;
}

/** Typed failure from {@link lengthPrefixed} when a header announces a frame
 *  bigger than `maxFrameBytes`. */
export class FrameTooLargeError extends TaggedError("FrameTooLargeError")<{
  readonly frameBytes: number;
  readonly maxFrameBytes: number;
}>() {}

/**
 * Re-frame a binary stream into length-prefixed messages: each message is
 * preceded by an unsigned integer header holding its byte length (4-byte
 * big-endian by default). Partial frames are buffered across chunk splits;
 * a trailing incomplete frame is dropped when the stream ends. Combine with
 * {@link binaryDecode} to decode each frame.
 */
export function lengthPrefixed(
  options: LengthPrefixedOptions & { maxFrameBytes: number },
): Pipe<Uint8Array, Uint8Array, Throws<FrameTooLargeError>>;
export function lengthPrefixed(options?: LengthPrefixedOptions): Pipe<Uint8Array, Uint8Array>;
export function lengthPrefixed(
  options?: LengthPrefixedOptions,
): Pipe<Uint8Array, Uint8Array, Throws<FrameTooLargeError>> {
  const headerBytes = options?.headerBytes ?? 4;
  const littleEndian = options?.littleEndian ?? false;
  const maxFrameBytes = options?.maxFrameBytes ?? Infinity;

  const readLength = (bytes: Uint8Array): number => {
    if (headerBytes === 1) return bytes[0]!;
    if (headerBytes === 2) {
      return littleEndian ? bytes[0]! | (bytes[1]! << 8) : (bytes[0]! << 8) | bytes[1]!;
    }
    return littleEndian
      ? (bytes[0]! | (bytes[1]! << 8) | (bytes[2]! << 16) | (bytes[3]! << 24)) >>> 0
      : ((bytes[0]! << 24) | (bytes[1]! << 16) | (bytes[2]! << 8) | bytes[3]!) >>> 0;
  };

  return ((input: Stream<Uint8Array, unknown>) =>
    parseChunks(input, () => {
      // Bytes we have but can't use yet, kept as the pieces they arrived in.
      // We only join pieces once a whole frame is there, so a big frame that
      // arrives in many small chunks is copied once, not once per chunk.
      let pieces: Uint8Array[] = [];
      let pendingBytes = 0;

      const joinPieces = (): Uint8Array => {
        if (pieces.length === 1) return pieces[0]!;
        const joined = new Uint8Array(pendingBytes);
        let offset = 0;
        for (const piece of pieces) {
          joined.set(piece, offset);
          offset += piece.length;
        }
        pieces = [joined];
        return joined;
      };

      return {
        step(chunk) {
          const frames: Uint8Array[] = [];
          for (let i = 0; i < chunk.length; i++) {
            const piece = chunk.get(i);
            if (piece.length === 0) continue;
            pieces.push(piece);
            pendingBytes += piece.length;
            while (pendingBytes >= headerBytes) {
              // The header itself can be split across pieces.
              const head = pieces[0]!.length >= headerBytes ? pieces[0]! : joinPieces();
              const frameBytes = readLength(head);
              if (frameBytes > maxFrameBytes) {
                return new PipeFailure(new FrameTooLargeError({ frameBytes, maxFrameBytes }));
              }
              const needed = headerBytes + frameBytes;
              if (pendingBytes < needed) break;
              const buffer = pieces[0]!.length >= needed ? pieces[0]! : joinPieces();
              frames.push(buffer.slice(headerBytes, needed));
              pendingBytes -= needed;
              if (buffer.length === needed) pieces.shift();
              else pieces[0] = buffer.subarray(needed);
            }
          }
          return frames;
        },
        end() {
          // A trailing incomplete frame is dropped (documented above).
          pieces = [];
          pendingBytes = 0;
          return [];
        },
      };
    })) as Pipe<Uint8Array, Uint8Array, Throws<FrameTooLargeError>>;
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
 * and trimmed text nodes. `<![CDATA[...]]>` becomes a text event as written;
 * the `<?xml ...?>` declaration, comments and `<!DOCTYPE ...>` are skipped.
 * Entities such as `&amp;` are not decoded.
 *
 * The text can arrive in pieces split anywhere, even in the middle of a tag:
 * an unfinished tag or text is kept until the rest arrives.
 *
 * @example
 * ```ts
 * stream.through(Pipes.xml)
 *   .filter((e) => e.type === "open" && e.tag === "item")
 * // yields: { type: "open", tag: "item", attributes: { id: "1" } }
 * ```
 */
export const xml: Pipe<string, XmlEvent> = (input) =>
  parseChunks(input, () => {
    // Pieces of the part we could not parse yet: an unfinished tag, or text
    // that may go on in the next piece.
    let pending: string[] = [];
    // A character the next piece must contain before the pending part can
    // be finished: ">" for a tag, "<" for text. Pieces without it are only
    // collected, so a huge text node arriving in many pieces is still read
    // once, not again for every piece.
    let waitFor = "";
    return {
      step(chunk) {
        const out: XmlEvent[] = [];
        for (let i = 0; i < chunk.length; i++) {
          const piece = chunk.get(i);
          if (pending.length > 0 && !piece.includes(waitFor)) {
            pending.push(piece);
            continue;
          }
          pending.push(piece);
          const rest = parseXml(pending.join(""), out, false);
          pending = rest === "" ? [] : [rest];
          waitFor = rest.startsWith("<") ? ">" : "<";
        }
        return out;
      },
      end() {
        const out: XmlEvent[] = [];
        parseXml(pending.join(""), out, true);
        pending = [];
        return out;
      },
    };
  });

const XML_TAG = /^<(\/?)([a-zA-Z][\w.-]*)((?:\s+[\w.-]+\s*=\s*"[^"]*")*)\s*(\/?)>$/;
const XML_ATTRIBUTE = /([\w.-]+)\s*=\s*"([^"]*)"/g;

// Things that start with "<" but are not tags, and where each one ends.
const XML_SPECIAL: ReadonlyArray<readonly [start: string, end: string, keepAsText: boolean]> = [
  ["<![CDATA[", "]]>", true],
  ["<!--", "-->", false],
  ["<?", "?>", false],
  ["<!", ">", false],
];

/**
 * Parse as much of `text` as is complete, pushing events into `out`, and
 * return the part that is not complete yet. When `atEnd` is true there is no
 * more input, so trailing text is emitted and an unfinished tag is dropped.
 */
function parseXml(text: string, out: XmlEvent[], atEnd: boolean): string {
  let pos = 0;
  while (pos < text.length) {
    const lt = text.indexOf("<", pos);
    if (lt !== pos) {
      // Text up to the next "<". Without a "<" the text may go on in the
      // next piece, so wait for it (unless the input has ended).
      if (lt === -1 && !atEnd) break;
      pushText(text.slice(pos, lt === -1 ? text.length : lt), out);
      pos = lt === -1 ? text.length : lt;
      continue;
    }

    const special = XML_SPECIAL.find(([start]) => text.startsWith(start, pos));
    if (special === undefined && isPrefixOfSpecial(text, pos) && !atEnd) break;
    if (special !== undefined) {
      const [start, end, keepAsText] = special;
      const close = text.indexOf(end, pos + start.length);
      if (close === -1) {
        if (atEnd) return "";
        break;
      }
      if (keepAsText) pushText(text.slice(pos + start.length, close), out);
      pos = close + end.length;
      continue;
    }

    const gt = text.indexOf(">", pos);
    if (gt === -1) {
      if (!atEnd) break;
      // The input ended without a ">", so this "<" was not a tag after all.
      pushText(text.slice(pos + 1), out);
      return "";
    }
    const tag = text.slice(pos, gt + 1);
    const match = XML_TAG.exec(tag);
    if (match === null) {
      // A "<" that does not start a tag, as in "a < b": skip just the "<"
      // and read on, so a real tag after it is still found.
      pos += 1;
      continue;
    }
    out.push(xmlTagEvent(match));
    pos = gt + 1;
  }
  return text.slice(pos);
}

// True when the text at `pos` could still turn into one of XML_SPECIAL once
// more input arrives, e.g. a piece that ends with "<!-".
function isPrefixOfSpecial(text: string, pos: number): boolean {
  const rest = text.slice(pos);
  return XML_SPECIAL.some(([start]) => rest.length < start.length && start.startsWith(rest));
}

function pushText(text: string, out: XmlEvent[]): void {
  const trimmed = text.trim();
  if (trimmed !== "") out.push({ type: "text", text: trimmed });
}

function xmlTagEvent(match: RegExpExecArray): XmlEvent {
  const [, slash, tag, attrText, selfClose] = match;
  if (slash) return { type: "close", tag: tag! };
  const attributes: Record<string, string> = {};
  for (const attr of attrText!.matchAll(XML_ATTRIBUTE)) attributes[attr[1]!] = attr[2]!;
  return {
    type: selfClose ? "selfClose" : "open",
    tag: tag!,
    attributes: Object.keys(attributes).length > 0 ? attributes : undefined,
  };
}

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
