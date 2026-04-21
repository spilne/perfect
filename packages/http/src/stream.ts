// HTTP streaming — ONE base + composable `Pipe`s.
//
// `httpStream(opts)` returns `Stream<Uint8Array, Throws<HttpClientError>>` —
// raw bytes from `response.body`. Every other helper is a thin composition
// of this base + pipes (utf8Decode, lines, parseSSE, parseNDJSON, ...).
//
// Pipes live in core (`utf8Decode`, `lines`) or this module (`parseSSE`,
// `parseNDJSON`). Users can compose ad-hoc:
//
//   httpStream(opts).through(Pipes.utf8Decode).through(Pipes.lines).through(parseSSE)
//
// The 4 named wrappers (httpStreamText / Lines / NDJSON / SSE) are kept for
// ergonomics — they just inline the pipe chain above.

import {
  type Throws,
  type Pipe,
  Stream,
  sync,
  Pipes,
} from "@perfect/core";
import {
  type HttpClientError,
  HttpParseError,
} from "./errors";
import { type ResponseParser } from "./response";
import { type HttpRequestOptions } from "./transport";
import { type WithTransport } from "./fetch";
import { httpFetchOk } from "./fetch";

// ── SSE event shape ──────────────────────────────────────────────

export interface SSEvent {
  readonly event: string;
  readonly data: string;
  readonly id?: string;
  readonly retry?: number;
}

type StreamOptions = HttpRequestOptions &
  WithTransport & { readonly acceptStatus?: (status: number) => boolean };

// ── httpStream — the one base ────────────────────────────────────

/**
 * Execute the request and yield the response body as a byte stream.
 * Cancellation (via take / interrupt) cancels the underlying reader so
 * the TCP connection closes immediately.
 */
export function httpStream(
  opts: StreamOptions,
): Stream<Uint8Array, Throws<HttpClientError>> {
  return Stream.fromEffect(httpFetchOk(opts)).flatMap((response) =>
    Stream.async<Uint8Array, Throws<HttpClientError>>((emit, close) =>
      sync(() => {
        if (!response.body) {
          close();
          return undefined;
        }
        const reader = response.body.getReader();
        (async () => {
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) {
                close();
                return;
              }
              emit(value);
            }
          } catch {
            close();
          }
        })();
        return () => {
          reader.cancel().catch(() => {});
        };
      }),
    ),
  );
}

// ── HTTP-specific pipes ──────────────────────────────────────────

/**
 * Parse a line-stream as NDJSON, validating each line through the schema.
 * Empty lines skipped. Parse or validation failures surface as typed
 * `HttpParseError` (the stream fails — `.catchTag` to recover).
 */
export function parseNDJSON<T>(
  schema: ResponseParser<T>,
  urlHint = "<ndjson>",
): Pipe<string, T, Throws<HttpClientError>> {
  return (input) =>
    input.flatMap((line) => {
      const trimmed = line.trim();
      if (trimmed.length === 0) return Stream.empty();
      let data: unknown;
      try {
        data = JSON.parse(trimmed);
      } catch (cause) {
        return Stream.fail(
          new HttpParseError({
            url: urlHint,
            cause,
            message: `NDJSON parse failed at line: ${trimmed.slice(0, 80)}`,
          }),
        );
      }
      const result = schema.safeParse(data);
      if (result.success) return Stream.of(result.data);
      return Stream.fail(
        new HttpParseError({
          url: urlHint,
          cause: result.error,
          message: `NDJSON line doesn't match schema`,
        }),
      );
    });
}

/**
 * Parse a line-stream as Server-Sent Events. Emits one SSEvent per event
 * (terminated by a blank line); multi-line `data:` joined by `\n`. Comment
 * lines (starting with `:`) are ignored. An in-progress event at stream
 * close is flushed.
 */
export const parseSSE: Pipe<string, SSEvent> = (input) => {
  let event = "message";
  let data = "";
  let id: string | undefined;
  let retry: number | undefined;
  let hasFields = false;

  const consume = (line: string): SSEvent | null => {
    if (line === "") {
      if (!hasFields) return null;
      const ev: SSEvent = {
        event,
        data,
        ...(id !== undefined ? { id } : {}),
        ...(retry !== undefined ? { retry } : {}),
      };
      event = "message";
      data = "";
      id = undefined;
      retry = undefined;
      hasFields = false;
      return ev;
    }
    if (line.startsWith(":")) return null; // comment
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let val = colon === -1 ? "" : line.slice(colon + 1);
    if (val.startsWith(" ")) val = val.slice(1);
    hasFields = true;
    switch (field) {
      case "event":
        event = val;
        break;
      case "data":
        data = data.length === 0 ? val : `${data}\n${val}`;
        break;
      case "id":
        id = val;
        break;
      case "retry": {
        const r = parseInt(val, 10);
        if (!Number.isNaN(r)) retry = r;
        break;
      }
      default:
        break;
    }
    return null;
  };

  return input
    .flatMap((line) => {
      const out = consume(line);
      return out === null ? Stream.empty() : Stream.of(out);
    })
    .concat(
      Stream.suspend(() => {
        // Flush any in-progress event on close (no trailing blank line)
        const ev = consume("");
        return ev ? Stream.of(ev) : Stream.empty();
      }),
    );
};

// ── Thin convenience wrappers ────────────────────────────────────

/** Decoded text chunks (UTF-8, boundary-aware). */
export function httpStreamText(
  opts: StreamOptions,
): Stream<string, Throws<HttpClientError>> {
  return httpStream(opts).through(Pipes.utf8Decode);
}

/** Line-buffered text (splits on `\n`, strips trailing `\r`). */
export function httpStreamLines(
  opts: StreamOptions,
): Stream<string, Throws<HttpClientError>> {
  return httpStream(opts).through(Pipes.utf8Decode).through(Pipes.lines);
}

/** NDJSON with per-line schema validation (typed `HttpParseError` on fail). */
export function httpStreamNDJSON<T>(
  opts: StreamOptions & { readonly schema: ResponseParser<T> },
): Stream<T, Throws<HttpClientError>> {
  const { schema, ...rest } = opts;
  const url = typeof opts.url === "string" ? opts.url : opts.url.toString();
  return httpStreamLines(rest).through(parseNDJSON(schema, url));
}

/** Server-Sent Events. */
export function httpStreamSSE(
  opts: StreamOptions,
): Stream<SSEvent, Throws<HttpClientError>> {
  return httpStreamLines(opts).through(parseSSE);
}
