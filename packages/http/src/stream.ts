// HTTP streaming helpers.
//
// Each helper fires the request, adapts the `Response.body`
// (`ReadableStream<Uint8Array>`) into a Perfect `Stream`, and returns it.
// Cancellation is wired: if the consumer stops the stream (runForEach
// returns, take(n) finishes, fiber interrupted), we cancel the underlying
// reader so the TCP connection closes.
//
// Available:
//   httpStreamText    — Stream<string>  (UTF-8 decoded chunks, no buffering)
//   httpStreamLines   — Stream<string>  (buffered; one line per emit)
//   httpStreamNDJSON  — Stream<T>       (JSON-per-line + parser.safeParse)
//   httpStreamSSE     — Stream<SSEvent> (Server-Sent Events)

import {
  type Eff,
  type Throws,
  Stream,
  sync,
  succeed,
  fail,
} from "@perfect/core";
import {
  type HttpClientError,
  HttpParseError,
} from "./errors";
import { type ResponseParser } from "./response";
import {
  type HttpRequestOptions,
  type HttpTransport,
  defaultTransport,
} from "./transport";
import { type WithTransport } from "./fetch";
import { httpFetchOk } from "./fetch";

// ── Server-Sent Events ────────────────────────────────────────────

export interface SSEvent {
  /** `event:` field. Defaults to `"message"` per the SSE spec. */
  readonly event: string;
  /** `data:` field — lines joined by `\n`. */
  readonly data: string;
  /** `id:` field, if present. */
  readonly id?: string;
  /** `retry:` field (ms), if present and numeric. */
  readonly retry?: number;
}

// ── Implementation helpers ────────────────────────────────────────

type StreamOptions = HttpRequestOptions &
  WithTransport & { readonly acceptStatus?: (status: number) => boolean };

/** Build the response-fetching effect shared by all stream variants. */
function fetchResponse(opts: StreamOptions): Eff<Response, Throws<HttpClientError>> {
  return httpFetchOk(opts);
}

/**
 * Wrap the async body reader + pump into a Stream. Callers provide a
 * `pump` that reads chunks and emits. Cancellation stops the pump AND
 * cancels the underlying reader.
 */
function streamFromReader<A>(
  response: Response,
  pump: (
    reader: ReadableStreamDefaultReader<Uint8Array>,
    emit: (value: A) => void,
    close: () => void,
  ) => void,
): Stream<A, Throws<HttpClientError>> {
  return Stream.async<A, Throws<HttpClientError>>((emit, close) =>
    sync(() => {
      if (!response.body) {
        close();
        return undefined;
      }
      const reader = response.body.getReader();
      pump(reader, emit, close);
      return () => {
        reader.cancel().catch(() => {});
      };
    }),
  );
}

/** Single entry point used by every helper — fetch + adapt body. */
function fetchAndStream<A>(
  opts: StreamOptions,
  pump: (
    reader: ReadableStreamDefaultReader<Uint8Array>,
    emit: (value: A) => void,
    close: () => void,
  ) => void,
): Stream<A, Throws<HttpClientError>> {
  return Stream.fromEffect(fetchResponse(opts)).flatMap((response) =>
    streamFromReader(response, pump),
  );
}

// ── httpStreamText ───────────────────────────────────────────────

/**
 * Stream raw decoded text chunks as they arrive — NO line buffering.
 * Each emit is whatever Bun/Node gave us from the network.
 */
export function httpStreamText(opts: StreamOptions): Stream<string, Throws<HttpClientError>> {
  return fetchAndStream(opts, (reader, emit, close) => {
    const decoder = new TextDecoder();
    (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            // Flush any pending multi-byte boundary
            const tail = decoder.decode();
            if (tail.length > 0) emit(tail);
            close();
            return;
          }
          const chunk = decoder.decode(value, { stream: true });
          if (chunk.length > 0) emit(chunk);
        }
      } catch {
        close();
      }
    })();
  });
}

// ── httpStreamLines ──────────────────────────────────────────────

/**
 * Stream body line-by-line. Lines are split on `\n`; trailing `\r`
 * is stripped (so it works for `\r\n` line endings too). The final
 * partial line (no trailing newline) is flushed on close.
 */
export function httpStreamLines(opts: StreamOptions): Stream<string, Throws<HttpClientError>> {
  return fetchAndStream(opts, (reader, emit, close) => {
    const decoder = new TextDecoder();
    let buffer = "";
    (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            if (buffer.length > 0) emit(stripCR(buffer));
            close();
            return;
          }
          buffer += decoder.decode(value, { stream: true });
          let idx: number;
          while ((idx = buffer.indexOf("\n")) !== -1) {
            emit(stripCR(buffer.slice(0, idx)));
            buffer = buffer.slice(idx + 1);
          }
        }
      } catch {
        close();
      }
    })();
  });
}

function stripCR(line: string): string {
  return line.endsWith("\r") ? line.slice(0, -1) : line;
}

// ── httpStreamNDJSON ─────────────────────────────────────────────

/**
 * One JSON object per line, each validated via `parser.safeParse`.
 * Empty lines are skipped. Parse/validation failures emit
 * `HttpParseError` typed failures — handle with `.catchTag`.
 */
export function httpStreamNDJSON<T>(
  opts: StreamOptions & { readonly schema: ResponseParser<T> },
): Stream<T, Throws<HttpClientError>> {
  const { schema, ...rest } = opts;
  const url = typeof opts.url === "string" ? opts.url : opts.url.toString();
  return httpStreamLines(rest).flatMap((line) => {
    const trimmed = line.trim();
    if (trimmed.length === 0) return Stream.empty();
    let data: unknown;
    try {
      data = JSON.parse(trimmed);
    } catch (cause) {
      return Stream.fail(
        new HttpParseError({
          url,
          cause,
          message: `NDJSON parse failed at line: ${trimmed.slice(0, 80)}`,
        }),
      );
    }
    const result = schema.safeParse(data);
    if (result.success) return Stream.of(result.data);
    return Stream.fail(
      new HttpParseError({
        url,
        cause: result.error,
        message: `NDJSON line doesn't match schema`,
      }),
    );
  });
}

// ── httpStreamSSE ────────────────────────────────────────────────

/**
 * Server-Sent Events. Events are separated by blank lines; within an
 * event, `event:`, `data:`, `id:`, `retry:` fields are recognized.
 * Comment lines (starting with `:`) are ignored.
 *
 * Multi-line `data:` fields are joined with `\n` per the SSE spec.
 */
export function httpStreamSSE(opts: StreamOptions): Stream<SSEvent, Throws<HttpClientError>> {
  return fetchAndStream(opts, (reader, emit, close) => {
    const decoder = new TextDecoder();
    let buffer = "";
    let event = "message";
    let data = "";
    let id: string | undefined;
    let retry: number | undefined;

    const flushEvent = () => {
      if (data.length === 0 && id === undefined && retry === undefined) {
        // Nothing pending — the blank line was between events but current is empty.
        return;
      }
      emit({ event, data, id, retry });
      event = "message";
      data = "";
      id = undefined;
      retry = undefined;
    };

    const consumeLine = (line: string): void => {
      const trimmed = stripCR(line);
      if (trimmed === "") {
        flushEvent();
        return;
      }
      if (trimmed.startsWith(":")) return; // comment
      const colon = trimmed.indexOf(":");
      const field = colon === -1 ? trimmed : trimmed.slice(0, colon);
      let val = colon === -1 ? "" : trimmed.slice(colon + 1);
      if (val.startsWith(" ")) val = val.slice(1);
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
          // Unknown field — ignore per spec.
          break;
      }
    };

    (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            if (buffer.length > 0) consumeLine(buffer);
            flushEvent(); // flush any in-progress event on close
            close();
            return;
          }
          buffer += decoder.decode(value, { stream: true });
          let idx: number;
          while ((idx = buffer.indexOf("\n")) !== -1) {
            consumeLine(buffer.slice(0, idx));
            buffer = buffer.slice(idx + 1);
          }
        }
      } catch {
        close();
      }
    })();
  });
}
