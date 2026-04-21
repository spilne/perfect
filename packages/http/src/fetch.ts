// Core fetch layer — three tiers of abstraction atop the transport:
//
//   httpFetch(opts)       — raw Response, no status check
//   httpFetchOk(opts)     — raw Response + status check (2xx by default)
//   httpRequest<T>(opts)  — full pipeline: fetch → status → JSON → parser
//
// Plus two unvalidated high-level variants:
//   httpRequestJson(opts) — fetch → status → JSON (returns unknown)
//   httpRequestText(opts) — fetch → status → text
//
// All return `Eff<T, Throws<HttpClientError>>` — composes with retry,
// timeout, race, etc. via Perfect's fluent API.

import { type Eff, type Throws, fail, succeed, tryPromise } from "@perfect/core";
import {
  type HttpClientError,
  HttpParseError,
  HttpStatusError,
  HttpUnknownError,
} from "./errors";
import { type ResponseParser } from "./response";
import { type HttpRequestOptions, type HttpTransport, defaultTransport } from "./transport";

/** Transport override available on every request-taking function. */
export interface WithTransport {
  readonly transport?: HttpTransport;
}

/** Status predicate — `true` means "treat as success". */
export type AcceptStatus = (status: number) => boolean;

/** Default: `[200, 300)`. */
const DEFAULT_ACCEPT: AcceptStatus = (s) => s >= 200 && s < 300;

const urlOf = (url: string | URL): string => (typeof url === "string" ? url : url.toString());

// ── Tier 1: raw fetch ─────────────────────────────────────────────

/** Execute a request via the transport, return the raw `Response`. */
export function httpFetch(
  options: HttpRequestOptions & WithTransport,
): Eff<Response, Throws<HttpClientError>> {
  const { transport = defaultTransport, ...rest } = options;
  return transport.execute(rest);
}

// ── Tier 2: fetch + status check ─────────────────────────────────

/**
 * Like `httpFetch` but rejects non-OK responses as `HttpStatusError`.
 * Override via `acceptStatus` (default: 200-299).
 *
 * If `errorSchema` is provided:
 *   - JSON.parse + `errorSchema.safeParse` succeeds → `HttpStatusError<B>` with `body: B`
 *   - either step fails → `HttpUnknownError` (carries raw body + parseError)
 *
 * Without `errorSchema`, an unparseable error body still comes back as
 * `HttpStatusError<string>` with the raw text — opt in to typed bodies
 * by providing the schema.
 */
export function httpFetchOk<E = string>(
  options: HttpRequestOptions &
    WithTransport & {
      readonly acceptStatus?: AcceptStatus;
      readonly errorSchema?: ResponseParser<E>;
    },
): Eff<Response, Throws<HttpClientError>> {
  const { acceptStatus = DEFAULT_ACCEPT, transport, errorSchema, ...rest } = options;
  return httpFetch({ ...rest, transport }).flatMap(
    (response): Eff<Response, Throws<HttpClientError>> => {
      if (acceptStatus(response.status)) return succeed(response);
      // Read body for diagnostics, then fail. `.text()` can throw if the body
      // was consumed already — default to empty string.
      return tryPromise<string, HttpClientError>(
        () => response.text().catch(() => ""),
        (cause): HttpClientError =>
          new HttpParseError({
            url: urlOf(rest.url),
            cause,
            message: `Failed to read error body from ${urlOf(rest.url)}`,
          }),
      ).flatMap((rawBody) => {
        const url = urlOf(rest.url);
        const message = `${rest.method ?? "GET"} ${url} returned ${response.status}`;
        if (!errorSchema) {
          return fail(
            new HttpStatusError({ url, status: response.status, body: rawBody, message }),
          ) as Eff<Response, Throws<HttpClientError>>;
        }
        // Try JSON.parse → schema.safeParse. Escalate to HttpUnknownError on failure.
        let parseError: unknown;
        try {
          const json = JSON.parse(rawBody);
          const result = errorSchema.safeParse(json);
          if (result.success) {
            return fail(
              new HttpStatusError<E>({
                url,
                status: response.status,
                body: result.data,
                message,
              }),
            ) as Eff<Response, Throws<HttpClientError>>;
          }
          parseError = result.error;
        } catch (e) {
          parseError = e;
        }
        return fail(
          new HttpUnknownError({
            url,
            status: response.status,
            body: rawBody,
            parseError,
            message: `${message} (errorSchema did not match)`,
          }),
        ) as Eff<Response, Throws<HttpClientError>>;
      });
    },
  );
}

// ── Tier 3: fetch + status + JSON + validation ───────────────────

/**
 * Full pipeline: fetch → status check → JSON parse → `schema.safeParse`.
 *
 *   const getUser = httpRequest({ url: "/u/1", schema: UserSchema });
 *   const user = await run(getUser);
 */
export function httpRequest<T, E = string>(
  options: HttpRequestOptions &
    WithTransport & {
      readonly acceptStatus?: AcceptStatus;
      readonly schema: ResponseParser<T>;
      readonly errorSchema?: ResponseParser<E>;
    },
): Eff<T, Throws<HttpClientError>> {
  const { schema, ...rest } = options;
  const url = urlOf(options.url);
  return httpFetchOk<E>(rest)
    .flatMap((response): Eff<unknown, Throws<HttpClientError>> =>
      tryPromise(
        () => response.json(),
        (cause): HttpClientError =>
          new HttpParseError({
            url,
            cause,
            message: `Failed to parse JSON from ${url}`,
          }),
      ),
    )
    .flatMap((data): Eff<T, Throws<HttpClientError>> => {
      const result = schema.safeParse(data);
      if (result.success) return succeed(result.data);
      return fail(
        new HttpParseError({
          url,
          cause: result.error,
          message: `Response from ${url} doesn't match schema: ${String(result.error)}`,
        }),
      ) as Eff<T, Throws<HttpClientError>>;
    });
}

// ── Unvalidated high-level variants ──────────────────────────────

/** fetch → status → JSON (`unknown` — validate manually if you care). */
export function httpRequestJson<E = string>(
  options: HttpRequestOptions &
    WithTransport & {
      readonly acceptStatus?: AcceptStatus;
      readonly errorSchema?: ResponseParser<E>;
    },
): Eff<unknown, Throws<HttpClientError>> {
  const url = urlOf(options.url);
  return httpFetchOk(options).flatMap((response) =>
    tryPromise(
      () => response.json(),
      (cause): HttpClientError =>
        new HttpParseError({
          url,
          cause,
          message: `Failed to parse JSON from ${url}`,
        }),
    ),
  );
}

/** fetch → status → text. */
export function httpRequestText<E = string>(
  options: HttpRequestOptions &
    WithTransport & {
      readonly acceptStatus?: AcceptStatus;
      readonly errorSchema?: ResponseParser<E>;
    },
): Eff<string, Throws<HttpClientError>> {
  const url = urlOf(options.url);
  return httpFetchOk(options).flatMap((response) =>
    tryPromise(
      () => response.text(),
      (cause): HttpClientError =>
        new HttpParseError({
          url,
          cause,
          message: `Failed to read text from ${url}`,
        }),
    ),
  );
}
