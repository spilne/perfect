// Header redaction — stop auth tokens / cookies leaking into span attributes.
//
// Case-insensitive by default; the matcher works against lower-cased names.

const DEFAULT_REDACTED: ReadonlyArray<string> = [
  "authorization",
  "proxy-authorization",
  "cookie",
  "set-cookie",
  "x-api-key",
  "x-auth-token",
  "x-csrf-token",
  "x-access-token",
  "x-goog-api-key",
  "x-amz-security-token",
];

export interface RedactionPolicy {
  /** Returns true if this header's value should be replaced with `"<redacted>"`. */
  isRedacted(name: string): boolean;
}

/**
 * Build a redaction policy. By default redacts the common auth / cookie
 * headers; pass `extra` to add more; pass `override` to replace the list.
 */
export function makeRedaction(
  opts: { readonly extra?: ReadonlyArray<string>; readonly override?: ReadonlyArray<string> } = {},
): RedactionPolicy {
  const names = new Set(
    (opts.override ?? [...DEFAULT_REDACTED, ...(opts.extra ?? [])]).map((n) => n.toLowerCase()),
  );
  return {
    isRedacted: (name) => names.has(name.toLowerCase()),
  };
}

/** Default policy — common auth / cookie / token headers. */
export const defaultRedaction: RedactionPolicy = makeRedaction();

/** Apply the policy to a headers record, producing attribute-safe values. */
export function redactHeaders(
  headers: Record<string, string> | undefined,
  policy: RedactionPolicy = defaultRedaction,
): Record<string, string> {
  if (!headers) return {};
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    out[k] = policy.isRedacted(k) ? "<redacted>" : v;
  }
  return out;
}

/** Redact a URL — strips query string entirely (conservative default). */
export function redactUrl(url: string): string {
  const qi = url.indexOf("?");
  return qi === -1 ? url : url.slice(0, qi);
}
