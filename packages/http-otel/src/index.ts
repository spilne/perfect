// @perfect/http-otel — OpenTelemetry tracing for @perfect/http.
//
//   tracingMiddleware(opts?)   — drop-in HttpMiddleware; starts/ends spans
//   TracingFetchTransport      — wraps a transport; injects W3C traceparent
//   tracingTransport           — the default, wrapping FetchTransport
//
//   defaultRedaction / makeRedaction — header redaction for span attrs

export { tracingMiddleware, TracingFetchTransport, tracingTransport } from "./tracing";
export type { TracingOptions, TracingTransportOptions } from "./tracing";

export { defaultRedaction, makeRedaction, redactHeaders, redactUrl } from "./redact";
export type { RedactionPolicy } from "./redact";
