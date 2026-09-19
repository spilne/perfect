// @spilne/perfect-http-otel — OpenTelemetry tracing for @spilne/perfect-http.
//
//   tracingMiddleware(opts?)   — drop-in HttpMiddleware; starts/ends spans
//   TracingFetchTransport      — wraps a transport; injects W3C traceparent
//   tracingTransport           — the default, wrapping FetchTransport
//
//   defaultRedaction / makeRedaction — header redaction for span attrs

export { tracingMiddleware, TracingFetchTransport, tracingTransport } from "./tracing.js";
export type { TracingOptions, TracingTransportOptions } from "./tracing.js";

export { defaultRedaction, makeRedaction, redactHeaders, redactUrl } from "./redact.js";
export type { RedactionPolicy } from "./redact.js";
