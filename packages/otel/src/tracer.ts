// OtelTracer — bridges @perfect/core's Tracer service to OpenTelemetry.
//
//   import { trace } from "@opentelemetry/api";
//   const tracer = new OtelTracer(trace.getTracer("my-app"));
//   run(provide(withSpan(program, "handle"), Tracer, tracer));
//
// Span parentage: core's withSpan passes the enclosing perfect Span as
// `options.parent`; we map it back to its otel span and set the otel
// context accordingly, so traces nest identically in both worlds.

import {
  type Span as OtelSpan,
  type Tracer as OtelApiTracer,
  type Attributes,
  SpanStatusCode,
  context as otelContext,
  trace,
} from "@opentelemetry/api";
import type { Span, SpanOptions, SpanStatus, Tracer } from "@perfect/core";

class OtelBridgeSpan implements Span {
  constructor(
    readonly name: string,
    readonly otelSpan: OtelSpan,
  ) {}

  setAttribute(key: string, value: unknown): void {
    this.otelSpan.setAttribute(key, value as never);
  }

  end(status: SpanStatus): void {
    if (status.ok) {
      this.otelSpan.setStatus({ code: SpanStatusCode.OK });
    } else if (status.interrupted) {
      this.otelSpan.setStatus({ code: SpanStatusCode.ERROR, message: "interrupted" });
      this.otelSpan.setAttribute("perfect.interrupted", true);
    } else {
      const message = status.error instanceof Error ? status.error.message : String(status.error);
      this.otelSpan.setStatus({ code: SpanStatusCode.ERROR, message });
      if (status.error instanceof Error) this.otelSpan.recordException(status.error);
    }
    this.otelSpan.end();
  }
}

export class OtelTracer implements Tracer {
  constructor(private readonly tracer: OtelApiTracer) {}

  startSpan(name: string, options?: SpanOptions): Span {
    const parent = options?.parent;
    const ctx =
      parent instanceof OtelBridgeSpan
        ? trace.setSpan(otelContext.active(), parent.otelSpan)
        : otelContext.active();

    const otelSpan = this.tracer.startSpan(
      name,
      { attributes: (options?.attributes ?? {}) as Attributes },
      ctx,
    );
    return new OtelBridgeSpan(name, otelSpan);
  }
}
