// Tracer service — spans over effects, with parent propagation through the
// context. The default is a no-op tracer and withSpan() short-circuits on
// it, so tracing costs nothing until a real tracer is provided (per the
// anti-goal: no tracing in the hot path).
//
//   const traced = withSpan(handler, "handle-request", { route });
//   provide(traced, Tracer, new TestTracer())   // or the @spilne/perfect-otel bridge

import { type Eff, Suspend, Op } from "./eff";
import { service, type ServiceTag } from "./service";
import { Cause } from "./cause";

export type SpanStatus =
  | { readonly ok: true }
  | { readonly ok: false; readonly error: unknown; readonly interrupted: boolean };

export interface Span {
  readonly name: string;
  setAttribute(key: string, value: unknown): void;
  end(status: SpanStatus): void;
}

export interface SpanOptions {
  readonly attributes?: Record<string, unknown>;
  readonly parent?: Span | null;
}

export interface Tracer {
  startSpan(name: string, options?: SpanOptions): Span;
}

export const Tracer: ServiceTag<Tracer> = service<Tracer>("Tracer");

export const CURRENT_SPAN_KEY = Symbol.for("spilne/svc/CurrentSpan");

// ── No-op default ──────────────────────────────────────────────────

const noopSpan: Span = {
  name: "noop",
  setAttribute() {},
  end() {},
};

export const noopTracer: Tracer = {
  startSpan: () => noopSpan,
};

// Sentinel meaning "no enclosing span" — a real `null` can't live in the
// context map (GetCtx treats undefined/absent as a missing service).
const NO_SPAN: Span = noopSpan;

export const currentSpan: Eff<Span | null, never> = new Suspend(
  Op.FlatMap,
  new Suspend(Op.GetCtx, CURRENT_SPAN_KEY, null),
  (s: Span) => new Suspend(Op.Succeed, s === NO_SPAN ? null : s, null),
) as any;

// ── withSpan ───────────────────────────────────────────────────────

export function withSpan<A, S>(
  eff: Eff<A, S>,
  name: string,
  attributes?: Record<string, unknown>,
): Eff<A, S> {
  return new Suspend(Op.FlatMap, new Suspend(Op.GetCtx, Tracer.key, null), (tracer: Tracer) => {
    if (tracer === noopTracer) return eff; // zero-cost when tracing is off

    return new Suspend(
      Op.FlatMap,
      new Suspend(Op.GetCtx, CURRENT_SPAN_KEY, null),
      (parentRaw: Span) => {
        const parent = parentRaw === NO_SPAN ? null : parentRaw;
        return new Suspend(
          Op.FlatMap,
          new Suspend(Op.Sync, () => tracer.startSpan(name, { attributes, parent }), null),
          (span: Span) => {
            const scoped = new Suspend(
              Op.Provide,
              eff,
              new Map<symbol, unknown>([[CURRENT_SPAN_KEY, span]]),
            );
            // reify the outcome, end the span with its status, re-raise
            const reified = new Suspend(
              Op.CatchAll,
              new Suspend(
                Op.FlatMap,
                scoped,
                (value: any) => new Suspend(Op.Succeed, { ok: true as const, value }, null),
              ),
              (cause: Cause) => new Suspend(Op.Succeed, { ok: false as const, cause }, null),
            );
            return new Suspend(
              Op.FlatMap,
              reified,
              (outcome: any) =>
                new Suspend(
                  Op.FlatMap,
                  new Suspend(
                    Op.Sync,
                    () => {
                      if (outcome.ok) span.end({ ok: true });
                      else
                        span.end({
                          ok: false,
                          error: Cause.squash(outcome.cause),
                          interrupted: Cause.isInterruptedOnly(outcome.cause),
                        });
                    },
                    null,
                  ),
                  () =>
                    outcome.ok
                      ? new Suspend(Op.Succeed, outcome.value, null)
                      : new Suspend(Op.Fail, outcome.cause, null),
                ),
            );
          },
        );
      },
    );
  }) as any;
}

// ── Test tracer: records finished spans for assertions ─────────────

export interface RecordedSpan {
  readonly name: string;
  readonly parentName: string | null;
  readonly attributes: Record<string, unknown>;
  status: SpanStatus | null;
}

class TestSpan implements Span {
  readonly record: RecordedSpan & { attributes: Record<string, unknown> };

  constructor(
    readonly name: string,
    parent: Span | null,
    attributes: Record<string, unknown>,
    private readonly onEnd: (r: RecordedSpan) => void,
  ) {
    this.record = {
      name,
      parentName: parent?.name ?? null,
      attributes: { ...attributes },
      status: null,
    };
  }

  setAttribute(key: string, value: unknown): void {
    this.record.attributes[key] = value;
  }

  end(status: SpanStatus): void {
    if (this.record.status !== null) return; // idempotent
    this.record.status = status;
    this.onEnd(this.record);
  }
}

export class TestTracer implements Tracer {
  private _finished: RecordedSpan[] = [];

  startSpan(name: string, options?: SpanOptions): Span {
    return new TestSpan(name, options?.parent ?? null, options?.attributes ?? {}, (r) =>
      this._finished.push(r),
    );
  }

  /** Spans that have ended, in end order (children before parents). */
  get finished(): readonly RecordedSpan[] {
    return this._finished;
  }

  find(name: string): RecordedSpan | undefined {
    return this._finished.find((s) => s.name === name);
  }

  clear(): void {
    this._finished = [];
  }
}

export { NO_SPAN };
