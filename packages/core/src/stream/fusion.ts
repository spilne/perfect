// Operator fusion for Stream.
//
// Pure ops (map, filter, filterMap, tap) accumulate into a FusibleOp[] buffer
// and are compiled into a single chunk-walk when a non-fusible op or a
// terminal op is reached. Replaces N intermediate mapChunks passes with one,
// saving N-1 chunk iterations per emit.
//
// Ported from promin (MIT) with adjustments for Perfect's Chunk API.

/** Sentinel returned by fused functions to mean "skip this element". */
export const SKIP: unique symbol = Symbol("stream/fusion/SKIP");
export type SKIP = typeof SKIP;

export type FusibleOp =
  | { readonly _tag: "map"; readonly fn: (v: any) => any }
  | { readonly _tag: "filter"; readonly fn: (v: any) => boolean }
  | { readonly _tag: "filterMap"; readonly fn: (v: any) => any | undefined }
  | { readonly _tag: "tap"; readonly fn: (v: any) => void };

/** Compile a list of pure ops into a single per-element function. Returns
 * SKIP to signal a filtered-out element. Small-case specializations avoid the
 * loop+switch overhead for common pairings. */
export function compileFused(ops: FusibleOp[]): (value: any) => any {
  if (ops.length === 0) return (v: any) => v;

  if (ops.length === 1) return compileOne(ops[0]!);

  if (ops.length === 2) {
    const [o1, o2] = ops as [FusibleOp, FusibleOp];
    // map → filter
    if (o1._tag === "map" && o2._tag === "filter") {
      const f = o1.fn,
        p = o2.fn;
      return (v: any) => {
        const m = f(v);
        return p(m) ? m : SKIP;
      };
    }
    // filter → map
    if (o1._tag === "filter" && o2._tag === "map") {
      const p = o1.fn,
        f = o2.fn;
      return (v: any) => (p(v) ? f(v) : SKIP);
    }
    // map → map
    if (o1._tag === "map" && o2._tag === "map") {
      const f = o1.fn,
        g = o2.fn;
      return (v: any) => g(f(v));
    }
    // filter → filter
    if (o1._tag === "filter" && o2._tag === "filter") {
      const p = o1.fn,
        q = o2.fn;
      return (v: any) => (p(v) && q(v) ? v : SKIP);
    }
    // general 2-op fallback (includes tap / filterMap)
    const op1 = compileOne(o1),
      op2 = compileOne(o2);
    return (v: any) => {
      const r1 = op1(v);
      if (r1 === SKIP) return SKIP;
      return op2(r1);
    };
  }

  // 3+ ops: loop + switch. Still one chunk walk — the goal is right even if the
  // per-element cost is higher than the 2-op specialization.
  return (value: any) => {
    let v: any = value;
    for (let i = 0; i < ops.length; i++) {
      const op = ops[i]!;
      switch (op._tag) {
        case "map":
          v = op.fn(v);
          break;
        case "filter":
          if (!op.fn(v)) return SKIP;
          break;
        case "filterMap": {
          const r = op.fn(v);
          if (r === undefined) return SKIP;
          v = r;
          break;
        }
        case "tap":
          op.fn(v);
          break;
      }
    }
    return v;
  };
}

function compileOne(op: FusibleOp): (v: any) => any {
  switch (op._tag) {
    case "map":
      return op.fn;
    case "filter":
      return (v: any) => (op.fn(v) ? v : SKIP);
    case "filterMap":
      return (v: any) => {
        const r = op.fn(v);
        return r === undefined ? SKIP : r;
      };
    case "tap":
      return (v: any) => {
        op.fn(v);
        return v;
      };
  }
}

/** True if the op list contains any filtering op (needs array-alloc path). */
export function hasFilterOps(ops: FusibleOp[]): boolean {
  for (let i = 0; i < ops.length; i++) {
    const t = ops[i]!._tag;
    if (t === "filter" || t === "filterMap") return true;
  }
  return false;
}
