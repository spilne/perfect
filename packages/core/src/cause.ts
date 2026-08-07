export type Cause<E = unknown> =
  | { readonly _tag: "Fail"; readonly error: E }
  | { readonly _tag: "Die"; readonly defect: unknown }
  | { readonly _tag: "Interrupt" }
  | { readonly _tag: "Both"; readonly left: Cause<E>; readonly right: Cause<E> }
  | { readonly _tag: "Then"; readonly left: Cause<E>; readonly right: Cause<E> };

type FailNode<E> = { readonly _tag: "Fail"; readonly error: E };
type DieNode = { readonly _tag: "Die"; readonly defect: unknown };

function foldLeafs<E, B>(
  c: Cause<E>,
  onFail: (e: E) => B,
  onDie: (d: unknown) => B,
  onInterrupt: () => B,
  combine: (a: B, b: B) => B,
): B {
  switch (c._tag) {
    case "Fail":
      return onFail(c.error);
    case "Die":
      return onDie(c.defect);
    case "Interrupt":
      return onInterrupt();
    case "Both":
    case "Then":
      return combine(
        foldLeafs(c.left, onFail, onDie, onInterrupt, combine),
        foldLeafs(c.right, onFail, onDie, onInterrupt, combine),
      );
  }
}

export const Cause = {
  fail: <E>(error: E): Cause<E> => ({ _tag: "Fail", error }),
  die: (defect: unknown): Cause<never> => ({ _tag: "Die", defect }),
  interrupt: (): Cause<never> => ({ _tag: "Interrupt" }),

  both: <E>(left: Cause<E>, right: Cause<E>): Cause<E> => ({ _tag: "Both", left, right }),

  then: <E>(left: Cause<E>, right: Cause<E>): Cause<E> => ({ _tag: "Then", left, right }),

  // leaf predicates (kept for back-compat — match only the top tag)
  isFailure: <E>(c: Cause<E>): c is FailNode<E> => c._tag === "Fail",
  isDie: <E>(c: Cause<E>): c is DieNode => c._tag === "Die",
  isInterrupt: <E>(c: Cause<E>): boolean => c._tag === "Interrupt",

  // tree queries
  failures: <E>(c: Cause<E>): E[] => {
    const out: E[] = [];
    foldLeafs<E, void>(
      c,
      (e) => {
        out.push(e);
      },
      () => {},
      () => {},
      () => {},
    );
    return out;
  },

  defects: <E>(c: Cause<E>): unknown[] => {
    const out: unknown[] = [];
    foldLeafs<E, void>(
      c,
      () => {},
      (d) => {
        out.push(d);
      },
      () => {},
      () => {},
    );
    return out;
  },

  hasFail: <E>(c: Cause<E>): boolean =>
    foldLeafs<E, boolean>(
      c,
      () => true,
      () => false,
      () => false,
      (a, b) => a || b,
    ),

  hasDie: <E>(c: Cause<E>): boolean =>
    foldLeafs<E, boolean>(
      c,
      () => false,
      () => true,
      () => false,
      (a, b) => a || b,
    ),

  hasInterrupt: <E>(c: Cause<E>): boolean =>
    foldLeafs<E, boolean>(
      c,
      () => false,
      () => false,
      () => true,
      (a, b) => a || b,
    ),

  // True iff the only leaves are Interrupt.
  isInterruptedOnly: <E>(c: Cause<E>): boolean =>
    foldLeafs<E, boolean>(
      c,
      () => false,
      () => false,
      () => true,
      (a, b) => a && b,
    ),

  // Drop Interrupt leaves; returns null if the whole tree collapses to interrupts.
  stripInterrupts: <E>(c: Cause<E>): Cause<E> | null => {
    switch (c._tag) {
      case "Fail":
        return c;
      case "Die":
        return c;
      case "Interrupt":
        return null;
      case "Both":
      case "Then": {
        const l = Cause.stripInterrupts(c.left);
        const r = Cause.stripInterrupts(c.right);
        if (l === null) return r;
        if (r === null) return l;
        return { _tag: c._tag, left: l, right: r };
      }
    }
  },

  // First Fail error, searching left-first.
  firstFail: <E>(c: Cause<E>): { value: E } | null => {
    switch (c._tag) {
      case "Fail":
        return { value: c.error };
      case "Die":
      case "Interrupt":
        return null;
      case "Both":
      case "Then":
        return Cause.firstFail(c.left) ?? Cause.firstFail(c.right);
    }
  },

  // First Die defect, searching left-first.
  firstDie: <E>(c: Cause<E>): { value: unknown } | null => {
    switch (c._tag) {
      case "Die":
        return { value: c.defect };
      case "Fail":
      case "Interrupt":
        return null;
      case "Both":
      case "Then":
        return Cause.firstDie(c.left) ?? Cause.firstDie(c.right);
    }
  },

  // Produce a single value representing the cause — for throwing from run().
  // Preference: first Fail > first Die > Interrupt.
  squash: <E>(c: Cause<E>): unknown => {
    const f = Cause.firstFail(c);
    if (f) return f.value;
    const d = Cause.firstDie(c);
    if (d) return d.value;
    return new Error("Interrupted");
  },

  /**
   * Compact one-line representation. Useful for log lines.
   *   Fail(boom)       — typed failure
   *   Die(...)         — defect (uncaught throw)
   *   Interrupt        — fiber cancellation
   *   (a & b)          — parallel composite (Both branches failed)
   *   (a ; b)          — sequential composite (Then composition)
   */
  pretty: <E>(c: Cause<E>): string => {
    switch (c._tag) {
      case "Fail":
        return `Fail(${formatValue(c.error)})`;
      case "Die":
        return `Die(${formatValue(c.defect)})`;
      case "Interrupt":
        return "Interrupt";
      case "Both":
        return `(${Cause.pretty(c.left)} & ${Cause.pretty(c.right)})`;
      case "Then":
        return `(${Cause.pretty(c.left)} ; ${Cause.pretty(c.right)})`;
    }
  },

  /**
   * Multi-line indented representation with structure preserved. For
   * human-readable error reports. Includes Error stack traces under Die.
   */
  prettyMultiline: <E>(c: Cause<E>): string => prettyMultilineInner(c, ""),
} as const;

function formatValue(v: unknown): string {
  if (v instanceof Error) return `${v.name}: ${v.message}`;
  if (v === null) return "null";
  if (v === undefined) return "undefined";
  if (typeof v === "object") {
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }
  return String(v);
}

function prettyMultilineInner<E>(c: Cause<E>, indent: string): string {
  switch (c._tag) {
    case "Fail":
      return `${indent}Fail: ${formatValue(c.error)}`;
    case "Die": {
      const d = c.defect;
      if (d instanceof Error && d.stack) {
        const stack = d.stack
          .split("\n")
          .map((l) => `${indent}  ${l}`)
          .join("\n");
        return `${indent}Die: ${d.name}: ${d.message}\n${stack}`;
      }
      return `${indent}Die: ${formatValue(d)}`;
    }
    case "Interrupt":
      return `${indent}Interrupt`;
    case "Both":
    case "Then": {
      const label = c._tag === "Both" ? "Both:" : "Then:";
      const child = `${indent}      `;
      return [
        `${indent}${label}`,
        `${indent}  ├── ${prettyMultilineInner(c.left, child).trimStart()}`,
        `${indent}  └── ${prettyMultilineInner(c.right, child).trimStart()}`,
      ].join("\n");
    }
  }
}
