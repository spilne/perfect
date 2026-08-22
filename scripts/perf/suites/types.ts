/** One measured operation. */
export interface BenchCase {
  readonly name: string;
  readonly unit: "ns/op" | "ns/item";
  /** Divide the raw timing by this to get a per-op / per-item figure. */
  readonly divisor: number;
  /**
   * Absolute ceiling in `unit`, enforced by the standalone gate. Omit for
   * cases that only make sense as a relative comparison.
   */
  readonly threshold?: number;
  readonly run: () => unknown | Promise<unknown>;
}

export interface Suite {
  readonly name: string;
  /**
   * Whether a regression here may fail the build. Default true.
   *
   * Set false for suites whose run-to-run variance swamps the effect being
   * measured. The HTTP suite is the case in point: on loopback it flagged
   * axios at +30% and raw fetch at +23% between two commits that touched
   * neither. Those numbers are still worth reporting — they are just not
   * evidence of anything.
   */
  readonly gating?: boolean;
  /** Acquire whatever the cases need (servers, fixtures). */
  setup?(): Promise<void> | void;
  teardown?(): Promise<void> | void;
  cases(): Promise<readonly BenchCase[]> | readonly BenchCase[];
}

/** One measured case in a results file. */
export interface BenchResult {
  readonly suite: string;
  readonly name: string;
  readonly unit: string;
  readonly median: number;
  readonly p25: number;
  readonly p75: number;
  readonly p99: number;
  readonly samples: number;
  readonly threshold?: number;
  /** False when this suite's numbers are informational only. */
  readonly gating?: boolean;
  /**
   * Set when the case could not run in this tree — typically the baseline
   * predates the API the case exercises. Such a case is reported, never
   * silently dropped, and never counted as a regression.
   */
  readonly unavailable?: string;
}

export interface ResultsFile {
  readonly schemaVersion: 1;
  readonly commit: string;
  readonly ref: string;
  readonly label: string;
  readonly runtime: {
    readonly bun: string;
    readonly platform: string;
    readonly arch: string;
    readonly cpus: number;
  };
  readonly config: { readonly samples: number; readonly warmup: number };
  readonly results: readonly BenchResult[];
}
