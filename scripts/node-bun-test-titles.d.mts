/** Pretty-print a value the way Bun's `%p` placeholder does. */
export function pretty(value: unknown, level?: number): string;

/** The title Bun gives row `index` of `test.each(table)(title, fn)`. */
export function formatEachTitle(title: string, args: readonly unknown[], index: number): string;
