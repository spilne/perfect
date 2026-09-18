// Titles and rows for test.each, run by bun-test-titles.fixture.ts under
// `bun test`. node-bun-test-shim.test.ts checks that the shim's title
// formatter reproduces every title Bun reports for them.

class Point {
  x = 1;
  y = "two";
}
class Empty {}

const nullPrototype: Record<string, number> = Object.create(null);
nullPrototype.k = 1;

const scalars: unknown[] = [
  "str",
  "",
  'q"q',
  "it's",
  "tab\tnew\nline",
  "12",
  1,
  1.5,
  -2,
  0.1,
  -1e-7,
  1e21,
  2 ** 40,
  2 ** 60,
  Number.NaN,
  Infinity,
  -Infinity,
  -0,
  10n,
  true,
  null,
  undefined,
  Symbol("s"),
];

const objects: unknown[] = [
  {},
  [],
  [1, "two"],
  [[1, 2], [3]],
  [{}],
  [[]],
  [1, [2]],
  [[1]],
  ["a", { b: 1 }],
  [{ a: 1 }, { b: "x" }],
  [1, [2, [3]]],
  [[1, [2]]],
  [[[1]]],
  [-0, Number.NaN, undefined, null, 1n],
  { a: 1 },
  { a: { b: [1] } },
  { a: [{ b: 1 }] },
  { a: [[1, [2]]] },
  { "a-b": 1, "c d": 2, valid_1: 3, $: 4, "0": 5 },
  { [Symbol("k")]: 1 },
  { s: "with\nnewline", d: new Date(0), r: /re/g },
  {
    get g() {
      return 1;
    },
  },
  { f() {}, g: () => 1 },
  { a: undefined, b: null, e: {}, ea: [] },
  { a: { b: { c: { d: { e: 1 } } } } },
  new Point(),
  new Empty(),
  nullPrototype,
  new Set([1, "x"]),
  new Map([[{ k: 1 }, "v"]]),
  new Map(),
  new Set(),
  new Map([["key", [1, { x: 2 }]]]),
  new Set([{ a: 1 }, [1, 2]]),
  new Date(0),
  /re/g,
  function named() {},
  () => 1,
  Point,
];

const hasBigInt = (value: unknown): boolean =>
  typeof value === "bigint" || (Array.isArray(value) && value.some(hasBigInt));

const placeholders = ["%s", "%d", "%i", "%f", "%j", "%o", "%p", "%O", "%#", "%%", "%x", "%"];

const row = {
  name: "n",
  n: null,
  u: undefined,
  z: 0,
  f: false,
  s: 'q"q',
  arr: [1, "two"],
  obj: { x: 1, y: "s" },
  a: { b: { c: "deep" } },
  e: {},
  $: "dollar",
  _x: 1,
  "0": "zero",
};

export const TITLE_MATRIX: [title: string, rows: unknown[]][] = [
  // Each placeholder against every kind of argument, with a second argument
  // left over and then none.
  ...placeholders.flatMap((placeholder): [string, unknown[]][] =>
    [...scalars, ...objects]
      // JSON has no BigInt: Bun throws when it registers such a row.
      .filter((value) => !(placeholder === "%j" || placeholder === "%o") || !hasBigInt(value))
      .map((value, index) => [
        `${index} ${placeholder}|${placeholder}|${placeholder}`,
        [[value, "second"]],
      ]),
  ),
  // $path against an object row.
  ...[
    "$name has $z|$f",
    "$missing-x",
    "$n-x",
    "$u!",
    "$s $arr $obj",
    "$a.b.c $a.b",
    "$a.zz end",
    "$a. dot",
    "$1abc x",
    "$$ $_x $0 x",
    "$ name",
    "$name$name x",
    "$name.length",
    "trailing $",
    "$e",
    "$missing $name",
    "x$name",
    "%s $name %#",
    "%p $name",
    "%%s %$name %",
  ].map((title): [string, unknown[]] => [title, [row]]),
  // Rows that are not objects, or have more arguments.
  ["$name %s %d", [[{ name: "n1" }, 2]]],
  ["$name %s", [[{ name: "n1" }, 2]]],
  ["obj %s $b %#", [{ b: "bee" }]],
  ["$length $0 %p", [[[1, 2]]]],
  ["$name %p", [5, "str"]],
  ["$name %p", [[function foo() {}]]],
  ["%d %d", [["x", 2]]],
  ["%s %s %# %%", [["a", "b"]]],
  ["%# %s %% $name", [[]]],
  ["%s %i %O %o", [[1, 2]]],
  ["%o", [[[1, 2]]]],
  ["%%%s", [[1, 2]]],
  ["row %# of %p", [1, 2, 3]],
];
